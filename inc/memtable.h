#pragma once

#include <atomic>
#include <ns.h>
#include <string>
#include <array>
#include <random>
#include <thread>
#include <literals.h>
#include <vector>
#include <cassert>
#include <cstring>

using namespace KVSTORE_NS::literals;

namespace KVSTORE_NS::memtable
{

struct skiptable
{
    // The maximum depth of the underlying skip-list. Higher values will increase the space required for the table,
    // and will increase the probability of stale data being returned for "get" operations.
    // However, search times for data in the table will decrease with higher values.
    // Therefore, read-heavy workloads may benefit from higher values.
    static size_t constexpr MAX_TABLE_LEVELS{16};

    // Simple options for configuring the characteristics of the table
    struct config_opts
    {
        // The maximum writes before the table is locked
        // The table will pre-allocate handle space for this many writes,
        // and a larger bound may delay a flush, causing the table to potentially hold onto more stale records.
        // Requires value < INT32_MAX.
        size_t writes_before_lock{2000};

        // The table will lock for insertion once it contains more than this much live data.
        // Due to concurrent access, this is not a hard limit, and may be overflowed by concurrent in-progress writes.
        size_t data_limit{16_MiB};

        // The table will lock for insertion once it contains more than this much data, including stale records.
        // Due to concurrent access, this is not a hard limit, and may be overflowed by concurrent in-progress writes.
        // Depending on usage patterns, this should be relatively large than "data_limit" -
        // if values are updated much more frequently than they are inserted, the stale data may significantly outweigh live values.
        size_t total_data_limit{160_MiB};
    };

    // A simple struct to pass C-style pointer-and-size for opaque data.
    // All records returned by member functions are valid for the lifetime of the instance.
    struct record
    {
        void * data{};
        size_t size{};
    };

    // Simple class tracking links in the overall table.
    // Contains forward links and a reference to the data index in the "records"
    // All nodes returned by member functions are valid for the lifetime of the instance
    struct node
    {
        node(memtable::skiptable const * owning_table, std::string_view k, int32_t record_idx) :
            table(owning_table), key(k), record_idx(record_idx) {}

        // returns the forward-linked node
        node * iterate(size_t level=0) const { return this->next[level]; }

        // NB: older and newer are not strictly inverses, equality matches with neither
        bool older(int32_t idx) const { return this->record_idx < idx; }
        bool newer(int32_t idx) const { return this->record_idx > idx; }

        int32_t idx() const { return this->record_idx; }
        int32_t update(int32_t new_idx) { return this->record_idx.exchange(new_idx); }

        void link(size_t level, node * n) { this->next[level] = n; }

        bool CE_link(size_t level, node * expected, node* n) { return this->next[level].compare_exchange_strong(expected, n); }

        memtable::skiptable::record const * value() const { return this->table->get(this); }

        std::string const key;
        memtable::skiptable const * table;
    private:
        std::atomic_int32_t record_idx;
        std::array<std::atomic<node *>, MAX_TABLE_LEVELS> next{};
    };

    skiptable(config_opts const & opts) : config(opts)
    {
        this->records.resize(opts.writes_before_lock);
        std::fill(this->records.begin(), this->records.end(), record{nullptr,0});
    }

    ~skiptable()
    {
        node const * node = this->first();
        while (node)
        {
            auto delnode = node;
            node = node->iterate();
            delete delnode;
        }

        for ( auto & record : this->records)
        {
            if (record.data) { free(record.data); }
        }
    }

    skiptable(skiptable&&) = delete;
    skiptable(skiptable const &) = delete;
    skiptable& operator=(skiptable&&) = delete;
    skiptable& operator=(skiptable const&) = delete;

    bool lock() { return this->is_locked.exchange(true); }

    bool locked() const
    {
        return this->total_data_size >= this->config.total_data_limit
            || this->next_record >= this->config.writes_before_lock
            || this->data_size >= this->config.data_limit
            || this->is_locked;
    }

    bool empty() const { return this->data_size == 0; }

    // Returns the first node in the table for the given level
    node const * first(size_t level=0) const
    {
        return this->head.iterate(level);
    }

    // Finds the node in the table with the given key, nullptr if the key is not found.
    // The returned pointer is valid for the lifetime of the table itself.
    node const * find(std::string_view key) const
    {
        node const * n = &this->head;
        for (int32_t i = MAX_TABLE_LEVELS - 1; i >= 0; i--)
        {
            while (true)
            {
                node const * n2 = n->iterate(i);
                // use compare and save the value to prevent re-testing potentially long-running string equality
                // if the next key is the tail (nullptr), set comp to -1 to signify that it is "larger" than our key
                int const comp = n2 ? key.compare(n2->key) : -1;
                if (comp < 0) { break; }
                else if (comp == 0) { return n2; }
                else { n = n2; }
            }
        }

        return nullptr;
    }

    // Inserts an element into the table, allowing for lock free concurrent import
    // Returns the node that was inserted, or nullptr on failure
    node const * insert(std::string_view key, void * data, size_t size)
    {
        // Ensure the table hasn't exceeded configured limits
        if (this->locked()) { return nullptr; }

        // Find the location in our record table where we will allocate new data
        // Concurrent write is consistent, as we only increment this value here, and never decrement
        // In addition, concurrent reads are consistent, though they may return stale data
        size_t const new_record_idx = this->next_record.fetch_add(1);

        // Write the new data into the record buffer, returning false on failure to allocate
        this->records[new_record_idx].data = malloc(size);
        if (this->records[new_record_idx].data)
        {
            memcpy(this->records[new_record_idx].data, data, size);
            this->records[new_record_idx].size = size;
        }
        else { return nullptr; }

        this->total_data_size += size;

        // Generate a random level to insert the new data, bounded by the max levels in our table
        // we leak the random generator until the thread is cleaned up, but that's relatively inconsequential
        static thread_local std::minstd_rand* gen = nullptr;
        if (!gen) gen = new std::minstd_rand(std::hash<std::thread::id>()(std::this_thread::get_id()));
        std::uniform_int_distribution<int> dist{};
        int32_t level = 0;
        while (level < MAX_TABLE_LEVELS)
        {
            int const rn = dist(*gen);
            if (rn % 2) level += 1; // increase the level until an even number, resulting in roughly 1/2 the count per level
            else break;
        }

        node * new_node = new node(this, key, new_record_idx);

        // for each level below the calculated layer, insert the node
        // This is where much of the complexity of the lock-free concurrency lies
        // we only want to commit updates if a new node has not been inserted, otherwise we must rollback and retry
        std::array<node *, MAX_TABLE_LEVELS> updates{};
        std::array<node *, MAX_TABLE_LEVELS> update_nexts{};

insert_loop:
        node * n = &this->head;
        for (int32_t i = level; i >= 0; i--)
        {
            while (true)
            {
                node * n2 = n->iterate(i);
                // use compare and save the value to prevent re-testing potentially long-running string equality
                // if the next key is the tail (nullptr), set comp to -1 to signify that it is "larger" than our key
                int comp = n2 ? key.compare(n2->key) : -1;
                if (comp < 0)
                {
                    updates[i] = n;
                    update_nexts[i] = n2;
                    break;
                }
                else if (comp == 0 && n2->newer(new_record_idx))
                {
                    // This key has been updated by a more recent insert operation - pretend this insertion "succeeded",
                    // and was then later overwritten by the other operation.
                    delete new_node;
                    return n2;
                }
                else if (comp == 0 && n2->older(new_record_idx))
                {
                    // Simply update the node to point to the new data in the store and return after adjusting sizes
                    int32_t const old = n2->update(new_record_idx);
                    this->data_size -= this->records[old].size;
                    this->data_size += size;
                    delete new_node;
                    return n2;
                }
                else if (comp == 0 /* !older && !newer */)
                {
                    // This should never occur, as we should not reuse record indicies
                    assert(false);
                }
                else
                {
                    // Keep iterating through the level
                    n = n2;
                }
            }
        }

        // At this point, we have all the links we need to update.
        // Attempt to update them from highest to lowest level, retrying the whole process if the links have changed
        // An adversary could potentially use well-timed/structured inserts to cause this loop to retry indefinitely,
        // so it might be practical to insert retry/fail logic here rather than an infinite retry loop.
        // However, this would require much more careful handling of the node deletion, as it could be partially linked.
        for (int32_t i = level; i >= 0; i--)
        {
            new_node->link(i, update_nexts[i]);
            if (!updates[i]->CE_link(i,update_nexts[i], new_node))
            {
                // The link was changed while we were updating - find new links and retry
                goto insert_loop;
            }
        }

        this->data_size += size;
        return new_node;
    }

    // If the passed node ptr is stale, it is possible that a subsequent (or concurrent)
    // insert operation has overwritten the record idx for this node.
    // In this case we will return a stale value for the data record.
    // However, this record will still reference valid data for the lifetime of this table.
    // returns nullptr on invalid input
    record const * get(node const * node) const
    {
        if (!node || node->idx() < 0 || node->idx() >= this->next_record) { return nullptr; }
        else { return &this->records[node->idx()]; }
    }

    // returns nullptr if the key is not found
    record const * get(std::string_view key) const { return this->get(this->find(key)); }

    config_opts const config;
private:
    std::vector<record> records{};
    std::atomic_size_t total_data_size{};
    std::atomic_size_t data_size{};
    std::atomic_bool is_locked{};
    std::atomic_int32_t next_record{};
    node head{this, std::string(), -1};
};

} // namespace KVSTORE_NS::memtable
