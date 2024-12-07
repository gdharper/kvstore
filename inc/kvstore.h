
#pragma once
#include <ns.h>
#include <wal.h>
#include <sstable.h>
#include <thread>
#include <queue>


namespace KVSTORE_NS
{
using namespace memtable;
using namespace WAL;
using namespace sst;
using namespace std::chrono_literals;

struct kvstore
{
    struct config_options
    {
        // see memtable.h
        skiptable::config_opts memtable_options{};

        // see sstable.h
        sstable::config_options sst_options{};

        // see wal.h
        walfile::config_options wal_options{};

        // how often the background thread wakes up to write memtable history to sst files
        std::chrono::milliseconds background_activity_period{50ms};

        // the number of locked memtables held in memory before writing to SST files
        // Increasing this value will potentially increase performance,
        // but will cause the memory footprint and WAL size to increase.
        // the actual history may exceed this value, as it is only flushed every "background_activity_period"
        size_t memtable_history{2};
    };

    explicit kvstore(config_options const & opts):
        config(opts),
        mtable(std::make_unique<skiptable>(opts.memtable_options)),
        wal(std::make_unique<walfile>(opts.wal_options))
    {
        // if we have an old WAL (from abnormal exit), read into our memtable and delete
        for (auto const & item : std::filesystem::directory_iterator(opts.wal_options.base_dir))
        {
            if (item.path().extension() == walfile::FILE_EXT && std::filesystem::is_regular_file(item))
            {
                walfile::load(item.path(), *this->mtable);
                if (this->mtable->locked()) { this->save_memtable(); }

                std::filesystem::remove(item);
            }
        }

        // load our old sst files into the queue
        for (auto const & item : std::filesystem::directory_iterator(opts.sst_options.base_dir))
        {
            if (item.path().extension() == sstable::FILE_EXT && std::filesystem::is_regular_file(item))
            {
                sstq.emplace(item.path());
            }
        }

        // startup the background thread
        this->background_thread = std::thread{ [this]{ this->background(); }};
    }

    ~kvstore()
    {
        this->exit = true;
        this->background_thread.join();
        this->flush_memtables();
    }

    kvstore(kvstore const &) = delete;
    kvstore(kvstore&&) = delete;
    kvstore & operator==(kvstore const &) = delete;
    kvstore & operator==(kvstore&&) = delete;

    // Insert a k,v pair into the table. Currently, this is written so as not to fail, rather retrying endlessly
    // An alternative design would be to implement bounded retry logic so as not to hang clients upon certain edge cases
    void put(std::string_view key, void * data, size_t data_size)
    {
put_retry:
        skiptable::node const * node = this->mtable->insert(key, data, data_size);
        // failure indicates the memtable is full / locked - retry after rereshing the table
        if (!node)
        {
            this->save_memtable();
            goto put_retry;
        }

        this->wal->log(node);
    }

    // Fetches the value bytes for a given key, returning true if the key is in the store
    // iff the key is found, the data will be copied into "data_out", which will be resized as needed.
    bool get(std::string_view key, std::vector<std::byte> & data_out) const
    {
        // first check our memtable
        skiptable::record const * record = this->mtable->get(key);
        if (record)
        {
            data_out.resize(record->size);
            memcpy(data_out.data(), record->data, record->size);
            return true;
        }

        // now check old memtables, most recent first
        hist_node * n = this->hist;
        while (n)
        {
            record = n->table->get(key);
            if (record)
            {
                data_out.resize(record->size);
                memcpy(data_out.data(), record->data, record->size);
                return true;
            }

            n = n->next;
        }

        // now check through our sst files. As the priority queue is sorted by timestamp,
        // the files will be checked from most -> least recent, ensuring freshness of data
        // we take a read-lock on the shared mutex here, which will block the brackground thread
        // issues with sst files not being created (and/or in-memory history growing unbounded)
        // can likely be traced to get-requests starving the background thread
        std::shared_lock sst_lock{this->sst_mutex};
        for (auto const & entry : this->sstq) { if (entry.get(key, data_out)) { return true; } }

        return false;
    }

    config_options const config;

private:
    // lock our current memtable and add it to the history
    // we want to insert this as the "head" of the history list, so that more recent values are read first,
    // before older tables are checked when serving "get" operations
    void save_memtable()
    {
        if (this->mtable->empty()) { return; }

        auto mt = std::make_unique<skiptable>(this->config.memtable_options);
        std::swap(this->mtable, mt);
        mt->lock();

        hist_node * hn = new hist_node{.table=std::move(mt)};
        do { hn->next = this->hist; } while (!this->hist.compare_exchange_weak(hn->next, hn));
    }

    // flush our memtable history to sst files, reseting the WAL and flushing the in-memory data to disk
    // This may block while trying to acquire the sst mutex.
    // This is _ok_ because we only run this in the background thread.
    // However, if that thread seems to be hanging, or "get" operations are blocked on the mutex,
    // this is where to debug
    void flush_memtables()
    {
        this->save_memtable();

        // swap out the WAL, but don't delete the old one yet, in case we crash in this process
        // the old WAL will be cleaned up after this block exits
        auto wf = std::make_unique<walfile>(this->config.wal_options);
        std::swap(this->wal, wf);

        hist_node * save = this->hist.exchange(nullptr);
        while (save)
        {
            this->sst_mutex.lock();
            this->sstq.emplace(this->config.sst_options, *save->table);
            this->sst_mutex.unlock();

            hist_node * delnode = save;
            save = save->next;
            delete delnode;
        }
    }

    // this function (executed by our background thread) periodically wakes and flushes memtables to disk as sst files
    void background()
    {
        while (!this->exit)
        {
            // If a period is passed in that's quite long, this will delay shutdown
            // A potential improvement is to partition the wait into smaller increments to check for "exit"
            auto const next_wake = std::chrono::steady_clock::now() + this->config.background_activity_period;
            std::this_thread::sleep_until(next_wake);

            if (exit) { break; }

            // Flush memtables to sst files if the history has grown excessively large
            size_t hist_count{};
            hist_node * n = this->hist;
            while (n)
            {
                hist_count +=1;
                n = n->next;
            }

            if (hist_count > this->config.memtable_history)
            {
                this->flush_memtables();
            }
        }
    }

    std::unique_ptr<skiptable> mtable;
    std::unique_ptr<walfile> wal;

    struct hist_node
    {
        std::shared_ptr<skiptable> table{};
        hist_node* next{};
    };

    std::atomic<hist_node *> hist{};

    mutable std::shared_mutex sst_mutex{};

    // We can safely iterate over the p_queue because we only iterate under a reader/writer mutex,
    // and therefore don't need to worry about priority inversion.
    struct sst_queue : std::priority_queue<sstable>
    {
        // iterators are pass-through to the underlying priority_queue container
        std::vector<sstable>::const_iterator  begin() const { return c.begin(); }
        std::vector<sstable>::const_iterator end() const { return c.end(); }
    } sstq{};
    bool exit{};
    std::thread background_thread{};
};

} // namespace KVSTORE_NS
