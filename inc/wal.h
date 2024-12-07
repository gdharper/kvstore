#pragma once

#include <ns.h>
#include <filesystem>
#include <memtable.h>
#include <fstream>
#include <unordered_set>
#include <atomic>
#include <shared_mutex>
#include <algorithm>

using namespace std::literals::chrono_literals;

namespace KVSTORE_NS::WAL
{
// Implements write-ahead-logging, enabling recovery of in-memory data upon abnormal process crash
struct walfile
{
    inline static std::string constexpr FILE_EXT{".kvwal"};

    struct config_options
    {
        // The maximum number of concurrent put operations that can be logged.
        // If a greater number of operations occur concurrently, the WAL may reflect inaccurate state.
        size_t concurrent_put_limit{256};

        // The directory where the logfile will be created
        std::filesystem::path base_dir{"."};
    };

    config_options const config;
    std::filesystem::path const logfile;

    walfile(config_options const & opts) :
        config(opts),
        logfile(opts.base_dir / (std::to_string(std::chrono::steady_clock::now().time_since_epoch() / 1ms) + FILE_EXT)),
        putq(opts.concurrent_put_limit)
    {
    }

    ~walfile()
    {
        std::filesystem::remove(this->logfile);
    }

    walfile(walfile const &) = delete;
    walfile(walfile&&) = delete;
    walfile & operator==(walfile const &) = delete;
    walfile & operator==(walfile&&) = delete;

    // Log a "put" operation to the WAL (represented by the node inserted into the memtable)
    // concurrent "log" calls are safe, as only 1 concurrent thread will write actual data to the logfile
    void log(memtable::skiptable::node const * node)
    {
log_retry:
        // first, take the shared_mutex in "shared mode" and write to the queue
        // the atomicity of the write-head ensures valid ordering
        // If the queue is full we loop (releasing the mutex) as eventually a concurrent thread will drain the queue
        // in practice this may occassionally cause significant latency on log operations, so a retry limit might be practical
        this->q_mutex.lock_shared();

        size_t w = this->write;
        size_t const next = (w + 1) % this->config.concurrent_put_limit;

        if (next == this->read || !this->write.compare_exchange_weak(w, next)) { goto log_retry; }
        else { this->putq.at(w) = node; }

        this->q_mutex.unlock_shared();

        // now try to take the lock exclusively, to drain the queue into the file
        // if we fail, another concurrent thread is doing the same job, so simply exit
        if (this->q_mutex.try_lock())
        {
            std::ofstream file{this->logfile, std::ios::app};
            assert(file.good());

            while (this->read != this->write)
            {
                memtable::skiptable::node const * n{};
                std::swap(this->putq.at(this->read), n);
                memtable::skiptable::record const * data = n->value();
                file << n->key << std::endl;
                file.write(reinterpret_cast<char const *>(data->data), data->size);
                this->read = (this->read + 1) % this->config.concurrent_put_limit;
            }

            this->q_mutex.unlock();
        }
    }

    // Load an existing logfile into the passed memtable.
    // Attempts to only write the most recent value for each key.
    static void load(std::filesystem::path const & logfile, memtable::skiptable & table)
    {
        assert(std::filesystem::exists(logfile));
        assert(std::filesystem::is_regular_file(logfile));
        assert(logfile.extension() == walfile::FILE_EXT);

        std::ifstream file{logfile};
        assert(file.good());

        std::unordered_set<std::string> inserted{};
        std::vector<std::pair<std::string, std::string>> kvs{};
        while (file.good())
        {
            std::string key{};
            std::string value{};
            std::getline(file, key);
            assert(file.good());
            std::getline(file, value);

            kvs.emplace_back(std::make_pair(std::move(key), std::move(value)));
        }

        // reverse the order as we want the most recent values, which are at the end of the file
        std::reverse(kvs.begin(), kvs.end());
        for (auto const & kv : kvs)
        {
            if (!inserted.contains(kv.first))
            {
                // drop newline characters
                assert(!table.locked());
                table.insert(kv.first.substr(0,kv.first.size() - 1), (void*)kv.second.data(), kv.second.size() - 1);
                inserted.insert(kv.first);
            }
        }
    }

private:
    std::shared_mutex q_mutex{};
    std::vector<memtable::skiptable::node const *> putq;
    std::atomic_size_t write{};
     // doesn't need to be atomic, will only be modified under exclusive mutex ownership
    size_t read{};
};

} // namespace KVSTORE_NS::WAL
