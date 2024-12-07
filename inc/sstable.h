#pragma once

#include <ns.h>
#include <filesystem>
#include <chrono>
#include <literals.h>
#include <memtable.h>
#include <fstream>
// Linux only for usage of file operations (open, ftruncate, mmap, etc)
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace std::literals::chrono_literals;
using namespace KVSTORE_NS::literals;
using namespace KVSTORE_NS::memtable;

/********************************************************************************
 * File Format Definition
 *
 * This format takes inspiration from the RocksDB "BlockBasedTable" format, with significant simplification.
 * https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
 *
 * Keys are prefix-compressed to reduce space, save for inermittent "index" keys, which reset the prefix for the next segment of blocks.
 * The first key of a data block is always an "index" key. Key entries are padded to 8-byte alignment, which may add up to 14 bytes per entry.
 * An out of scope improvement woud be a bloom filter, used for a fast check to determine if a given key is stored in the given file
 * In addition, instead of using fixed size blocks, which might lead to significant wasted space in the file,
 * blocks could be
 * Data Block 0
 *  Key Entry 0.0
 *   prefix_bytes: uint64 - number of shared bytes from last index key: all index keys have value "0".
 *   suffix_bytes: uint64 - number of bytes in the remainder of the key after the shared prefix from the last index key.
 *   value_bytes: uint64 - size of the value data
 *   key_suffix: byte[suffix_bytes] - the remaining bytes of the key after the shared prefix. NOT nul-terminated.
 *   padding: byte[] - zero padding to 8-byte alignment
 *   value_data: byte[value_bytes] - the value for the given key.
 *   padding: byte[] - zero padding to 8-byte alignment
 *  Key Entry 0.1
 *  ...
 *  Key Entry 0.x
 *  Padding: byte[] - zero padding to fill the block up to the configured size
 *  Block Footer
 *   indicies: uint64[index_count] - block-relative offsets for each "index" key
 *   index_count: uint64 - number of "index" keys in the block
 * Data Block 1
 *  Key Entry 1.0
 *  ...
 *  Key Entry 1.y
 *  Block Footer
 * ...
 * Data Block N
 * Footer
 *  block_size: uint64_t - the size in bytes of each data block
 *  block_count: uint64_t - number of blocks (of block_size bytes) in the file
 *  entry_count: uint64 - total count of entries in all data blocks
 *  key_bytes: uint64 - total size of all keys before prefix compression
 *  value_bytes: uint64 - total size of all value data in the file
 *  magic: uint64 - fixed 0x677265676F727968
 */

namespace KVSTORE_NS::sst
{
struct sstable
{
    inline static std::string constexpr FILE_EXT{".kvsst"};
    struct config_options
    {
        size_t max_block_size{4_MiB};
        std::filesystem::path base_dir{"."};
    };

    sstable(config_options const & opts) :
        t(std::chrono::steady_clock::now()),
        // file path under the base directory is simply the timestamp. Assume that the files will be created at a rate of less than 1/ms
        path(opts.base_dir / (std::to_string(this->t.time_since_epoch() / 1ns) + FILE_EXT)),
        config(opts)
    {

    }

    // Use this ctor to simultaneously write the file from the passed table
    sstable(config_options const & opts, memtable::skiptable const & table) : sstable(opts)
    {
        bool built = this->build(table);
        assert(built);
    }

    // Load the config information for an existing file and take ownership of that sst file
    sstable(std::filesystem::path const & sstfile) : t(t_from(sstfile)), path(sstfile), config(config_from(sstfile))
    {
    }

    // sort sst files by timestamp
    bool operator<(sstable const & other) const { return this->t < other.t; }

    // Build a sst file from the data in a given memtable - the memtable must be locked.
    // This uses platform-agnostic c++ streams for portability, as writing sequentially should still be "fast"
    // (compared to platform-specific file operations).
    bool build(memtable::skiptable const & table) const
    {
        if (!table.locked() ) { return false; }

        std::ofstream of{this->path, std::ios::binary};
        assert(of.good());

        // iterate over the keys, writing data to the file as we go
        size_t blocks = 0;
        size_t key_bytes{};
        size_t data_bytes{};
        size_t entries{};
        std::string_view prefix{};
        size_t block_bytes{};
        std::vector<uint64_t> idx_offsets{};

        memtable::skiptable::node const * n = table.first();
        while (n)
        {
            auto record = table.get(n);
            std::string_view key{n->key};

            key_bytes += key.size();
            data_bytes += record->size;
            entries += 1;

            entry_header hdr{header_from(prefix, n)};

            // Each time a key doesn't match a prefix, we denote it an index key
            bool const idx_key = hdr.prefix_bytes == 0;

            size_t const entry_bytes = sizeof(entry_header)
                                + hdr.suffix_bytes
                                + entry_header::padding_bytes(hdr.suffix_bytes)
                                + record->size
                                + entry_header::padding_bytes(record->size);

            // If we need a new block, write the block footer and update counters
            if (block_bytes > (this->config.max_block_size
                                - entry_bytes // total bytes for this entry
                                - (idx_key * sizeof(uint64_t)) // index_offset for this entry
                                - (idx_offsets.size() * sizeof(uint64_t)) // previous index_offsets
                                - sizeof(uint64_t))) // final "index_count" uint64
            {
                uint64_t const idx_count = idx_offsets.size();
                size_t const footer_bytes = sizeof(uint64_t) * (idx_count + 1);
                for (; block_bytes < this->config.max_block_size - footer_bytes; block_bytes++) { of << (char)0; }
                of.write(reinterpret_cast<char const *>(idx_offsets.data()), idx_count * sizeof(uint64_t));
                of.write(reinterpret_cast<char const *>(&idx_count), sizeof(idx_count));

                blocks += 1;
                block_bytes = 0;
                idx_offsets.clear();
                prefix = std::string_view();
            }

            // write the entry data
            if (idx_key) { idx_offsets.emplace_back(block_bytes); }

            of.write(reinterpret_cast<char const *>(&hdr), sizeof(hdr)); // hdr
            of << key.substr(hdr.prefix_bytes, hdr.suffix_bytes); // key suffix (entire key in case of idx key)
            for (size_t i = 0; i < entry_header::padding_bytes(hdr.suffix_bytes); i++) { of << (char)0; } // suffix padding
            of.write(reinterpret_cast<char const *>(record->data), record->size); // value
            for (size_t i = 0; i < entry_header::padding_bytes(record->size); i++) { of << (char)0; } // value padding
            block_bytes += entry_bytes;


            n = n->iterate();

            // If we are about to exit iteration, we need to write the final block footer
            if (!n)
            {
                uint64_t const idx_count = idx_offsets.size();
                size_t const footer_bytes = sizeof(uint64_t) * (idx_count + 1);
                for (; block_bytes < this->config.max_block_size - footer_bytes; block_bytes++) { of << (char)0; }
                of.write(reinterpret_cast<char const *>(idx_offsets.data()), idx_count * sizeof(uint64_t));
                of.write(reinterpret_cast<char const *>(&idx_count), sizeof(idx_count));

                blocks += 1;
            }
        }

        // write the footer
        footer const ftr{
            .block_size = this->config.max_block_size,
            .block_count = blocks,
            .entry_count = entries,
            .key_bytes = key_bytes,
            .value_bytes = data_bytes,
            .magic{footer::MAGIC_NUMBER}
        };

        of.write(reinterpret_cast<char const *>(&ftr), sizeof(ftr));
        of.flush();
        of.close();
        return true;
    }

    // Retrieve the data for a given key. Returns true  and copies value into "data_out"
    // if the key is found, otherwise returns false
    // This operation could be optimized on the "not-found" path with the addition of a bloom filter
    // NB: this code is not platform agnostic, but rather depends on linux file operations.
    // This design was chosen for performance purposes, as c++ streams are slower for non-sequential reads
    bool get(std::string_view key, std::vector<std::byte> & data_out) const
    {
        assert(std::filesystem::exists(this->path));
        size_t const file_size = std::filesystem::file_size(this->path);

        // Create a file with read permissions for everyone - this will still allow writing with this descriptor
        // Return false on failure, otherwise mmap the file and write our data to it
        int fd = open(this->path.c_str(), O_RDONLY);
        assert(fd != -1);

        // cast the ptr to allow bytewise ptr math
        // Currently, we mmap the whole file for purposes of simplicity.
        // An alternative design, that might be faster for some use cases or necesary for very large sst file,
        // would be to mmap the file block by block as needed, mapping only the footer initially.
        std::byte * fptr = reinterpret_cast<std::byte *>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
        assert(fptr != MAP_FAILED);

        auto ftr = reinterpret_cast<footer const *>(fptr + file_size - sizeof(footer));
        assert(ftr->magic == footer::MAGIC_NUMBER);

        // Find the block for our key
        size_t block{};
        for (; block < ftr->block_count; block++)
        {
            size_t const offset = block * ftr->block_size;
            auto hdr = reinterpret_cast<entry_header const *>(fptr + offset);
            assert(hdr->prefix_bytes == 0); // the first key in each block should always be an index key
            std::string_view k{reinterpret_cast<char const *>(hdr + 1), hdr->suffix_bytes};
            if (key < k) { break; }
        }

        // We want to look in the block previous to the last checked, as we will break once the block is all keys > "key"
        block -= 1;

        size_t const block_base = block * ftr->block_size;
        uint64_t const idx_count = *reinterpret_cast<uint64_t const *>(fptr + block_base + ftr->block_size - sizeof(uint64_t));
        uint64_t idx_offset{};
        std::string_view prefix{};
        for (size_t idx = 0; idx < idx_count; idx ++)
        {
            uint64_t last_offset = idx_offset;
            idx_offset = *reinterpret_cast<uint64_t const *>(fptr + block_base + ftr->block_size - (sizeof(uint64_t) * (1 + idx_count - idx)));
            auto hdr = reinterpret_cast<entry_header const *>(fptr + block_base + idx_offset);
            assert(hdr->prefix_bytes == 0);
            std::string_view k{reinterpret_cast<char const *>(hdr + 1), hdr->suffix_bytes};

            // same logic as before, we want to look in the sub-block before the last key we checked
            if (key < k)
            {
                idx_offset = last_offset;
                break;
            }
            else { prefix = k; }
        }

        // search through the section of keys under this prefix / index_key to try and find target
        // stop iterating if we find our key (and return), or when we reach the next prefix key
        auto hdr = reinterpret_cast<entry_header const *>(fptr + block_base + idx_offset);
        do
        {
            std::string_view suffix{reinterpret_cast<char const *>(hdr + 1), hdr->suffix_bytes};
            if (key.substr(0, hdr->prefix_bytes) == prefix.substr(0, hdr->prefix_bytes) &&
                key.substr(hdr->prefix_bytes, hdr->suffix_bytes) == suffix)
            {
                // we found out key - copy data and return
                data_out.resize(hdr->value_bytes);
                auto src = reinterpret_cast<std::byte const *>(hdr + 1) + hdr->suffix_bytes + entry_header::padding_bytes(hdr->suffix_bytes);
                memcpy(data_out.data(), src, hdr->value_bytes);
                munmap(fptr, file_size);
                return true;
            }
            else
            {
                hdr = reinterpret_cast<entry_header const *>(reinterpret_cast<std::byte const *>(hdr + 1)
                    + hdr->suffix_bytes
                    + entry_header::padding_bytes(hdr->suffix_bytes)
                    + hdr->value_bytes
                    + entry_header::padding_bytes(hdr->value_bytes));
            }
        } while (hdr->prefix_bytes != 0);

        munmap(fptr, file_size);
        return false;
    }

private:
    std::chrono::steady_clock::time_point t;
    std::filesystem::path path;
    config_options config;

    struct entry_header
    {
        uint32_t prefix_bytes{};
        uint32_t suffix_bytes{};
        uint64_t value_bytes{};
        static size_t constexpr padding_bytes(size_t data_size) { return sizeof(uint64_t) - (data_size % sizeof(uint64_t)); }
    };

    struct footer
    {
        static uint64_t constexpr MAGIC_NUMBER = 0x677265676F727968;
        uint64_t block_size{};
        uint64_t block_count{};
        uint64_t entry_count{};
        uint64_t key_bytes{};
        uint64_t value_bytes{};
        uint64_t magic{MAGIC_NUMBER};
    };

    static std::chrono::steady_clock::time_point t_from(std::filesystem::path const & sstfile)
    {
        assert(std::filesystem::exists(sstfile));
        assert(std::filesystem::is_regular_file(sstfile));
        assert(sstfile.extension() == sstable::FILE_EXT);

        std::string msstr = sstfile.stem().generic_string();
        size_t steady_ns{};
        std::stringstream sstream(msstr);
        sstream >> steady_ns;

        return std::chrono::steady_clock::time_point{std::chrono::nanoseconds{steady_ns}};
    }

    static config_options config_from(std::filesystem::path const & sstfile)
    {
        assert(std::filesystem::exists(sstfile));
        assert(std::filesystem::is_regular_file(sstfile));
        assert(sstfile.extension() == sstable::FILE_EXT);

        std::ifstream f{sstfile, std::ios::binary};
        footer ftr;
        f.seekg(0, std::ios::end);
        size_t const file_size = f.tellg();
        f.seekg(file_size-sizeof(ftr), std::ios::beg);

        f >> ftr.block_size >> ftr.block_count >> ftr.entry_count >> ftr.key_bytes >> ftr.value_bytes >> ftr.magic;
        assert(ftr.magic == footer::MAGIC_NUMBER);

        return config_options{.max_block_size=ftr.block_size,.base_dir=sstfile.parent_path()};
    }

    // generates the header for the entry corresponding to a given node
    static entry_header header_from(std::string_view & prefix, memtable::skiptable::node const * n)
    {
        std::string_view key{n->key};
        entry_header hdr{};
        if (prefix.empty()) { prefix = key; }
        else
        {
            for ( ; hdr.prefix_bytes < prefix.length() && hdr.prefix_bytes < key.length(); hdr.prefix_bytes++)
            {
                if (prefix.at(hdr.prefix_bytes) != key.at(hdr.prefix_bytes)) { break; }
            }
        }

        hdr.suffix_bytes = key.length() - hdr.prefix_bytes;
        hdr.value_bytes = n->value()->size;

        return hdr;
    }
};

};
