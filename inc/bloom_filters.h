#pragma once

#include <climits>
#include <cmath>
#include <vector>
#include <xxhash64.h>
#include <array>

#include <iostream>

namespace bloom_filters {

// Implements a simple bloom filter: https://en.wikipedia.org/wiki/Bloom_filter
// The current implementation computes N hashes for each operation, based on the
// filter parameters. A potential optimization ("Kirsch-Mitzenmacher") would
// allow for only 2 hashes, and use double hashing to compute the remaining N-2
// bits. Additionally, this implementation is not optimized for cpu-cache hits,
// as locality is very low
struct static_filter
{
  // Simple struct to hold filter specification parameters
  struct parameters
  {
    // Maximum allowable false-positive rate. 0 < target_error_rate < 1
    // Defaults to 1% fpr, as a acceptable trade-off for general scenarios
    static double constexpr DEFAULT_FPR = 0.01;
    double target_error_rate{ DEFAULT_FPR };

    // Maximum elements that can be inserted before fpr exceeds
    // target_error_rate Requires capacity > 0. Defaults to 1000 elements, but
    // should typically be modified based on usage.
    static size_t constexpr DEFAULT_CAPACITY = 1000;
    size_t capacity{ DEFAULT_CAPACITY };

    // Seeds to use for hashing on filter operations.
    // Caller must ensure that each seed is unique, or filter performance will
    // be degraded. Caller can use "hash_count" to determine how many seeds to
    // generate for a given fpr. In practice, MAX_HASH_COUNT will allow for
    // extremely low fpr, approxmately 1/100,000,000.
    static size_t constexpr MAX_HASH_COUNT = 32;
    std::array<uint64_t, MAX_HASH_COUNT> hash_seeds;

    // Calculation for optimal hash count per operation to achieve desired fpr
    static constexpr size_t hash_count(double const target_error_rate) { return ceil(log2(1.0 / target_error_rate)); }

    // Calculation for the optimal size of each filter slice (in bits) to achieve fpr at given capacity.
    static constexpr size_t slice_bits(double const target_error_rate, size_t const capacity)
    {
      double const numerator = capacity * abs(log(target_error_rate));
      double const denominator = hash_count(target_error_rate) * log(2) * log(2);
      return ceil(numerator / denominator);
    }
  };

  static_filter(parameters const& params)
    : params(params)
    , mem(parameters::hash_count(params.target_error_rate),
          parameters::slice_bits(params.target_error_rate, params.capacity))
  {
  }

  // returns true while there fewer elements in the filter than its capacity
  // after this point, fpr drastically worsens with each element added
  bool good() const { return this->element_count < this->params.capacity; }

  bool count() const { return this->element_count; }

  // Returns the bit index for the ith hash of given data
  // Requires i < this->mem.slices
  size_t hash_i(size_t const i, void* data, size_t const data_size) const
  {
    uint64_t const hash_i = XXHash64::hash(data, data_size, this->params.hash_seeds.at(i));
    return (hash_i % this->mem.bps) + (i * this->mem.bps);
  }

  // Returns false if we are certain the element is not in the filter, otherwise true.
  // Might return a false positive due to hash collisions.
  bool might_contain(void* data, size_t data_size) const
  {
    // break out early and return false as soon as we don't see an expected hash
    // bit
    for (size_t i = 0; i < this->mem.slices; i++) {
      if (!this->mem.check(this->hash_i(i, data, data_size))) {
        return false;
      }
    }

    // if all expected bits are set, we probably have inserted the value
    return true;
  }

  // inserts an element into the filter, returning true if the key was already inserted
  bool insert(void* data, size_t const data_size)
  {
    bool all_set = true;
    for (size_t i = 0; i < this->mem.slices; i++) {
      all_set = this->mem.check_set(this->hash_i(i, data, data_size)) && all_set;
    }

    if (!all_set) {
      this->element_count += 1;
    }
    return all_set;
  }

  // inserts a new element into the filter,
  // where the element is known not to have been inserted previously
  void insert_new(void* data, size_t const data_size)
  {
    this->element_count += 1;
    for (size_t i = 0; i < this->mem.slices; i++) {
      this->mem.set(this->hash_i(i, data, data_size));
    }
  }

  // allow owners to reference the parameters used to create the filter
  parameters const params;

private:
  // Helper struct to encapsulate memory operations on filter bits
  struct memory
  {
    memory(size_t const slice_count, size_t const bits_per_slice)
      : slices(slice_count)
      , bps(bits_per_slice)
      , bit_count(slices * bps)
    {
      // Ensure we have enough bytes for bits lost to integer division
      size_t const byte_count = this->bit_count / CHAR_BIT + (this->bit_count % CHAR_BIT != 0);

      // zero-initialize memory
      this->bits.resize(byte_count);
      std::fill(this->bits.begin(), this->bits.end(), std::byte{ 0 });
    }

    static size_t constexpr byte_idx(size_t bit) { return bit / CHAR_BIT; }
    static std::byte constexpr sub_bit(size_t bit) { return std::byte(1 << (bit % CHAR_BIT)); }

    // sets a given bit to 1
    void set(size_t bit_idx) { this->bits[byte_idx(bit_idx)] |= sub_bit(bit_idx); }

    // Returns true if a bit is set, false otherwise
    bool check(size_t bit_idx) const { return (this->bits[byte_idx(bit_idx)] & sub_bit(bit_idx)) != std::byte{ 0 }; }

    // sets a given bit to 1, returns true if the bit was previously set
    bool check_set(size_t bit_idx)
    {
      bool const old = this->check(bit_idx);
      this->set(bit_idx);
      return old;
    }

    size_t const slices;
    size_t const bps;
    size_t const bit_count;

  private:
    std::vector<std::byte> bits{};
  };

  memory mem;
  size_t element_count{};
};

// Implements a scalable bloom filter as presented in
// P. Almeida, C.Baquero, N. Preguiça, D. Hutchison, Scalable Bloom Filters, (GLOBECOM 2007), IEEE, 2007.
// Uses a list of dynamically created "static_filter"s to increase capacity as new elements are added.
struct scalable_filter
{
  // Simple struct to hold filter specification parameters
  struct parameters : public static_filter::parameters
  {
    // As further sub-filters are created, the target fpr is adjusted by this factor.
    // Requires 0.0 < tightening_ratio < 1.0. Typical values are between 0.8 and 0.9
    double tightening_ratio{ 0.9 };

    // As further sub-filters are created, the capacity is scaled by this factor
    // Smaller values are more space efficient but decrease performance.
    // Requires scaling_factor > 1
    size_t scaling_factor{ 2 };
  };

  scalable_filter(parameters const& params)
    : params(params)
  {
    filters.emplace_back(params);
  }

  parameters const params;

  size_t capacity() const
  {
    size_t capacity = 0;
    for (auto const& f : this->filters) {
      capacity += f.params.capacity;
    }

    return capacity;
  }

  size_t count() const
  {
    size_t count = 0;
    for (auto const& f : this->filters) {
      count += f.count();
    }

    return count;
  }

  bool might_contain(void* data, size_t const data_size) const
  {
    // simply test for membership in all sub-filters
    for (auto const& f : this->filters) {
      if (f.might_contain(data, data_size))
        return true;
    }

    return false;
  }

  // inserts an element into the filter, returning true if the key was previously  inserted
  bool insert(void* data, size_t const data_size)
  {
    // Don't do any work if the element is already a filter member
    if (this->might_contain(data, data_size)) {
      return true;
    } else if (!this->filters.back().good()) {
      // Our existing filters are all full - add a new filter with scaled parameters
      static_filter::parameters new_params{ this->filters.back().params };
      new_params.capacity *= this->params.scaling_factor;
      new_params.target_error_rate *= this->params.tightening_ratio;

      this->filters.emplace_back(new_params);
    }

    this->filters.back().insert_new(data, data_size);
    return false;
  }

private:
  std::vector<static_filter> filters;
};

} // namespace bloom_filters
