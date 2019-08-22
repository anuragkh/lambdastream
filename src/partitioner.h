#ifndef LAMBDASTREAM_PARTITIONER_H
#define LAMBDASTREAM_PARTITIONER_H

#include <boost/functional/hash.hpp>
#include "output_buffers.h"

namespace lambdastream {

class partitioner {
 public:
  explicit partitioner(size_t num_out) : num_out_(num_out) {}
  virtual ~partitioner() = default;

  virtual void partition_batch(output_buffers *buffers, const record_batch_t &batch) = 0;
 protected:
  size_t num_out_;
};

class round_robin_partitioner : public partitioner {
 public:
  explicit round_robin_partitioner(size_t num_out) : partitioner(num_out), idx_(0) {}

  void partition_batch(output_buffers *buffers, const record_batch_t &batch) override {
    for (const record_t &record: batch)
      buffers->append(idx_++ % num_out_, record);
  }

 private:
  size_t idx_;
};

class hash_partitioner: public partitioner {
 public:
  explicit hash_partitioner(size_t num_out) : partitioner(num_out) {
  }

  void partition_batch(output_buffers *buffers, const record_batch_t &batch) override {
    for (const record_t &record: batch)
      buffers->append(hash_(as_kv_pair(record).first) % num_out_, record);
  }

 private:
  hash_t hash_;
};
}

#endif //LAMBDASTREAM_PARTITIONER_H
