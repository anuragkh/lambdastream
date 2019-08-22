#ifndef LAMBDASTREAM_OUTPUT_BUFFERS_H
#define LAMBDASTREAM_OUTPUT_BUFFERS_H

#include <cstddef>
#include <vector>
#include "channel.h"
#include "record.h"
#include "utils.h"

namespace lambdastream {

class output_buffers {
 public:
  typedef std::shared_ptr<output_channel> output_channel_ptr_t;

  output_buffers(size_t batch_size, std::vector<output_channel_ptr_t> &output)
      : timestamp_(0),
        batch_size_(batch_size),
        output_(output),
        num_out_(output.size()),
        output_buffers_(output.size()) {
  }

  void set_timestamp(size_t timestamp) {
    timestamp_ = timestamp;
  }

  void append(size_t i, const record_t &record) {
    output_buffers_[i].push_back(record);
    if (output_buffers_[i].size() == batch_size_) {
      output_[i]->put(serde_utils::serialize(timestamp_, output_buffers_[i]));
      output_buffers_[i].clear();
      timestamp_ = 0;
    }
  }

  void close() {
    for (size_t i = 0; i < num_out_; ++i) {
      if (!output_buffers_[i].empty())
        output_[i]->put(serde_utils::serialize(timestamp_, output_buffers_[i]));
      output_[i]->put(serde_utils::serialize(timestamp_, EMPTY()));
    }
  }

 private:
  uint64_t timestamp_;
  size_t batch_size_;
  std::vector<output_channel_ptr_t> &output_;
  size_t num_out_;
  std::vector<std::vector<record_t>> output_buffers_;
};

}

#endif //LAMBDASTREAM_OUTPUT_BUFFERS_H
