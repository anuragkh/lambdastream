#ifndef LAMBDASTREAM_STREAM_OPERATOR_H
#define LAMBDASTREAM_STREAM_OPERATOR_H

#include <string>
#include <utility>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <map>

#include "channel.h"
#include "output_buffers.h"
#include "utils.h"
#include "constants.h"
#include "partitioner.h"

namespace lambdastream {

template<typename Functor>
class stream_operator {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;

  stream_operator(std::string op_id,
                  std::vector<std::string> output_op_ids,
                  functor_t op_fn,
                  partitioner_t partitioner,
                  size_t upstream_count,
                  size_t batch_size)
      : op_id_(std::move(op_id)),
        output_op_ids_(std::move(output_op_ids)),
        op_fn_(op_fn),
        partitioner_(std::move(partitioner)),
        upstream_count_(upstream_count),
        batch_size_(batch_size),
        num_processed_(0) {
  }

  virtual ~stream_operator() = default;

  virtual void run() = 0;

 protected:
  std::string op_id_;
  std::vector<std::string> output_op_ids_;
  functor_t op_fn_;
  partitioner_t partitioner_;
  size_t upstream_count_;
  size_t batch_size_;
  size_t num_processed_;
};

template<typename Functor>
class source : public stream_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef stream_operator<Functor> super;
  typedef std::shared_ptr<output_channel> output_channel_ptr_t;

  source(const std::string &op_id,
         const std::vector<std::string> &output_op_ids,
         functor_t op_fn,
         const partitioner_t& partitioner,
         size_t upstream_count,
         size_t batch_size,
         channel_builder builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size) {
    num_batches_ = 0;
    for (const auto &oid: this->output_op_ids_) {
      output_.push_back(builder.build_output_channel(oid));
    }
    output_buffers_ = new output_buffers(batch_size, output_);
  }

  ~source() {
    delete output_buffers_;
  }

  void run() {
    bool done = false;
    for (auto &out: output_) {
      out->connect();
    }
    while (!done) {
      record_batch_t batch = this->op_fn_(this->batch_size_);
      uint64_t timestamp = 0;
      if (num_batches_ % TIMESTAMP_INTERVAL == 0) {
        timestamp = time_utils::now_us();
      }
      output_buffers_->set_timestamp(timestamp);
      if (is_terminal(batch)) {
        done = true;
        output_buffers_->close();
      } else {
        this->partitioner_->partition_batch(output_buffers_, batch);
        this->num_processed_++;
        num_batches_ += batch.size();
      }
    }
  }

 private:
  size_t num_batches_;
  std::vector<output_channel_ptr_t> output_;
  output_buffers *output_buffers_;
};

template<typename Functor>
class sink : public stream_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef stream_operator<Functor> super;
  typedef std::shared_ptr<input_channel> input_channel_ptr_t;

  sink(const std::string &op_id,
       const std::vector<std::string> &output_op_ids,
       functor_t op_fn,
       const partitioner_t& partitioner,
       size_t upstream_count,
       size_t batch_size,
       channel_builder builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size) {
    input_ = builder.build_input_channel(this->op_id_);
    num_terminal_markers_ = 0;
  }

  void run() {
    bool done = false;
    input_->connect();
    while (!done) {
      auto data = serde_utils::deserialize(input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0) {
        auto now = time_utils::now_us();
        std::cerr << now - timestamp << std::endl;
        latencies_.push_back(now - timestamp);
      }
      if (is_terminal(batch)) {
        num_terminal_markers_++;
        if (num_terminal_markers_ == this->upstream_count_)
          done = true;
      } else {
        for (const auto &record: batch)
          this->op_fn_(record);
        this->num_processed_ += batch.size();
      }
    }
    // Dump latencies to file
    std::ofstream out("latency.txt");
    for (const auto &latency: latencies_)
      out << latency << "\n";
    out.close();
  }

 private:
  input_channel_ptr_t input_;
  size_t num_terminal_markers_;
  std::vector<uint64_t> latencies_;
};

template<typename Functor>
class single_output_operator : public stream_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;

  typedef stream_operator<Functor> super;
  typedef std::shared_ptr<input_channel> input_channel_ptr_t;
  typedef std::shared_ptr<output_channel> output_channel_ptr_t;

  single_output_operator(const std::string &op_id,
                         const std::vector<std::string> &output_op_ids,
                         functor_t op_fn,
                         const partitioner_t& partitioner,
                         size_t upstream_count,
                         size_t batch_size,
                         channel_builder builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size) {
    input_ = builder.build_input_channel(this->op_id_);
    for (const auto &oid: this->output_op_ids_) {
      output_.push_back(builder.build_output_channel(oid));
    }
    output_buffers_ = new output_buffers(batch_size, output_);
    num_terminal_markers_ = 0;
  }

  ~single_output_operator() {
    delete output_buffers_;
  }

 protected:
  void connect() {
    this->input_->connect();
    for (auto &out: this->output_) {
      out->connect();
    }
  }

  input_channel_ptr_t input_;
  size_t num_terminal_markers_;
  std::vector<output_channel_ptr_t> output_;
  output_buffers *output_buffers_;
  std::unordered_map<key_t, value_t, hash_t> state_;
};

template<typename Functor>
class map_operator : public single_output_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;

  typedef single_output_operator<Functor> super;

  map_operator(const std::string &op_id,
               const std::vector<std::string> &output_op_ids,
               functor_t op_fn,
               const partitioner_t& partitioner,
               size_t upstream_count,
               size_t batch_size,
               channel_builder builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size, builder) {
  }

  void run() {
    this->connect();
    bool done = false;
    while (!done) {
      auto data = serde_utils::deserialize(this->input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0)
        this->output_buffers_->set_timestamp(timestamp);
      if (is_terminal(batch)) {
        this->num_terminal_markers_++;
        if (this->num_terminal_markers_ == this->upstream_count_) {
          done = true;
          this->output_buffers_->close();
        }
      } else {
        for (size_t i = 0; i < batch.size(); ++i)
          batch[i] = this->op_fn_(batch[i]);
        this->partitioner_->partition_batch(this->output_buffers_, batch);
      }
    }
  }
};

template<typename Functor>
class flat_map_operator : public single_output_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef single_output_operator<Functor> super;

  flat_map_operator(const std::string &op_id,
                    const std::vector<std::string> &output_op_ids,
                    functor_t op_fn,
                    const partitioner_t& partitioner,
                    size_t upstream_count,
                    size_t batch_size,
                    const channel_builder& builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size, builder) {
  }

  void run() {
    this->connect();
    bool done = false;
    while (!done) {
      auto data = serde_utils::deserialize(this->input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0) {
        this->output_buffers_->set_timestamp(timestamp);
      }
      if (is_terminal(batch)) {
        this->num_terminal_markers_++;
        if (this->num_terminal_markers_ == this->upstream_count_) {
          done = true;
          this->output_buffers_->close();
        }
      } else {
        for (size_t i = 0; i < batch.size(); ++i) {
          record_batch_t processed_batch = this->op_fn_(batch[i]);
          this->partitioner_->partition_batch(this->output_buffers_, processed_batch);
        }
      }
    }
  }
};

template<typename Functor>
class filter_operator : public single_output_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef single_output_operator<Functor> super;

  filter_operator(const std::string &op_id,
                  const std::vector<std::string> &output_op_ids,
                  functor_t op_fn,
                  const partitioner_t& partitioner,
                  size_t upstream_count,
                  size_t batch_size,
                  const channel_builder& builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size, builder) {
  }

  void run() {
    this->connect();
    bool done = false;
    while (!done) {
      auto data = serde_utils::deserialize(this->input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0)
        this->output_buffers_->set_timestamp(timestamp);
      if (is_terminal(batch)) {
        this->num_terminal_markers_++;
        if (this->num_terminal_markers_ == this->upstream_count_) {
          done = true;
          this->output_buffers_->close();
        }
      } else {
        record_batch_t filtered_batch;
        for (size_t i = 0; i < batch.size(); ++i)
          if (this->op_fn_(batch[i])) filtered_batch.push_back(batch[i]);
        this->partitioner_->partition_batch(this->output_buffers_, filtered_batch);
      }
    }
  }
};

template<typename Functor>
class key_by_operator : public single_output_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef single_output_operator<Functor> super;

  key_by_operator(const std::string &op_id,
                  const std::vector<std::string> &output_op_ids,
                  functor_t op_fn,
                  const partitioner_t& partitioner,
                  size_t upstream_count,
                  size_t batch_size,
                  const channel_builder &builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size, builder) {
  }

  void run() {
    this->connect();
    bool done = false;
    while (!done) {
      auto data = serde_utils::deserialize(this->input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0)
        this->output_buffers_->set_timestamp(timestamp);
      if (is_terminal(batch)) {
        this->num_terminal_markers_++;
        if (this->num_terminal_markers_ == this->upstream_count_) {
          done = true;
          this->output_buffers_->close();
        }
      } else {
        for (size_t i = 0; i < batch.size(); ++i)
          batch[i].insert(batch[i].begin(), this->op_fn_(batch[i]));
        this->partitioner_->partition_batch(this->output_buffers_, batch);
      }
    }
  }
};

template<typename Functor>
class reduce_operator : public single_output_operator<Functor> {
 public:
  typedef std::shared_ptr<partitioner> partitioner_t;
  typedef Functor functor_t;
  typedef single_output_operator<Functor> super;

  reduce_operator(const std::string &op_id,
                  const std::vector<std::string> &output_op_ids,
                  functor_t op_fn,
                  const partitioner_t& partitioner,
                  size_t upstream_count,
                  size_t batch_size,
                  const channel_builder& builder)
      : super(op_id, output_op_ids, op_fn, partitioner, upstream_count, batch_size, builder) {
  }

  void run() {
    this->connect();
    bool done = false;
    while (!done) {
      auto data = serde_utils::deserialize(this->input_->get());
      auto timestamp = data.first;
      auto batch = data.second;
      if (timestamp > 0) {
        this->output_buffers_->set_timestamp(timestamp);
      }
      if (is_terminal(batch)) {
        this->num_terminal_markers_++;
        if (this->num_terminal_markers_ == this->upstream_count_) {
          done = true;
          this->output_buffers_->close();
        }
      } else {
        for (size_t i = 0; i < batch.size(); ++i) {
          auto kv = as_kv_pair(batch[i]);
          auto it = this->state_.find(kv.first);
          value_t new_value;
          if (it == this->state_.end()) {
            new_value = kv.second;
          } else {
            new_value = this->op_fn_(it->second, kv.second);
          }
          this->state_[kv.first] = new_value;
          batch[i] = std::make_pair(kv.first, new_value);
        }
        this->partitioner_->partition_batch(this->output_buffers_, batch);
      }
    }
  }
};

class operator_wrapper {
 public:
  template<typename Operator>
  explicit operator_wrapper(std::shared_ptr<Operator> op) {
    fn_ = [op]() { op->run(); };
  }

  void run() {
    fn_();
  }

 private:
  std::function<void(void)> fn_;
};

template<typename Operator, typename Functor>
operator_wrapper build_operator(const std::string &op_id,
                                const std::vector<std::string> &out_op_ids,
                                const std::string &channel,
                                const std::map<std::string, std::string> &channel_args,
                                std::shared_ptr<partitioner> part,
                                Functor op_fn,
                                size_t upstream_count,
                                size_t batch_size) {
  channel_builder builder(channel, channel_args);
  auto op = std::make_shared<Operator>(op_id, out_op_ids, op_fn, part, upstream_count, batch_size, builder);
  return operator_wrapper(op);
}

}

#endif //LAMBDASTREAM_STREAM_OPERATOR_H
