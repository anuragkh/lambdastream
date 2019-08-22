#ifndef LAMBDASTREAM_FUNCTORS_H
#define LAMBDASTREAM_FUNCTORS_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "record.h"
#include "utils.h"
#include "rate_limiter.h"

namespace lambdastream {

struct word_source {
  static const size_t SENTENCE_LENGTH = 100;
  word_source(const std::string &words_file, size_t num_records, double rate)
      : num_records_(num_records), rate_(rate), num_generated_(0) {
    std::ifstream in(words_file.c_str());
    if (!in)
      throw std::invalid_argument("Cannot open file: " + words_file);
    std::string str;
    while (std::getline(in, str)) {
      if (str.size() > 0)
        words_.push_back(str);
    }
    in.close();
    limiter_ = std::make_shared<rate_limiter>(rate_);
  }

  record_batch_t operator()(size_t batch_size) {
    if (num_generated_ >= num_records_) {
      std::cerr << "Generated all records" << std::endl;
      return EMPTY();
    }
    if (rate_ > 0.0) {
      limiter_->acquire();
    }
    record_batch_t sentences(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
      std::stringstream ss;
      for (size_t j = 0; j < SENTENCE_LENGTH; ++j) {
        ss << words_[rand_utils::rand_uint32(words_.size())] << " ";
      }
      sentences[i] = ss.str();
    }
    num_generated_ += batch_size;
    return sentences;
  }

 private:
  std::vector<std::string> words_;
  size_t num_records_;
  double rate_;
  size_t num_generated_;
  std::shared_ptr<rate_limiter> limiter_;
};

struct split_words {
  record_batch_t operator()(const record_t &record) {
    auto sentence = as<std::string>(record);
    std::stringstream ss(sentence);
    std::string item;
    record_batch_t batch;
    while (std::getline(ss, item, ' ')) {
      kv_pair_t word_count = std::make_pair(item, 1);
      batch.push_back(word_count);
    }
    return batch;
  }
};

struct count_words {
  value_t operator()(const value_t &value1, const value_t &value2) {
    return value_as<int>(value1) + value_as<int>(value2);
  }
};

struct empty_sink {
  void operator()(const record_t &) {
  }
};

}

#endif //LAMBDASTREAM_FUNCTORS_H
