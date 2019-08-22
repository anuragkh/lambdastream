#ifndef LAMBDASTREAM_CHANNEL_H
#define LAMBDASTREAM_CHANNEL_H

#include <map>
#include <utility>
#include <cpp_redis/cpp_redis>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/storage/client/fifo_queue_client.h>

namespace lambdastream {

class input_channel {
 public:
  explicit input_channel(std::string name) : name_(std::move(name)) {}

  virtual ~input_channel() = default;

  virtual void connect() = 0;

  virtual std::string get() = 0;

 protected:
  std::string name_;
};

class output_channel {
 public:
  explicit output_channel(std::string name) : name_(std::move(name)) {}

  virtual ~output_channel() = default;

  virtual void connect() = 0;

  virtual void put(const std::string &data) = 0;

 protected:
  std::string name_;
};

class redis_input_channel : public input_channel {
 public:
  redis_input_channel(std::string name, const std::map<std::string, std::string> &channel_args)
      : input_channel(std::move(name)) {
    host_ = channel_args.at("redis_host");
    port_ = std::stoi(channel_args.at("redis_port"));
    conn_ = nullptr;
  }

  void connect() override {
    conn_ = std::make_shared<cpp_redis::client>();
    conn_->connect(host_, port_);
  }

  std::string get() override {
    auto fut = conn_->blpop({name_}, 0);
    conn_->commit();
    auto reply = fut.get();
    if (reply.is_error()) {
      throw std::runtime_error(reply.error());
    }
    return reply.as_array()[1].as_string();
  }

 private:
  std::string host_;
  int port_;
  std::shared_ptr<cpp_redis::client> conn_;
};

class redis_output_channel : public output_channel {
 public:
  redis_output_channel(std::string name, const std::map<std::string, std::string> &channel_args)
      : output_channel(std::move(name)) {
    host_ = channel_args.at("redis_host");
    port_ = std::stoi(channel_args.at("redis_port"));
    conn_ = nullptr;
  }

  void connect() override {
    conn_ = std::make_shared<cpp_redis::client>();
    conn_->connect(host_, port_);
  }

  void put(const std::string &data) override {
    auto fut = conn_->rpush(name_, {data});
    conn_->commit();
    auto reply = fut.get();
    if (reply.is_error()) {
      throw std::runtime_error(reply.error());
    }
  }

 private:
  std::string host_;
  int port_;
  std::shared_ptr<cpp_redis::client> conn_;
};

class jiffy_input_channel : public input_channel {
 public:
  jiffy_input_channel(std::string name, const std::map<std::string, std::string> &channel_args)
      : input_channel(std::move(name)) {
    host_ = channel_args.at("jiffy_host");
    service_port_ = std::stoi("jiffy_service_port");
    lease_port_ = std::stoi("jiffy_lease_port");
  }

  void connect() override {
    jiffy::client::jiffy_client jc(host_, service_port_, lease_port_);
    conn_ = jc.open_fifo_queue("/" + name_);
  }

  std::string get() override {
    return conn_->dequeue();
  }

 private:
  std::string host_;
  int service_port_;
  int lease_port_;
  std::shared_ptr<jiffy::storage::fifo_queue_client> conn_;
};

class jiffy_output_channel : public output_channel {
 public:
  jiffy_output_channel(std::string name, const std::map<std::string, std::string> &channel_args)
      : output_channel(std::move(name)) {
    host_ = channel_args.at("jiffy_host");
    service_port_ = std::stoi("jiffy_service_port");
    lease_port_ = std::stoi("jiffy_lease_port");
  }

  void connect() override {
    jiffy::client::jiffy_client jc(host_, service_port_, lease_port_);
    conn_ = jc.open_fifo_queue("/" + name_);
  }

  void put(const std::string &data) override {
    conn_->enqueue(data);
  }
 private:
  std::string host_;
  int service_port_;
  int lease_port_;
  std::shared_ptr<jiffy::storage::fifo_queue_client> conn_;
};

class channel_builder {
 public:
  typedef std::shared_ptr<input_channel> input_channel_ptr_t;
  typedef std::shared_ptr<output_channel> output_channel_ptr_t;

  explicit channel_builder(std::string type, std::map<std::string, std::string> channel_args)
      : type_(std::move(type)), channel_args_(std::move(channel_args)) {
  }

  input_channel_ptr_t build_input_channel(const std::string &name) {
    if (type_ == "redis") {
      return std::make_shared<redis_input_channel>(name, channel_args_);
    } else if (type_ == "jiffy") {
      return std::make_shared<jiffy_input_channel>(name, channel_args_);
    }
    throw std::invalid_argument("No such channel: " + type_);
  }

  output_channel_ptr_t build_output_channel(const std::string &name) {
    if (type_ == "redis") {
      return std::make_shared<redis_output_channel>(name, channel_args_);
    } else if (type_ == "jiffy") {
      return std::make_shared<jiffy_output_channel>(name, channel_args_);
    }
    throw std::invalid_argument("No such channel: " + type_);
  }

 private:
  std::string type_;
  std::map<std::string, std::string> channel_args_;
};

}

#endif //LAMBDASTREAM_CHANNEL_H
