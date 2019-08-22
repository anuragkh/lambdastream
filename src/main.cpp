#include "stream_operator.h"
#include "functors.h"

using namespace lambdastream;

int main(int, char **) {
  std::string channel = "redis";
  std::map<std::string, std::string>
      channel_args{{"redis_host", "127.0.0.1"}, {"redis_port", "6379"}, {"redis_db", "0"}};

  lambdastream::word_source src("/Users/anuragk/Work/Projects/MemoryMUX/ray-stream-benchmarks/words.txt", 100000, 100.0);
  lambdastream::split_words sw;
  lambdastream::count_words cw;
  lambdastream::empty_sink es;
  auto rr1 = std::make_shared<round_robin_partitioner>(1);
  auto hash = std::make_shared<hash_partitioner>(1);
  auto op1 = build_operator<source<word_source>>("0", {"1"}, channel, channel_args, rr1, src, 1, 64);
  auto op2 = build_operator<flat_map_operator<split_words>>("1", {"2"}, channel, channel_args, hash, sw, 1, 64);
  auto op3 = build_operator<reduce_operator<count_words>>("2", {"3"}, channel, channel_args, hash, cw, 1, 64);
  auto op4 = build_operator<sink<empty_sink>>("3", {}, channel, channel_args, hash, es, 1, 64);

  auto begin = time_utils::now_us();
  std::thread t4([&]() {
    op4.run();
  });

  std::thread t3([&]() {
    op3.run();
  });

  std::thread t2([&]() {
    op2.run();
  });

  std::thread t1([&]() {
    op1.run();
  });

  t1.join();
  t2.join();
  t3.join();
  t4.join();
  auto end = time_utils::now_us();
  double throughput = (100000.0 * 1e6) / static_cast<double>(end - begin);
  std::cerr << "Throughput: " << throughput;
}