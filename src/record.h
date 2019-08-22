#ifndef STORAGE_BENCH_RECORD_H
#define STORAGE_BENCH_RECORD_H

#include <vector>
#include <string>
#include <boost/any.hpp>
#include <boost/variant.hpp>
#include <boost/serialization/variant.hpp>

namespace lambdastream {

typedef boost::variant<int8_t,
                       int16_t,
                       int32_t,
                       int64_t,
                       uint8_t,
                       uint16_t,
                       uint32_t,
                       uint64_t,
                       float,
                       double,
                       std::string> primitive_t;

typedef primitive_t key_t;
typedef boost::hash<key_t> hash_t;
typedef std::pair<primitive_t, primitive_t> primitive_pair_t;
typedef std::vector<primitive_t> primitive_list_t;
typedef boost::variant<primitive_t, primitive_pair_t, primitive_list_t> value_t;
typedef std::pair<key_t, value_t> kv_pair_t;
typedef boost::variant<value_t, kv_pair_t> record_t;
typedef std::vector<record_t> record_batch_t;

inline bool is_terminal(const record_batch_t &batch) {
  return batch.empty();
}

inline value_t as_value(const record_t &record) {
  return boost::get<value_t>(record);
}

kv_pair_t as_kv_pair(const record_t &record) {
  return boost::get<kv_pair_t>(record);
}

template<typename T>
inline T as(const record_t &record) {
  return boost::get<T>(boost::get<primitive_t>(as_value(record)));
}

template<typename T>
inline T value_as(const value_t &value) {
  return boost::get<T>(boost::get<primitive_t>(value));
}

template<typename T>
inline T key_as(const key_t &key) {
  return boost::get<T>(key);
}

template<typename T>
inline T as_primitive(const value_t &record) {
  return boost::get<T>(boost::get<value_t>(record));
}

inline record_batch_t EMPTY() {
  static record_batch_t ___empty{};
  return ___empty;
}

}

#endif //STORAGE_BENCH_RECORD_H
