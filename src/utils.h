#ifndef LAMBDASTREAM_UTILS_H
#define LAMBDASTREAM_UTILS_H

#include <cstdint>
#include <random>
#include <cstdio>
#include <ctime>
#include <chrono>
#include <string>
#include <boost/utility/binary.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

namespace lambdastream {

class time_utils {
 public:
  /**
   * @brief Fetch current time in nanoseconds
   * @return Time
   */
  static uint64_t now_ns() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in microseconds
   * @return Time
   */
  static uint64_t now_us() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in milliseconds
   * @return Time
   */
  static uint64_t now_ms() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in seconds
   * @return Time
   */
  static uint64_t now_s() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
  }

  template<typename F, typename ...Args>
  static uint64_t time_function_ns(F &&f, Args &&... args) {
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    f(std::forward<Args>(args)...);
    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  }
};

class serde_utils {
 public:
  static std::string serialize(uint64_t timestamp, const record_batch_t &batch) {
    std::stringstream ss;
    boost::archive::binary_oarchive archive(ss);
    archive << timestamp;
    archive << batch;
    return ss.str();
  }

  static std::pair<uint64_t, record_batch_t> deserialize(const std::string &data) {
    std::stringstream ss(data);
    boost::archive::binary_iarchive archive(ss);
    std::pair<uint64_t, record_batch_t> out;
    archive >> out.first;
    archive >> out.second;
    return out;
  }
};

class string_utils {
 public:
  /**
   * @brief Split string
   * @param s String
   * @param delim Separation symbol
   * @param count Total parts
   * @return Separated strings
   */

  inline static std::vector<std::string> split(const std::string &s, char delim,
                                               size_t count) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    size_t i = 0;
    while (std::getline(ss, item, delim) && i < count) {
      elems.push_back(std::move(item));
      i++;
    }
    while (std::getline(ss, item, delim))
      elems.back() += item;
    return elems;
  }

  /**
   * @brief Split string with default count
   * @param s String
   * @param delim Separation symbol
   * @return Separated strings
   */

  inline static std::vector<std::string> split(const std::string &s,
                                               char delim) {
    return split(s, delim, UINT64_MAX);
  }

  /**
   * @brief Combine multiple strings to one string
   * @param v Strings
   * @param delim Separation symbol
   * @return String
   */

  inline static std::string mk_string(const std::vector<std::string> &v,
                                      const std::string &delim) {
    std::string str = "";
    size_t i = 0;
    for (; i < v.size() - 1; i++) {
      str += v[i] + delim;
    }
    return str + v[i];
  }

  template<typename functor>
  /**
   * @brief Transform string according to transformation function
   * @tparam functor Transformation function type
   * @param str String
   * @param f Transformation function
   * @return String after transform
   */
  inline static std::string transform(const std::string &str, functor f) {
    std::string out;
    out.resize(str.length());
    std::transform(str.begin(), str.end(), out.begin(), f);
    return out;
  }

  /**
   * @brief Transform string to upper characters
   * @param str String
   * @return String after transformation
   */

  inline static std::string to_upper(const std::string &str) {
    return transform(str, ::toupper);
  }

  /**
   * @brief Transform string to lower characters
   * @param str String
   * @return String after transformation
   */

  inline static std::string to_lower(const std::string &str) {
    return transform(str, ::tolower);
  }

  template<typename T>
  /**
   * @brief Lexical cast of string
   * @tparam T Cast type
   * @param s String
   * @return String after cast
   */

  inline static T lexical_cast(const std::string &s) {
    std::stringstream ss(s);

    T result;
    if ((ss >> result).fail() || !(ss >> std::ws).eof()) {
      throw std::bad_cast();
    }

    return result;
  }
};

template<>
/**
 * @brief Lexical cast of string to bool
 * @param s String
 * @return Bool after cast
 */

inline bool string_utils::lexical_cast<bool>(const std::string &s) {
  std::stringstream ss(to_lower(s));

  bool result;
  if ((ss >> std::boolalpha >> result).fail() || !(ss >> std::ws).eof()) {
    throw std::bad_cast();
  }

  return result;
}

class rand_utils {
 public:
  /**
   * @brief Generate int64 random number from 0 to max
   * @param max Max number
   * @return Int64 random number
   */

  static int64_t rand_int64(const int64_t &max) {
    return rand_int64(0, max);
  }

  /**
   * @brief Generate int64 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Int64 random number
   */

  static int64_t rand_int64(const int64_t &min, const int64_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<int64_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate uint64 random number from 0 to max
   * @param max Max number
   * @return Uint64 random number
   */

  static uint64_t rand_uint64(const uint64_t &max) {
    return rand_uint64(0, max);
  }

  /**
   * @brief Generate uint64 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Uint64 random number
   */


  static uint64_t rand_uint64(const uint64_t &min, const uint64_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<uint64_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate int32 random number from 0 to max
   * @param max Max number
   * @return Int32 random number
   */

  static int32_t rand_int32(const int32_t &max) {
    return rand_int32(0, max);
  }

  /**
   * @brief Generate int32 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Int32 random number
   */

  static int32_t rand_int32(const int32_t &min, const int32_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<int32_t> distribution(min, max);
    return distribution(generator);
  }

  /**
   * @brief Generate uint32 random number from 0 to max
   * @param max Max number
   * @return uint32 random number(in uint64 form)
   */

  static uint64_t rand_uint32(const uint32_t &max) {
    return rand_uint32(0, max);
  }

  /**
   * @brief Generate uint32 random number from range [min, max]
   * @param min Min
   * @param max Max
   * @return Uint32 random number
   */


  static uint64_t rand_uint32(const uint32_t &min, const uint32_t &max) {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 generator(rd());
    std::uniform_int_distribution<uint32_t> distribution(min, max);
    return distribution(generator);
  }
};

}

#endif //LAMBDASTREAM_UTILS_H
