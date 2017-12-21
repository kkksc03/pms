#pragma once

#include <boost/tokenizer.hpp>
#include <string>
#include <utility>
#include "boost/utility/string_ref.hpp"
#include "lib/kdd_sample.hpp"
#include "lib/svm_sample.hpp"
// For testing
// #include "glog/logging.h"
namespace csci5570 {
namespace lib {

template <typename Sample, typename DataStore>
class Parser {
 public:
  /**
   * Parsing logic for one line in file
   *
   * @param line    a line read from the input file
   */
  static Sample parse_libsvm(boost::string_ref line, int n_features) {
    // check the LibSVM format and complete the parsing
    // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
    // so far we tried all the tree and found std::strtok_r is fastest :)
    Sample temp_sample = SVMSample();

    boost::char_separator<char> sep(" :");
    boost::tokenizer<boost::char_separator<char>> tok(line, sep);

    // boost::tokenizer<> tok(line);
    int count = 0;
    for (boost::tokenizer<boost::char_separator<char>>::iterator beg = tok.begin(); beg != tok.end(); ++beg) {
      int index;
      double value;
      if (count == 0) {
        if (line.substr(0, 1) == "+") {
          // LOG(INFO) << "Positive";
          temp_sample.y_ = 1;
        } else {
          // LOG(INFO) << "Negative";
          temp_sample.y_ = -1;
        }
      } else if (count % 2 == 1) {
        index = stoi(*beg);
        // LOG(INFO) << *beg;
        // LOG(INFO)<<index;
        temp_sample.x_.push_back(index);
      } else {
        value = stof(*beg);
        // temp_sample.x_.push_back(std::make_pair(index,value));
      }
      count++;
    }
    return temp_sample;
  }
  static Sample parse_kdd(boost::string_ref line, int n_features) {
    // check the kdd format and complete the parsing
    Sample temp_sample = KddSample();

    boost::char_separator<char> sep(" :");
    boost::tokenizer<boost::char_separator<char>> tok(line, sep);

    // boost::tokenizer<> tok(line);
    int count = 0;
    int index = 666;
    double value = 0.66;
    // LOG(INFO) << "Original string" << line;
    for (boost::tokenizer<boost::char_separator<char>>::iterator beg = tok.begin(); beg != tok.end(); ++beg) {
      if (count == 0) {
        if (line.substr(0, 1) == "1") {
          // LOG(INFO) << "Positive";
          temp_sample.y_ = 1;
        } else {
          // LOG(INFO) << "Negative";
          temp_sample.y_ = -1;
        }
      } else if (count % 2 == 1) {
        index = stoi(*beg);
      } else {
        value = stod(*beg);
        // LOG(INFO) << "String:" << (*beg);
        // LOG(INFO) << "Value" << value;
        temp_sample.x_.push_back(std::make_pair(index, value));
      }
      count++;
    }
    return temp_sample;
  }
  static Sample parse_mnist(boost::string_ref line, int n_features) {
    // check the MNIST format and complete the parsing
  }

  // You may implement other parsing logic

};  // class Parser

}  // namespace lib
}  // namespace csci5570
