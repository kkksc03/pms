#pragma once
#include <string>
#include <utility>
#include "base/third_party/sarray.h"
#include "lib/labeled_sample.hpp"
namespace csci5570 {
namespace lib {

// Consider both sparse and dense feature abstraction
// You may use Eigen::Vector and Eigen::SparseVector template

//std::pair(int,floatf)
class SVMSample : public LabeledSample<third_party::SArray< int >, int> {
public:
  std::string toString(){
    std::string result="Label: "+std::to_string(y_)+" Feature:";
    for (int i = 0; i < x_.size(); i++) {
      result=result+" "+/*std::to_string(x_[i].first)+":"+std::to_string(x_[i].second)*/std::to_string(x_[i]);
    }
    return result;
  }
  /*
  void test() {
    for (int i = 0; i < 5; i++) {
      x_.push_back(i);
    }
    y_ = 2;
    
  }
  */

  //  public:
  //   vector<int> x_;
  //   Label y_;
};  // class LabeledSample

}  // namespace lib
}  // namespace csci5570
