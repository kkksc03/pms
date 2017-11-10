#include "server/util/progress_tracker.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) {
  // TODO
  min_clock_ = 0;
  for (int i = 0; i < tids.size(); i++) {
    this->progresses_.insert(std::map<int, int>::value_type(tids[i], 0));
  }
}

int ProgressTracker::AdvanceAndGetChangedMinClock(int tid) {
  // TODO
  int newProgress =this->progresses_[tid] + 1;
  if (IsUniqueMin(tid) == true)
  // The min clock need to be updated
  {
    this->progresses_[tid] = newProgress;
    min_clock_++;
    return min_clock_;
  } else {
    this->progresses_[tid] = newProgress;
    return -1;
  }
}

int ProgressTracker::GetNumThreads() const {
  return this->progresses_.size();
  // TODO
}

int ProgressTracker::GetProgress(int tid) const {
  return this->progresses_.at(tid);
  // TODO
}

int ProgressTracker::GetMinClock() const {
  return this->min_clock_;
  // TODO
}

bool ProgressTracker::IsUniqueMin(int tid) const {
  if (this->progresses_.at(tid) != GetMinClock()) {
    // It is not min
    return false;
  }
  // std::map<int, int>::iterator it = this->progresses_.begin();
  auto it = this->progresses_.cbegin();
  for (; it != this->progresses_.end(); ++it){    
    if(it->first!=tid && it->second==GetMinClock()){
      return false;
    }
  }
  return true;

  // TODO
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  return (this->progresses_.find(tid) != this->progresses_.end());
  // TODO
}

}  // namespace csci5570
