#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/hash_partition_manager.hpp"
#include "base/magic.hpp"

namespace csci5570 {

class TestHashPartitionManager : public testing::Test {
 protected:
  void SetUp() {}
  void TearDown() {}
};  // class TestHashPartitionManager

TEST_F(TestHashPartitionManager, Init) { HashPartitionManager pm({0, 1, 2}); }

TEST_F(TestHashPartitionManager, SliceKeys) {
  HashPartitionManager pm({0, 1, 2});
  third_party::SArray<Key> keys({1, 2, 3, 4, 5, 6, 7, 8, 9});
  std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
  pm.Slice(keys, &sliced);
  LOG(INFO) << "Total server thread" << sliced.size();
  for (int i = 0; i < sliced.size(); i++) {
    LOG(INFO) << "server thread id:" << sliced[i].first;
    if (sliced[i].second.size() > 0) {
      LOG(INFO) << "keys:";
      for (int j = 0; j < sliced[i].second.size(); j++) {
        LOG(INFO) << sliced[i].second[j];
      }
    }
  }
  third_party::SArray<Key> keys_2({1, 8, 9, 2, 3, 4, 5, 6, 7, 12, 13});
  std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced_2;
  pm.Slice(keys_2, &sliced_2);

  LOG(INFO) << "Total server thread" << sliced_2.size();
  for (int i = 0; i < sliced_2.size(); i++) {
    LOG(INFO) << "server thread id:" << sliced_2[i].first;
    if (sliced_2[i].second.size() > 0) {
      LOG(INFO) << "keys:";
      for (int j = 0; j < sliced_2[i].second.size(); j++) {
        LOG(INFO) << sliced_2[i].second[j];
      }
    }
  }
}

TEST_F(TestHashPartitionManager, SliceKVs) {
  HashPartitionManager pm({0, 1, 3});
  third_party::SArray<Key> keys({1, 2, 3, 4, 5, 6, 7, 8, 9});
  third_party::SArray<double> vals({.9, .8, .7, .6, .5, .4, .3, .2, .1});
  std::vector<std::pair<int, AbstractPartitionManager::KVPairs>> sliced;
  LOG(INFO) << "Start splice";
  pm.Slice(std::make_pair(keys, vals), &sliced);
  LOG(INFO) << "End splice";
  LOG(INFO) << sliced.size();
  for (int i = 0; i < sliced.size(); i++) {
    LOG(INFO) << "Node id:" << sliced[i].first;
    if (sliced[i].second.first.size() > 0) {
      for (int j = 0; j < sliced[i].second.first.size(); j++) {
        LOG(INFO) << "Key:" << sliced[i].second.first[j] << " Values:" << sliced[i].second.second[j];
      }
    }
  }
}

}  // namespace csci5570
