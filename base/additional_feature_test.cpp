#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/magic.hpp"
#include "lib/data_loader.hpp"
#include "lib/kdd_sample.hpp"

#include "base/hash_partition_manager.hpp"
#include "base/range_partition_manager.hpp"
#include "lib/batchiterator.hpp"
namespace csci5570 {

class TestAdditionalFeature : public testing::Test {
 protected:
  void SetUp() {}
  void TearDown() {}
  third_party::SArray<uint32_t> getTestKeys() {
    using DataStore = std::vector<lib::KddSample>;
    using Parser = lib::Parser<lib::KddSample, DataStore>;
    using Parse = std::function<lib::KddSample(boost::string_ref, int)>;
    DataStore data_store;
    lib::KddSample kdd_sample;
    auto kdd_parse = Parser::parse_kdd;
    int n_features = 10;
    std::string url = "hdfs:///datasets/classification/kdd12";
    lib::DataLoader<lib::KddSample, DataStore> data_loader;
    data_loader.load<Parse>(url, n_features, kdd_parse, &data_store);
    BatchIterator<lib::KddSample> batch(data_store);
    // third_party::SArray<uint32_t> keys;
    std::vector<uint32_t> keys;
    for (int i = 0; i < data_store.size(); i++) {
      auto sample = data_store[i];
      auto& x = sample.x_;
      for (auto& field : x) {
        int key = field.first;
        std::vector<uint32_t>::iterator result = find(keys.begin(), keys.end(), key);
        if (result == keys.end()) {
          keys.push_back(key);
        }
      }
    }
    third_party::SArray<uint32_t> testKeys;
    for (int i = 0; i < keys.size(); i++) {
      testKeys.push_back(keys[i]);
    }
    return testKeys;
  }
};  // class TestHashPartitionManager

TEST_F(TestAdditionalFeature, LoadHash) {
  third_party::SArray<uint32_t> keys = this->getTestKeys();
  HashPartitionManager pm({0, 1, 2, 3, 4, 5});
  std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
  LOG(INFO) << "Start split";
  pm.Slice(keys, &sliced);
  LOG(INFO) << "End split";
  LOG(INFO) << "Key distribution";
  for (int i = 0; i < 6; i++) {
    LOG(INFO) << "Number of keys on server:" << i << " is " << sliced[i].second.size();
  }
}

TEST_F(TestAdditionalFeature, LoadRange) {
  third_party::SArray<uint32_t> keys = this->getTestKeys();
  uint32_t rangeLimit = 10000000;
  RangePartitionManager pm({0, 1, 2, 3, 4, 5}, {{0, rangeLimit},
                                                {rangeLimit, 2 * rangeLimit},
                                                {2 * rangeLimit, 3 * rangeLimit},
                                                {3 * rangeLimit, 4 * rangeLimit},
                                                {4 * rangeLimit, 5 * rangeLimit},
                                                {5 * rangeLimit, 6 * rangeLimit}});
  std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
  LOG(INFO) << "Start split";
  pm.Slice(keys, &sliced);
  LOG(INFO) << "End split";
  LOG(INFO) << "Key distribution";
  for (int i = 0; i < 6; i++) {
    LOG(INFO) << "Number of keys on server:" << i << " is " << sliced[i].second.size();
  }
}

}  // namespace csci5570
