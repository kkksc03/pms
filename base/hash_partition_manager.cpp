#include "base/hash_partition_manager.hpp"

namespace csci5570 {

int32_t HashPartitionManager::JumpConsistentHash(uint64_t key, int32_t num_buckets) const {
  int64_t b = -1, j = 0;
  while (j < num_buckets) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
  }
  return b;
}

HashPartitionManager::HashPartitionManager(const std::vector<uint32_t>& server_thread_ids)
    : AbstractPartitionManager(server_thread_ids) {
  // int key;
  // int num_buckets;
  // int result;
  // key = 2000;
  // num_buckets = 10;
  // result = this->JumpConsistentHash(key, num_buckets);
  // LOG(INFO) << "Result(int):" << result;
  // result = this->JumpConsistentHash((int64_t) key, num_buckets);
  // LOG(INFO) << "Result(int64):" << result;
  // key = 2000000;
  // result = this->JumpConsistentHash(key, num_buckets);
  // LOG(INFO) << "Result(int):" << result;
  // result = this->JumpConsistentHash((int64_t) key, num_buckets);
  // LOG(INFO) << "Result(int64):" << result;
}

void HashPartitionManager::Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const {
  const int keys_size = keys.size();                            // Num of keys
  const int32_t num_buckets = this->server_thread_ids_.size();  // Num of server_id
  // Init
  for (int i = 0; i < num_buckets; i++) {
    Keys tempKeys;
    std::pair<int, Keys> temp_pair(this->server_thread_ids_[i], tempKeys);
    sliced->push_back(temp_pair);
  }
  for (int i = 0; i < keys.size(); i++) {
    int32_t target_bucket = this->JumpConsistentHash((int64_t) keys[i], num_buckets);
    sliced->at(target_bucket).second.push_back(keys[i]);
    // sliced->at(this->server_thread_ids_[target_bucket]).second.push_back(keys[i]);
    // sliced->at(0).second.push_back(keys[i]);
  }
}

void HashPartitionManager::Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const {
  Keys keys = kvs.first;
  third_party::SArray<double> vals = kvs.second;
  const int keys_size = keys.size();                            // Num of keys
  const int32_t num_buckets = this->server_thread_ids_.size();  // Num of server_id
  for (int i = 0; i < num_buckets; i++) {
    Keys temp_keys;
    third_party::SArray<double> temp_vals;
    std::pair<int, KVPairs> temp_pair(this->server_thread_ids_[i], std::make_pair(temp_keys, temp_vals));
    sliced->push_back(temp_pair);
  }
  for (int i = 0; i < keys.size(); i++) {
    int32_t target_bucket = this->JumpConsistentHash((int64_t) keys[i], num_buckets);
    sliced->at(target_bucket).second.first.push_back(keys[i]);
    sliced->at(target_bucket).second.second.push_back(vals[i]);
  }
}

}  // namespace csci5570
