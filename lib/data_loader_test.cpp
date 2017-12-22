#include "lib/data_loader.hpp"
#include <vector>
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "lib/kdd_sample.hpp"
#include "lib/svm_sample.hpp"
namespace csci5570 {

class TestDataLoader : public testing::Test {
 public:
  TestDataLoader() {}
  ~TestDataLoader() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestDataLoader, LoadSVMData) {
  using DataStore = std::vector<lib::SVMSample>;
  using Parser = lib::Parser<lib::SVMSample, DataStore>;
  // using Parse = int;
  using Parse = std::function<lib::SVMSample(boost::string_ref, int)>;

  // using Parse=std::function<Sample(boost::string_ref, int)>;
  DataStore data_store;
  lib::SVMSample svm_sample;
  // Parser svm_parser();
  auto svm_parse = Parser::parse_libsvm;
  int n_features = 10;
  lib::DataLoader<lib::SVMSample, DataStore> data_loader;
  std::string url = "hdfs:///datasets/classification/a9";  // Do not change
  std::string hdfs_namenode = "proj10";                       // Do not change
  std::string master_host = "proj10";                         // Set to worker name
  std::string worker_host = "proj10";                         // Set to worker name
  int hdfs_namenode_port = 9000;                              // Do not change
  int master_port = 45743;                                    // Do not change
  // data_loader.load<Parse>(url, n_features, svm_parse, &data_store);
  data_loader.load<Parse>(url, hdfs_namenode, master_host, worker_host, hdfs_namenode_port, master_port, n_features,
                          svm_parse, &data_store);
  for (int i = 0; i < data_store.size(); i++) {
    LOG(INFO) << "Index :" << i << " " << data_store[i].toString();
  }
  LOG(INFO) << "Size " << data_store.size();
}
TEST_F(TestDataLoader, LoadKddData) {
  using DataStore = std::vector<lib::KddSample>;
  using Parser = lib::Parser<lib::KddSample, DataStore>;
  // using Parse = int;
  using Parse = std::function<lib::KddSample(boost::string_ref, int)>;

  // using Parse=std::function<Sample(boost::string_ref, int)>;
  DataStore data_store;
  lib::KddSample kdd_sample;
  // Parser svm_parser();
  auto kdd_parse = Parser::parse_kdd;
  int n_features = 10;
  std::string url = "hdfs:///datasets/classification/kdd12";  // Do not change
  std::string hdfs_namenode = "proj10";                       // Do not change
  std::string master_host = "proj10";                         // Set to worker name
  std::string worker_host = "proj10";                         // Set to worker name
  int hdfs_namenode_port = 9000;                              // Do not change
  int master_port = 45743;                                    // Do not change
  lib::DataLoader<lib::KddSample, DataStore> data_loader;
  data_loader.load<Parse>(url, hdfs_namenode, master_host, worker_host, hdfs_namenode_port, master_port, n_features,
                          kdd_parse, &data_store);
  for (int i = 0; i < data_store.size(); i++) {
    LOG(INFO) << "Index :" << i << " " << data_store[i].toString();
  }
  LOG(INFO) << "Size " << data_store.size();
}

}  // namespace csci5570
