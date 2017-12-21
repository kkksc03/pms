#include <vector>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "driver/engine.hpp"
#include "lib/data_loader.hpp"
#include "lib/svm_sample.hpp"
#include "worker/kv_client_table.hpp"

/*
third_party::SArray<double> compute_gradients(
    const std::vector<lib::SVMSample*>& samples,
    const third_party::SArray<Key>& keys,
    const third_party::SArray<double>& vals,
    double alpla
) {
    third_party::SArray<double> deltas(keys.size(),0.);
    for (auto sample : samples) {
        auto& x= sample-> x_;
        double& y = sample-> y_;
        double predict = 0.;
        if (y < 0)
            y = 0;
        int idx = 0;
        for (auto& field : x) {
            while (keys[idx] < field.first)
                ++idx;
            predict += vals[idx] * field.second();
        }
        predict += vals.back();
        predict = 1. / (1. + exp(-1 * predict));
        idx = 0;
        for (auto & field : x){
            while (keys[idx] < field.first)
                ++idx;
            deltas[idx] += alpha * field.second * (y - predict);
        }
        deltas[deltas.size() - 1] += alpha * (y - predict);
    }
    return deltas;
}
*/

//. Define arguments
DEFINE_int32(my_id, -1, "the process id of this program");
DEFINE_string(config_file, "", "The config file path");
// Data loading config
DEFINE_string(hdfs_namenode, "proj10", "The hdfs namenode hostname");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs port");
DEFINE_int32(hdfs_master_port, 23489, "A port number for the hdfs assigner host");
DEFINE_int32(n_loaders_per_node, 1, "The number of loaders per node");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(n_features, 10, "The number of feature in the dataset");
// Traing config
DEFINE_int32(n_workers_per_node, 1, "The number of workers per node");
DEFINE_int32(n_iters, 10, "The number of interattions");
DEFINE_int32(batch_size, 100, "Batch size");
DEFINE_double(alpha, 0.001, "learning rate");

namespace csci5570 {
/*
void LrTest() {
  int my_id = FLAGS_my_id;
  int n_nodes = 5;
  std::vector<Node> nodes(n_nodes);
  // Should read from config file
  for (int i = 0; i < n_nodes; ++i) {
    nodes[i].id = i;
    nodes[i].hostname = "proj" + std::to_string(i + 5);
    nodes[i].port = 45612;
    //"0:proj5:45612"
    //"1:proj6:45612"
  }

  const Node& node = nodes[my_id];

  // Load Data
  using DataStore = std::vector<lib::SVMSample>;
  using Parser = lib::Parser<lib::SVMSample, DataStore>;
  using Parse = std::function<lib::SVMSample(boost::string_ref, int)>;

  // using Parse=std::function<Sample(boost::string_ref, int)>;
  DataStore data_store;
  lib::SVMSample svm_sample;
  // Parser svm_parser();
  auto svm_parse = Parser::parse_libsvm;
  std::string url = "hdfs:///datasets/classification/a9";
  lib::DataLoader<lib::SVMSample, DataStore> data_loader;
  data_loader.load<Parse>(FLAGS_hdfs_namenode, FLAGS_hdfs_namenode_port, FLAGS_hdfs_master_port, url, FLAGS_n_features,
                          svm_parse, &data_store);
  for (int i = 0; i < data_store.size(); i++) {
    LOG(INFO) << "Index :" << i << " " << data_store[i].toString();
  }
  LOG(INFO) << "Size " << data_store.size();

  // Start Engine
  Engine engine(node, nodes);
  engine.StartEverything();

  // Create table on the server side
  // const auto kTable = engine.CreateTable<double>(ModelType::ASP,StoreageType::Map,FLAGS_n_features+1,RangePartition);
  const auto kTable = engine.CreateTable<double>(ModelType::ASP, StoreageType::Map);

  // Specify task
  MLTask task;
  task.SetTables({kTable});
  std::vector<WorkerAlloc> worker_alloc;
  for (int i = 0; i < n_nodes; ++i) {
    woker_alloc.push_back({nodes[i].id, static_cast<uint32_t>(FLAGS_n_workers_per_node)});
  }
  task.SetWorkerAlloc(worker_alloc);
  // get client table
  task.SetLambda([kTable, &datastore](const Info& info) {
    auto table = info.CreateKVClientTable<double>(kTable);

    BatchIterator<lib::SVMSample> batch(datastore);

    // interations
    for (int iter = 0; iter < FLAGS_n_iters; ++iter) {
      // get data batch
      auto keys_data = batch.NextBatch(FLAGS_batch_size);

      // prepare parameters
      third_party::SArray<double> vals;
      table.Get(keys_data.first, &vals);
      // compute gradients

      // update parameters

      // clock
    }
  });

  engine.StopEverything();
  return 1;
}
}  // namespace csci5570
*/
int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_stderrthreshold = 0;
  FLAGS_colorlogtostderr = true;
  //   csci5570::LrTest();
}