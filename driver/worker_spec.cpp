#include "driver/worker_spec.hpp"
#include "glog/logging.h"

namespace csci5570 {

WorkerSpec::WorkerSpec(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
  // {{0, 3}, {1, 2}}: 3 workers on node 0, 2 workers on node 1.
  int current_worker_id = 0;
  for (int i = 0; i < worker_alloc.size(); i++) {
    int node_id = worker_alloc[i].node_id;
    int num_workers = worker_alloc[i].num_workers;
    std::vector<uint32_t> worker_on_current_node;
    for (int i = 0; i < num_workers; i++) {
      // Update worker_to_node
      this->worker_to_node_.insert(std::map<uint32_t, uint32_t>::value_type(current_worker_id, node_id));

      worker_on_current_node.push_back(current_worker_id);
      current_worker_id++;
    }
    // Update node_to_worker
    this->node_to_workers_.insert(
        std::map<uint32_t, std::vector<uint32_t>>::value_type(node_id, worker_on_current_node));
  }
  this->num_workers_=current_worker_id;
}
bool WorkerSpec::HasLocalWorkers(uint32_t node_id) const {
  /**
   * Check if the local process is allocatd any worker for a task
   */
  if (this->node_to_workers_.find(node_id) != this->node_to_workers_.end()) {
    if (this->node_to_workers_.at(node_id).size() > 0)
      return true;
    else
      return false;
  }
}
const std::vector<uint32_t>& WorkerSpec::GetLocalWorkers(uint32_t node_id) const {
  /**
   * Return the ids of worker threads on the local process
   */
  return node_to_workers_.at(node_id);
}
const std::vector<uint32_t>& WorkerSpec::GetLocalThreads(uint32_t node_id) const {
  // TODO
  return node_to_threads_.at(node_id);
}

std::map<uint32_t, std::vector<uint32_t>> WorkerSpec::GetNodeToWorkers() {
  return node_to_workers_;
  // TODO
}

std::vector<uint32_t> WorkerSpec::GetAllThreadIds() {
  // TODO
  // Change thread_ids_  from set to vector
  std::vector<uint32_t> vec_thread_ids_;
  std::copy(thread_ids_.begin(),thread_ids_.end(),back_inserter(vec_thread_ids_));
  return vec_thread_ids_;


}

void WorkerSpec::InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id) {
  // TODO
  /**
   * Register worker id (specific to a task) along with the corresponding thread id
   */


}

void WorkerSpec::Init(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
  /**
   * Initiates the worker specification with the specified allocation
   * Update worker_to_node_, node_to_workers_ and num_workers_
   */
  std::map<uint32_t, uint32_t> worker_to_node_;
  std::map<uint32_t, std::vector<uint32_t>> node_to_workers_;
  std::map<uint32_t, uint32_t> worker_to_thread_;
  std::map<uint32_t, uint32_t> thread_to_worker_;
  std::map<uint32_t, std::vector<uint32_t>> node_to_threads_;
  std::set<uint32_t> thread_ids_;
  uint32_t num_workers_ = 0;
}
}  // namespace csci5570
