#include "driver/simple_id_mapper.hpp"

#include <cinttypes>
#include <vector>

#include "base/node.hpp"

namespace csci5570 {

SimpleIdMapper::SimpleIdMapper(Node node, const std::vector<Node>& nodes) {
 this->node_=node;
 int i=0;
 while(i<nodes.size()){
   this->nodes_.push_back(nodes[i]);
   i++;
  }
}

uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) {
  return tid/this->kMaxThreadsPerNode;
}

void SimpleIdMapper::Init(int num_server_threads_per_node) {
    int i=0;
    while(i<nodes_.size()){
    int n=nodes_[i].id;
    std::vector<uint32_t> threadid;
    int j=0;
    while(j<num_server_threads_per_node){
      threadid.push_back(n*(this->kMaxThreadsPerNode)+j);
      j++;
    }
    this->node2server_.insert(std::make_pair(n,threadid));
    i++;
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
    int i=0;
    while(i+this->kMaxBgThreadsPerNode<kMaxThreadsPerNode){
      if(this->node2worker_[node_id].find(node_id*kMaxThreadsPerNode+i+this->kMaxBgThreadsPerNode)==this->node2worker_[node_id].end()){
        this->node2worker_[node_id].insert(node_id*kMaxThreadsPerNode+i+this->kMaxBgThreadsPerNode);
        return node_id*kMaxThreadsPerNode+i+this->kMaxBgThreadsPerNode;
      }
      else{
        i++;
      }
    }
    return -1;
}
void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
    if(this->node2worker_[node_id].find(tid)!=this->node2worker_[node_id].end()){
      this->node2worker_[node_id].erase(tid);
    }
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
  return this->node2server_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  std::vector<uint32_t> thread_vec=this->node2server_[node_id];
  std::vector<uint32_t> tmp;
  int i=0;
  while(i<thread_vec.size()){
    tmp.push_back(thread_vec[i]+this->kWorkerHelperThreadId);
    i++;
  }
  return tmp;
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  std::vector<uint32_t> tmp;
  std::set<uint32_t>::iterator it=this->node2worker_[node_id].begin();
  for(;it!=this->node2worker_[node_id].end();it++){
    tmp.push_back(*it);
  }
  return tmp;
}
std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
  std::vector<uint32_t> tmp;
  int i=0;
  while(i<this->node2server_.size()){
      std::vector<uint32_t>::iterator it=this->node2server_[i].begin();
      for(;it!=this->node2server_[i].end();it++){
        tmp.push_back(*it);
      }
    }
  return tmp;
}

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
const uint32_t SimpleIdMapper::kWorkerHelperThreadId;

}  // namespace csci5570
