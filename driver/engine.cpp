#include "driver/engine.hpp"

#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/node.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/server_thread.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/worker_thread.hpp"

namespace csci5570 {

void Engine::StartEverything(int num_server_threads_per_node) {
  // TODO
}
void Engine::CreateIdMapper(int num_server_threads_per_node) {
  // TODO
}
void Engine::CreateMailbox() {
  // TODO
}
void Engine::StartServerThreads() {
  // TODO
}
void Engine::StartWorkerThreads() {
  // TODO
}
void Engine::StartMailbox() {
  // TODO
}
void Engine::StartSender() {
  // TODO
}

void Engine::StopEverything() {
  // TODO
}
void Engine::StopServerThreads() {
  // TODO
}
void Engine::StopWorkerThreads() {
  // TODO
}
void Engine::StopSender() {
  // TODO
}
void Engine::StopMailbox() {
  // TODO
}

void Engine::Barrier() {
  // TODO
}

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  // TODO
  CHECK(id_mapper_);
  std::vector<uint32_t> local_server =id_mapper_->GetServerThreadsForId(node_.id);
  int count=local_server.size();
  if(count == 0)
    return ;
  auto id = id_mapper_->AllocateWorkerThread(node_.id);
  ThreadsafeQueue<Message>queue;
  mailbox_->RegisterQueue(id,&queue);

  Message reset_msg;
  reset_msg.meta.flag=Flag::kResetWorkerInModel;
  reset_msg.meta.model_id=table_id;
  reset_msg.meta.sender=id;
  reset_msg.AddData(third_party::SArray<uint32_t>(worker_ids));
  for(auto server: local_server){
    reset_msg.meta.recver=server;
    sender_->GetMessageQueue()->Push(reset_msg);
  }
  Message reply;
  while(count>0){
    queue.WaitAndPop(&reply);
    CHECK(reply.meta.flag==Flag::kResetWorkerInModel);
    CHECK(reply.meta.model_id==table_id);
    --count;
  }
  mailbox_->RegisterQueue(id);
  id_mapper_->DeallocateWorkerThread(node_.id,id);


}

void Engine::Run(const MLTask& task) {
  // TODO
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  // TODO
}

}  // namespace csci5570
