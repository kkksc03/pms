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
  /**
   * The flow of starting the engine:
   * 1. Create an id_mapper and a mailbox
   * 2. Start Sender
   * 3. Create ServerThreads and WorkerThreads
   * 4. Register the threads to mailbox through ThreadsafeQueue
   * 5. Start the communication threads: bind and connect to all other nodes
   *
   * @param num_server_threads_per_node the number of server threads to start on each node
   */
  this->CreateIdMapper(num_server_threads_per_node);
  this->CreateMailbox();
  this->StartSender();
  this->StartServerThreads();
  this->StartWorkerThreads();
  this->StartMailbox();
}
void Engine::CreateIdMapper(int num_server_threads_per_node) {
  this->id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  this->id_mapper_->Init(num_server_threads_per_node);
}
void Engine::CreateMailbox() { this->mailbox_.reset(new Mailbox(node_, nodes_, this->id_mapper_.get())); }
void Engine::StartServerThreads() {
  std::vector<uint32_t> server_thread_ids = id_mapper_->GetAllServerThreads();
  std::vector<uint32_t>::iterator it = server_thread_ids.begin();
  for (; it != server_thread_ids.end(); it++) {
    std::unique_ptr<ServerThread> s_pt(new ServerThread(*it));
    s_pt->Start();
    this->server_thread_group_.push_back(std::move(s_pt));
  }
}
void Engine::StartWorkerThreads() {
  Actor actor;
  this->actor->Start();
  // TODO
}
void Engine::StartMailbox() { this->mailbox_->Start(); }
void Engine::StartSender() {
  this->sender_.reset(new Sender(this->mailbox_.get()));
  this->sender_->Start();
}
void Engine::StopEverything() {
  /**
   * The flow of stopping the engine:
   * 1. Stop the Sender
   * 2. Stop the mailbox: by Barrier() and then exit
   * 3. The mailbox will stop the corresponding registered threads
   * 4. Stop the ServerThreads and WorkerThreads
   */
  this->StopSender();
  this->StopMailbox();
  this->StopServerThreads();
  this->StopWorkerThreads();
}
void Engine::StopServerThreads() {
  int i = 0;
  while (i < this->server_thread_group_.size()) {
    std::unique_ptr<ServerThread> t = std::move(this->server_thread_group_[i]);
    t->Stop();
    i++;
  }
}
void Engine::StopWorkerThreads() {
  Actor actor;
  this->actor->Stop();
  // TODO
}
void Engine::StopSender() { this->sender_->Stop(); }
void Engine::StopMailbox() { this->mailbox_->Stop(); }

void Engine::Barrier() { this->mailbox_->Barrier(); }

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  WorkerSpec worker_spec(worker_alloc);
  // // {node_id, {worker_ids}}: {0, {0,1,2}}, {1, {3,4}}
  // std::map<uint32_t, std::vector<uint32_t>> node_to_workers_;
  auto node_to_workers = worker_spec.GetNodeToWorkers();
  for (auto& workers : node_to_workers) {
    for (auto worker : workers.second) {
      // workers.first is the node is
      // workers.second is the worker id
      // id is the thread id
      auto id = id_mapper_->AllocateWorkerThread(workers.first);
      worker_spec.InsertWorkerIdThreadId(worker, id);
    }
  }
  return worker_spec;
}  // namespace csci5570

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  // TODO
  CHECK(id_mapper_);
  std::vector<uint32_t> local_server = id_mapper_->GetServerThreadsForId(node_.id);
  int count = local_server.size();
  if (count == 0)
    return;
  auto id = id_mapper_->AllocateWorkerThread(node_.id);
  ThreadsafeQueue<Message> queue;
  mailbox_->RegisterQueue(id, &queue);

  Message reset_msg;
  reset_msg.meta.flag = Flag::kResetWorkerInModel;
  reset_msg.meta.model_id = table_id;
  reset_msg.meta.sender = id;
  reset_msg.AddData(third_party::SArray<uint32_t>(worker_ids));
  for (auto server : local_server) {
    reset_msg.meta.recver = server;
    sender_->GetMessageQueue()->Push(reset_msg);
  }
  Message reply;
  while (count > 0) {
    queue.WaitAndPop(&reply);
    CHECK(reply.meta.flag == Flag::kResetWorkerInModel);
    CHECK(reply.meta.model_id == table_id);
    --count;
  }
  mailbox_->RegisterQueue(id, &queue);
  id_mapper_->DeallocateWorkerThread(node_.id, id);
}

void Engine::Run(const MLTask& task) {
  // TODO
  CHECK(task.IsSetup());
  auto worker_spec = AllocateWorkers(task.GetWorkerAlloc());
  // Init table
  const auto& tables = task.GetTables();
  for (auto& table : tables)
    InitTable(table, worker_spec.GetAllThreadIds());
  Barrier();
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  this->partition_manager_map_.insert(std::map<uint32_t, std::unique_ptr<AbstractPartitionManager>>::value_type(
      table_id, std::move(partition_manager)));
}

}  // namespace csci5570
