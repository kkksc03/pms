#pragma once

#include "base/actor_model.hpp"
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"

#include <condition_variable>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
#include "worker/callback_runner.cpp"
#include "worker/abstract_callback_runner.hpp"

namespace csci5570 {

class AbstractWorkerThread : public Actor {
 public:
  AbstractWorkerThread(uint32_t worker_id,AbstractCallbackRunner* callback_runner) : Actor(worker_id){
    callback_runner_=callback_runner;
  }

 
 protected:
  void OnReceive(Message& msg){
    callback_runner_->AddResponse(msg.meta.recver,msg.meta.model_id,msg);
    for(int i=0;i<msg.data[1].size();i++){
      local_val.push_back(msg.data[1][i]);
    }  
  }
  AbstractCallbackRunner* callback_runner_;
  std::vector<float> local_val;
  void Main(){
    while(true){
      Message msg;
      work_queue_.WaitAndPop(&msg);
      if(msg.meta.flag==Flag::kExit){
        break;
      }
      OnReceive(msg);
    }
  }
    // callback on receival of a message

  // there may be other functions
  //   Wait() and Nofify() for telling when parameters are ready

  // there may be other states such as
  //   a local copy of parameters
  //   some locking mechanism for notifying when parameters are ready
  //   a counter of msgs from servers, etc.
};

}  // namespace csci5570
