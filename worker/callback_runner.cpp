#include <functional>

#include "base/message.hpp"
#include "worker/abstract_callback_runner.hpp"
#include <condition_variable>
#include <mutex>
#include <thread>

namespace csci5570 {

class callbackRunner: public AbstractCallbackRunner {
 public:
  callbackRunner(){}

  void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) override{
       recv_handle_map_[app_thread_id][model_id]=recv_handle;
 }
  
  void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                const std::function<void()>& recv_finish_handle) override {
    recv_finish_handle_map_[app_thread_id][model_id] = recv_finish_handle;
  }

  void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override {
    tracker_map_[app_thread_id][model_id] = {expected_responses, 0};
  }
  void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override {
    std::unique_lock<std::mutex> lk(mutex_map_[app_thread_id][model_id]);
    cond_map_[app_thread_id][model_id].wait(lk, [this,app_thread_id,model_id] { return tracker_map_[app_thread_id][model_id].first == tracker_map_[app_thread_id][model_id].second; });
  }
  void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& m) override {
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mutex_map_[app_thread_id][model_id]);
      recv_finish = tracker_map_[app_thread_id][model_id].first == tracker_map_[app_thread_id][model_id].second + 1 ? true : false;
    }
    recv_handle_map_[app_thread_id][model_id](m);
    if (recv_finish) {
     recv_finish_handle_map_[app_thread_id][model_id]();
    }
    {
      std::lock_guard<std::mutex> lk(mutex_map_[app_thread_id][model_id]);
      tracker_map_[app_thread_id][model_id].second += 1;
      if (recv_finish) {
        cond_map_[app_thread_id][model_id].notify_all();
      }
    }
  }

 private:
  std::map<uint32_t,std::map<uint32_t,std::function<void(Message&)>>> recv_handle_map_;
  std::map<uint32_t,std::map<uint32_t,std::function<void()>>> recv_finish_handle_map_;

  std::map<uint32_t,std::map<uint32_t,std::mutex>> mutex_map_ ;
  std::map<uint32_t,std::map<uint32_t,std::condition_variable>> cond_map_;
  std::map<uint32_t,std::map<uint32_t,std::pair<uint32_t, uint32_t>>> tracker_map_;
};  // class CallbackRunner

}  // namespace csci5570
