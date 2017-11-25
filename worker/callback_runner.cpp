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
    recv_handle_ = recv_handle; 
 }
  
  void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                const std::function<void()>& recv_finish_handle) override {
    recv_finish_handle_ = recv_finish_handle;
  }

  void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override {
    tracker_ = {expected_responses, 0};
  }
  void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return tracker_.first == tracker_.second; });
  }
  void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& m) override {
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      recv_finish = tracker_.first == tracker_.second + 1 ? true : false;
    }
    recv_handle_(m);
    if (recv_finish) {
      recv_finish_handle_();
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      tracker_.second += 1;
      if (recv_finish) {
        cond_.notify_all();
      }
    }
  }

 private:
  std::function<void(Message&)> recv_handle_;
  std::function<void()> recv_finish_handle_;

  std::mutex mu_;
  std::condition_variable cond_;
  std::pair<uint32_t, uint32_t> tracker_;
};  // class CallbackRunner

}  // namespace csci5570
