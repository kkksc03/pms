#include "server/consistency/ssp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr, int staleness,
                   ThreadsafeQueue<Message>* reply_queue) {
  model_id_ = model_id;
  staleness_ = staleness;
  reply_queue_ = reply_queue;
  storage_ = std::move(storage_ptr);
}

void SSPModel::Clock(Message& msg) {
  int tid = msg.meta.sender;
  if (progress_tracker_.CheckThreadValid(tid)) {
  	int temp = progress_tracker_.AdvanceAndGetChangedMinClock(tid);
  	if (temp != -1) {
  		int buffersize = GetPendingSize(temp);
  		if (buffersize != 0) {
  			std::vector<Message> ms = buffer_.Pop(temp);
  			for (size_t index =0; index < ms.size(); index++){
  				Message r = storage_->Get(ms[index]);
  				reply_queue_->Push(r);
  			}
  		}
  	}       
  }
}

void SSPModel::Add(Message& msg) {
  // TODO
  storage_->Add(msg);
}

void SSPModel::Get(Message& msg) {
  // TODO
   int tid = msg.meta.sender;
  int min_clock_ = progress_tracker_.GetMinClock();
  int progress = GetProgress(tid);
  if((progress - min_clock_) > staleness_){
  	buffer_.Push(progress - staleness_, msg);
  } else {
  	Message reply = storage_->Get(msg);
  	reply_queue_->Push(reply);
  }
}

int SSPModel::GetProgress(int tid) {
   return progress_tracker_.GetProgress(tid);
}

int SSPModel::GetPendingSize(int progress) {
   return buffer_.Size(progress);
}

void SSPModel::ResetWorker(Message& msg) {
  // TODO
  // msg.meta.flag = Flag::kResetWorkerInModel;
  // third_party::SArray<int> tidstemp;
  // tidstemp = msg.data[0];
  // std::vector<uint32_t> tids;
  // for(size_t i=0; i<tidstemp.size(); i++){
  // 	tids.push_back(tidstemp[i]);
  // }
  // progress_tracker_.Init(tids);
  // reply_queue_->Push(msg);
  CHECK_EQ(msg.data.size(), 1);
  third_party::SArray<uint32_t> tids;
  tids = msg.data[0];
  std::vector<uint32_t> tids_vec(tids.begin(), tids.end());
  this->progress_tracker_.Init(tids_vec);
  Message reply_msg;
  reply_msg.meta.model_id = model_id_;
  reply_msg.meta.recver = msg.meta.sender;
  reply_msg.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(reply_msg);
}

}  // namespace csci5570
