#include "server/util/pending_buffer.hpp"

namespace csci5570 {

std::vector<Message> PendingBuffer::Pop(const int clock) {
  return this->map_[clock];
}

void PendingBuffer::Push(const int clock, Message& msg) {
  this->map_[clock].push_back(msg);
}

int PendingBuffer::Size(const int progress) {
  return this->map_[progress].size();
}

}  // namespace csci5570
