#pragma once

#include <atomic>
#include <vector>
#include <memory>
#include <cassert>

namespace civic {

    template<typename T>
    class RingBuffer {
    public:
        explicit RingBuffer(size_t bufferSize) 
            : buffer_(bufferSize), bufferMask_(bufferSize - 1),
              sequence_(new std::atomic<size_t>[bufferSize])
        {
            assert((bufferSize != 0) && ((bufferSize & (bufferSize - 1)) == 0));
            
            for (size_t i = 0; i < bufferSize; ++i) {
                sequence_[i].store(i, std::memory_order_relaxed);
            }
            
            enqueuePos_.store(0, std::memory_order_relaxed);
            dequeuePos_.store(0, std::memory_order_relaxed);
        }

        bool push(const T& data) {
            size_t pos;
            while (true) {
                pos = enqueuePos_.load(std::memory_order_relaxed);
                size_t seq = sequence_[pos & bufferMask_].load(std::memory_order_acquire);
                intptr_t diff = (intptr_t)seq - (intptr_t)pos;

                if (diff == 0) {
                    if (enqueuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                        break;
                    }
                } else if (diff < 0) {
                    return false;
                } else {
                }
            }

            buffer_[pos & bufferMask_] = data;
            sequence_[pos & bufferMask_].store(pos + 1, std::memory_order_release);
            return true;
        }

        bool pop(T& data) {
            size_t pos;
            while (true) {
                pos = dequeuePos_.load(std::memory_order_relaxed);
                size_t seq = sequence_[pos & bufferMask_].load(std::memory_order_acquire);
                intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);

                if (diff == 0) {
                    if (dequeuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                        break;
                    }
                } else if (diff < 0) {
                    return false;
                } else {
                }
            }

            data = buffer_[pos & bufferMask_];
            sequence_[pos & bufferMask_].store(pos + bufferMask_ + 1, std::memory_order_release);
            return true;
        }

    private:
        struct Node {
            std::atomic<size_t> sequence;
        };

        std::vector<T> buffer_;
        size_t bufferMask_;
        std::unique_ptr<std::atomic<size_t>[]> sequence_;
        
        alignas(64) std::atomic<size_t> enqueuePos_;
        alignas(64) std::atomic<size_t> dequeuePos_;
    };
} 

