#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>

#include "../../types.hpp"
#include "../debug.hpp"
#include "../logging.hpp"

#define MAX_THREADS 512

namespace {
  struct WorkItem {
    AmmoniteWork work;
    void* userPtr;
    AmmoniteGroup* group;
  };

  struct Node {
    WorkItem workItem;
    Node* nextNode;
  };

  class WorkQueue {
  private:
    std::mutex readMutex;
    Node* nextPopped;
    std::atomic<Node*> nextPushed;

  public:
    WorkQueue() {
      //Start with an empty queue, 1 'old' node
      nextPushed = new Node{{nullptr, nullptr, nullptr}, nullptr};
      nextPopped = nextPushed;
    }

    ~WorkQueue() {
      //Clear out any remaining nodes
      WorkItem workItem;
      do {
        this->pop(&workItem);
      } while (workItem.work != nullptr);

      //Clear up next free node
      delete nextPopped;
    }

    void push(AmmoniteWork work, void* userPtr, AmmoniteGroup* group) {
      //Create a new empty node
      Node* newNode = new Node{{nullptr, nullptr, nullptr}, nullptr};

      //Atomically swap the next node with newNode, then fill in the old new node now it's free
      *(nextPushed.exchange(newNode)) = {{work, userPtr, group}, newNode};
    }

    void pushMultiple(AmmoniteWork work, void* userBuffer, int stride,
                                AmmoniteGroup* group, unsigned int count) {
      //Generate section of linked list to insert
      Node* newNode = new Node{{nullptr, nullptr, nullptr}, nullptr};
      Node sectionStart;
      Node* sectionPtr = &sectionStart;
      if (userBuffer == nullptr) {
        for (unsigned int i = 0; i < count; i++) {
          sectionPtr->nextNode = new Node{{work, nullptr, group}, nullptr};
          sectionPtr = sectionPtr->nextNode;
        }
      } else {
        for (unsigned int i = 0; i < count; i++) {
          sectionPtr->nextNode = new Node{{
            work, (void*)((char*)userBuffer + (std::size_t)(i) * stride), group}, nullptr};
          sectionPtr = sectionPtr->nextNode;
        }
      }

      //Insert the generated section atomically
      sectionPtr->nextNode = newNode;
      *(nextPushed.exchange(newNode)) = *sectionStart.nextNode;
      delete sectionStart.nextNode;
    }

    void pop(WorkItem* workItemPtr) {
      //Use the most recently popped node to find the next
      readMutex.lock();

      //Copy the data and free the old node, otherwise return if we don't have a new node
      Node* currentNode = nextPopped;
      if (currentNode->nextNode != nullptr) {
        nextPopped = nextPopped->nextNode;
        readMutex.unlock();

        *workItemPtr = currentNode->workItem;
        delete currentNode;
      } else {
        readMutex.unlock();
        workItemPtr->work = nullptr;
      }
    }
  };
}

namespace ammonite {
  namespace utils {
    namespace thread {
      namespace internal {
        namespace {
          unsigned int poolThreadCount = 0;
          std::thread* threadPool;
          bool stayAlive = false;

          //Write true / false to threadBlockTrigger to block / unblock threads
          std::atomic<bool> threadBlockTrigger{false};
          std::atomic<unsigned int> blockedThreadCount{0};
          std::atomic<bool> threadsBlocked{false};

          WorkQueue* workQueue;
          std::atomic<uintmax_t> jobCount = 0;
        }

        namespace {
          static void initWorker() {
            while (stayAlive) {
              //Fetch the work
              WorkItem workItem;
              workQueue->pop(&workItem);

              //Execute the work or sleep
              if (workItem.work != nullptr) {
                jobCount--;
                workItem.work(workItem.userPtr);

                //Update the group semaphore, if given
                if (workItem.group != nullptr) {
                  workItem.group->release();
                }
              } else {
                //Sleep while 0 jobs remain
                jobCount.wait(0);
              }
            }
          }

          static void blocker(void*) {
            if (++blockedThreadCount == poolThreadCount) {
              threadsBlocked = true;
              threadsBlocked.notify_all();
            }

            threadBlockTrigger.wait(true);

            if (--blockedThreadCount == 0) {
              threadsBlocked = false;
              threadsBlocked.notify_all();
            }
          }
        }

        unsigned int getHardwareThreadCount() {
          return std::thread::hardware_concurrency();
        }

        unsigned int getThreadPoolSize() {
          return poolThreadCount;
        }

        void submitWork(AmmoniteWork work, void* userPtr, AmmoniteGroup* group) {
          //Add work to the queue
          workQueue->push(work, userPtr, group);

          //Increase job count, wake a sleeping thread
          jobCount++;
          jobCount.notify_one();
        }

        //Submit multiple jobs without locking multiple times
        void submitMultiple(AmmoniteWork work, void* userBuffer, int stride,
                            AmmoniteGroup* group, unsigned int newJobs) {
          workQueue->pushMultiple(work, userBuffer, stride, group, newJobs);
          jobCount += newJobs;
          jobCount.notify_all();
        }

        //Create thread pool, existing work will begin executing
        int createThreadPool(unsigned int threadCount) {
          //Exit if thread pool already exists
          if (poolThreadCount != 0) {
            return -1;
          }

          //Create the queue
          workQueue = new WorkQueue();

          //Default to creating a worker thread for every hardware thread
          if (threadCount == 0) {
            threadCount = getHardwareThreadCount();
          }

          //Cap at configured thread limit, allocate memory for pool
          threadCount = (threadCount > MAX_THREADS) ? MAX_THREADS : threadCount;
          threadPool = new std::thread[threadCount];
          if (threadPool == nullptr) {
            return -1;
          }

          //Create the threads for the pool
          stayAlive = true;
          for (unsigned int i = 0; i < threadCount; i++) {
            threadPool[i] = std::thread(initWorker);
          }

          poolThreadCount = threadCount;
          return 0;
        }

        /*
         - Prevent the threads from executing newly submitted jobs
         - Work submitted after the call returns is guaranteed to be blocked
         - Return when the block takes effect
        */
        void blockThreads() {
          if (!threadsBlocked) {
            threadBlockTrigger = true;
            submitMultiple(blocker, nullptr, 0, nullptr, poolThreadCount);

            threadsBlocked.wait(false);
          }
        }

        /*
         - Allow the threads to execute queued jobs
         - Return when all threads are unblocked
        */
        void unblockThreads() {
          if (threadsBlocked) {
            threadBlockTrigger = false;
            threadBlockTrigger.notify_all();

            threadsBlocked.wait(true);
          }
        }

        //Finish all work in the queue as of calling, return when done
        void finishWork() {
          //Unblock if blocked, block threads then wait for completion
          unblockThreads();
          blockThreads();
          unblockThreads();
        }

#ifdef DEBUG
        bool debugCheckRemainingWork(bool verbose) {
          bool issuesFound = false;

          if (jobCount != 0) {
            issuesFound = true;
            if (verbose) {
              ammoniteInternalDebug << "WARNING: Job count is non-zero (" \
                                    << jobCount << ")" << std::endl;
            }
          }

          return issuesFound;
        }
#endif

        //Finish work already in the queue and kill the threads
        void destroyThreadPool() {
          //Finish existing work and block new work from starting
          unblockThreads();
          blockThreads();

          //Kill all threads after they're unblocked and wake up
          stayAlive = false;

          //Unblock threads and wake them up
          unblockThreads();

          //Wait until all threads are done
          for (unsigned int i = 0; i < poolThreadCount; i++) {
            try {
              threadPool[i].join();
            } catch (const std::system_error&) {
              ammonite::utils::warning << "Failed to join thread " << i \
                                       << " while destroying thread pool" << std::endl;
            }
          }

#ifdef DEBUG
          debugCheckRemainingWork(true);
#endif

          //Reset remaining data
          delete workQueue;
          delete [] threadPool;
          poolThreadCount = 0;
        }
      }
    }
  }
}
