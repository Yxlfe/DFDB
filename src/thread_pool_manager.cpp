#include "thread_pool_manager.h"
#include "constant.h"

ThreadPoolManager::ThreadPoolManager() {
    _flushThreadPool.size_controller().resize(POOL_THREADS_NUM);
}