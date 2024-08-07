//
// Created by apple on 2023/2/18.
//

#ifndef TREEKV_THREAD_POOL_MANAGER_H
#define TREEKV_THREAD_POOL_MANAGER_H

#include "threadpool/pool.hpp"

class ThreadPoolManager {

private:
    ThreadPoolManager();

public:
    boost::threadpool::pool _flushThreadPool;

    static ThreadPoolManager *getInstance() {
        static ThreadPoolManager instance;
        return &instance;
    }

};


#endif //TREEKV_THREAD_POOL_MANAGER_H
