//
// Created by apple on 2023/1/17.
//

#include "gc_manager.h"
#include "value_log.h"
#include "file_manager.h"
#include "constant.h"
#include "buffer_manager.h"
#include <unordered_set>
#include <iostream>
#include "thread_pool_manager.h"
#include "server.h"
#include "statistics_manager.h"

void GcManager::gc(int groupId) {

//    std::cout << "=====================gc begin=====================" << std::endl;

    BufferManager *bufferManager = BufferManager::getInstance();
    if (!bufferManager->pivotsGenerated()) {
        printf("pivot not generated\n");
        return;
    }

    StatisticsManager *statisticsManager = StatisticsManager::getInstance();
    int randomNumber = statisticsManager->startTimer();

    ValueLog *valueLog = ValueLog::getInstance();
    if (groupId == INVALID_GROUP_ID) {
        groupId = valueLog->getGroupWithMaxIncr();
    }
//    printf("gc group %d\n", groupId);

    FileManager *fileManager = FileManager::getInstance();
    dfdb::Server *server = dfdb::Server::getInstance();
    assert(server != nullptr);
    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();

    lock_guard<recursive_mutex> lockGuard1(bufferManager->mutex);
    lock_guard<recursive_mutex> lockGuard2(levelDbKeyManager->mutex);

    fileManager->operateFileMutex(groupId, LOCK);

    // 得到 group 的 range
    vector<string> v = bufferManager->getGroupBound(groupId);
    string lowerBound = v[0];
    string upperBound = v[1];

    // 对 server 进行一次 range 内的范围查询
    vector<string> keys;
    vector<string> values;
    std::cout << "start: server->getRange (lsm get gc old index)" << std::endl;
    server->getRange(lowerBound, upperBound, keys, values);
//    for (int i = 0; i < keys.size(); ++i) {
//        printf("key = %s, value = %s\n", keys[i].c_str(), values[i].c_str());
//    }
    std::cout << "finish: server->getRange (lsm get gc old index)" << std::endl;
    // 用最新的 kv 覆写原本的 group
    vector<ValueLayout> valueLayouts;
    std::cout << "start: valueLog->groupRewrite (val-log gc)" << std::endl;
    size_t rewriteSize = valueLog->groupRewrite(keys, values, groupId, valueLayouts);
    std::cout << "finish: valueLog->groupRewrite (val-log gc)" << std::endl;
    statisticsManager->addCount(GC_WRITE_BYTES, rewriteSize);
//    for (int i = 0; i < valueLayouts.size(); ++i) {
//        printf("groupId = %d, offset = %d, length = %d\n", valueLayouts[i].getPositionInfo().groupId,
//               valueLayouts[i].getPositionInfo().offset, valueLayouts[i].getPositionInfo().length);
//    }

    std::cout << "start: levelDbKeyManager->batchPut (lsm update gc new index)" << std::endl;
    // 更新 lsm 中 value 的位置
    levelDbKeyManager->batchPut(valueLayouts);
    std::cout << "finish: levelDbKeyManager->batchPut (lsm update gc new index)" << std::endl;

    fileManager->operateFileMutex(groupId, UNLOCK);

    statisticsManager->stopTimer(GC_TIME_COST, randomNumber);

//    std::cout << "=====================gc end=====================" << std::endl;

}

// GcManager::GcManager(boost::asio::io_service &_ctx) : ctx(_ctx), timer(_ctx) {
//     startTimer();
//     // 一定要 run 才会真正让计时器启动，但 run 又是个阻塞操作，直到计时器操作完毕，因此得放到一个线程里 run
//     thread = std::thread([this]() {
//         ctx.run();
//     });
// }

GcManager::GcManager() {

}

// GcManager::~GcManager() {
//     timer.cancel();
//     ctx.stop();
//     thread.join();
// //    printf("destructor GcManager\n");
// }

// GcManager::~GcManager() {
//     timer.cancel();
//     ctx.stop();
//     thread.join();
// //    printf("destructor GcManager\n");
// }

GcManager::~GcManager() {

}

// void GcManager::startTimer() {
//     timer.expires_from_now(std::chrono::seconds(GC_CHECK_INTERVAL));
//     timer.async_wait([this](const boost::system::error_code &ec) {
//         if (ec) {
//             printf("timer error!!\n");
//         } else {
//             ValueLog *valueLog = ValueLog::getInstance();
//             int groupId = valueLog->getGroupWithMaxIncr();
//             if (groupId != -1) {
//                 gc(groupId);
//             }
//         }
//         startTimer();
//     });
// }

void GcManager::gcAll() {
    for (int i = 0; i < GROUP_NUM; ++i) {
        gc(i);
    }
}
