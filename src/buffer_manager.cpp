//
// Created by apple on 2023/1/15.
//

#include "buffer_manager.h"
#include "constant.h"
#include <iostream>
#include "server.h"
#include "util.h"
#include "thread_pool_manager.h"
#include "gc_manager.h"
#include "file_manager.h"
#include <numeric>

/*
    初始化 BufferManager 时，先查看 lsm 中有没有 pivots 的信息：
    - 如果有，就直接使用已有的 pivots 的信息，kv 都直接放到 buffers 里
    - 如果没有，那么 kv 都先放到 initialBuffer 里，当 initialBuffer 满了，将其排序后等分点上的 key 作为 pivots，写入 lsm 中
*/
BufferManager::BufferManager() {

    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();

    string pivotsInfo;
    bool exist = levelDbKeyManager->getMeta(PIVOTS_KEY, pivotsInfo);

    if (!exist) {

        unordered_map<string, ValueLayout> layouts;
        // 构造函数里应该也不需要考虑锁
        ValueLog::getInstance()->readGroupAndReset(INITIAL_GROUP_ID, layouts);

        for (auto &layout: layouts) {
            initialBuffer[layout.first] = layout.second.getValueInfo().value;
            initialBufferSize += (sizeof(uint32_t) + KEY_LENGTH + layout.second.getValueInfo().value.length());
        }

        return;

    }

    pivots = split(pivotsInfo, "|");

    if (pivots.size() + 1 != GROUP_NUM) {
        printf("pivots size = %d, groupNum = %d\n", pivots.size(), GROUP_NUM);
        printf("invalid pivots size, maybe the db config and the old pivots are not match\n");
    }

    buffers.resize(GROUP_NUM);
    bufferSizes.resize(GROUP_NUM);

    size_t flushThreadsNums = ConfigManager::getInstance().getNumParallelFlush();
    _flushThreadPool.size_controller().resize(flushThreadsNums);
    
}

int BufferManager::put(const string &key, const string &value) {

    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();

//    printf("key is %s!\n", key.c_str());

    lock_guard<recursive_mutex> lockGuard(mutex);

    // 还没有 pivots 的信息，则放到 initialBuffer 中
    if (!pivotsGenerated()) {

        auto _it = initialBuffer.find(key);

        if (_it != initialBuffer.end()) {
            initialBufferSize -= _it->second.length();
            initialBufferSize += value.length();
        } else {
            initialBufferSize += (sizeof(uint32_t) + KEY_LENGTH + value.length());
        }

        initialBuffer[key] = value;

        // 当 initialBuffer 满了，将其排序后等分点上的 key 作为 pivots，写入 lsm 中
        if (initialBuffer.size() > MAX_INITIAL_BUFFER_AMOUNT) {

            printf("generating pivots...\n");

            // 把 key 都拿出来
            vector<string> keys;
            for (auto &it: initialBuffer) {
                keys.push_back(it.first);
            }

            // 对 key 进行排序
            sort(keys.begin(), keys.end());

            string pivotInfo;

            int firstIndex = keys.size() % (GROUP_NUM - 1);
            int gap = keys.size() / (GROUP_NUM - 1);
            for (int i = firstIndex; i < keys.size(); i += gap) {
                pivots.push_back(keys[i]);
                pivotInfo += (keys[i] + "|");
            }

            if (pivots.size() != (GROUP_NUM - 1)) {
                std::cout << "generate pivots fail" << std::endl;
            } else {
                std::cout << "generate pivots success" << std::endl;
                // for (int i = 0; i < pivots.size(); ++i) {
                //     cout << pivots[i] << " ";
                // }
                // cout << endl;
            }

            buffers.resize(GROUP_NUM);
            bufferSizes.resize(GROUP_NUM);

            // 至此，分组的信息已经出来了，把 pivot 的信息写到 lsm 中
            levelDbKeyManager->writeMeta(PIVOTS_KEY, pivotInfo);

            // 接下来要把 initialBuffer 里的东西放到真正的 buffer 里
            // 各个 group 的范围是左开右闭
            int ptr = 0;

            for (const auto &_key: keys) {

                string _value = initialBuffer[_key];

                if (ptr == pivots.size()) {
                    buffers[ptr][_key] = _value;
                    bufferSizes[ptr] += (sizeof(uint32_t) + KEY_LENGTH + _value.length());
                    continue;
                }

                if (_key <= pivots[ptr]) {
                    buffers[ptr][_key] = _value;
                    bufferSizes[ptr] += (sizeof(uint32_t) + KEY_LENGTH + _value.length());
                    if (_key == pivots[ptr]) {
                        ptr++;
                    }
                }

            }

            initialBuffer.clear();
            initialBufferSize = 0;

        }

        return -1;

    }

    // 已经有 pivots 的信息，则找到对应的 group
    int idx = getBelongingGroup(key);

    auto _it = buffers[idx].find(key);

    if (_it != buffers[idx].end()) {
        bufferSizes[idx] -= _it->second.length();
        bufferSizes[idx] += value.length();
    } else {
        bufferSizes[idx] += (sizeof(uint32_t) + KEY_LENGTH + value.length());
    }

    buffers[idx][key] = value;

//    // 每次 put 都这么算一次感觉挺花时间的
//    // key valueSize
//    int totalSize = buffers[idx].size() * (KEY_LENGTH + sizeof(uint32_t));
//    // value
//    totalSize += accumulate(buffers[idx].begin(), buffers[idx].end(), 0,
//                            [](int sum, const unordered_map<string, string>::value_type &p) {
//                                return sum + p.second.length();
//                            }
//    );

    if (bufferSizes[idx] > MAX_BUFFER_SIZE) {
        return idx;
    }

    return -1;

}

bool BufferManager::get(const string &key, string &value) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    unordered_map<string, string> *bufferToOperate;

    if (!pivotsGenerated()) {
        bufferToOperate = &initialBuffer;
    } else {
        int idx = getBelongingGroup(key);
        bufferToOperate = &buffers[idx];
    }

    if ((*bufferToOperate).find(key) == (*bufferToOperate).end()) {
        return false;
    }

    value = (*bufferToOperate)[key];

    return true;

}

void BufferManager::del(const std::string &key) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    unordered_map<string, string> *bufferToOperate;
    size_t *bufferSize;

    if (!pivotsGenerated()) {
        bufferToOperate = &initialBuffer;
        bufferSize = &initialBufferSize;
    } else {
        int idx = getBelongingGroup(key);
        bufferToOperate = &buffers[idx];
        bufferSize = &bufferSizes[idx];
    }

    auto it = (*bufferToOperate).find(key);

    if (it != (*bufferToOperate).end()) {
        *bufferSize -= (sizeof(uint32_t) + KEY_LENGTH + it->second.length());
        (*bufferToOperate).erase(it);
    }

}

bool BufferManager::flush(int idx, bool needLock) {

    if (!pivotsGenerated()) {
        printf("pivot not generated, flush not allowed\n");
        return false;
    }

//    printf("flush group%d\n", idx);

    if (needLock) mutex.lock();

    unordered_map<string, string> buffer = buffers[idx];
    size_t bufferSize = bufferSizes[idx];
    buffers[idx].clear();
    bufferSizes[idx] = 0;

    if (needLock) mutex.unlock();

    if (buffer.empty()) {
        return true;
    }

    int loopCount = 0;
    while (needLock && ValueLog::getInstance()->getTotalDbSize() >= DISK_SIZE) {
        if (++loopCount == 10000) {
            std::cout << "disk size not enough, exit" << std::endl;
            exit(0);
        }
        std::cout << "disk size not enough,waiting for gc" << std::endl;
        GcManager::getInstance()->gc(INVALID_GROUP_ID);
    }

    vector<ValueLayout> valueLayouts;
    ValueLog::getInstance()->groupBatchPut(buffer, bufferSize, idx, valueLayouts);

    bool ret = LevelDBKeyManager::getInstance()->batchPut(valueLayouts);

    if (!ret) {
        printf("flush group%d fail\n", idx);
    }

    return ret;

}

void BufferManager::flushAll() {

    if (!pivotsGenerated()) {
        printf("pivot not generated, flush not allowed\n");
        return;
    }

    mutex.lock();

    for (int i = 0; i < buffers.size(); ++i) {
        flush(i, false);
    }

    mutex.unlock();

}

bool BufferManager::pivotsGenerated() {
    return !pivots.empty();
}

void BufferManager::initialGetRange(string &startingKey, int numKeys, vector<string> &keys,
                                    vector<string> &values) {

    mutex.lock();

    // 放到 map 里来达到排序的效果
    map<string, string> m;

    for (auto &it: initialBuffer) {
        m[it.first] = it.second;
    }

    mutex.unlock();

    auto it = m.find(startingKey);

    int count = 0;

    while (true) {

        if (it == m.end() || count == numKeys) {
            break;
        }

        if (it->second == DELETED_VALUE) {
            it++;
            continue;
        }

        keys.push_back(it->first);
        values.push_back(it->second);

        it++;
        count++;

    }

}

int BufferManager::getBelongingGroup(const string &key) {
    int idx = lower_bound(pivots.begin(), pivots.end(), key) - pivots.begin();
    return idx;
}

vector<string> BufferManager::getGroupBound(int groupId) {
    vector<string> v;
    if (groupId == 0) {
        v.emplace_back(INF_LOWER_BOUND);
        v.emplace_back(pivots[0]);
    } else if (groupId == GROUP_NUM - 1) {
        v.emplace_back(pivots[GROUP_NUM - 2]);
        v.emplace_back(INF_UPPER_BOUND);
    } else {
        v.emplace_back(pivots[groupId - 1]);
        v.emplace_back(pivots[groupId]);
    }
    return v;
}

BufferManager::~BufferManager() {

    // flush initial buffer
    if (!pivotsGenerated()) {

        if (initialBuffer.empty()) {
            return;
        }

        vector<ValueLayout> useless;
        ValueLog::getInstance()->groupBatchPut(initialBuffer, initialBufferSize, INITIAL_GROUP_ID, useless);

//        printf("destructor BufferManager\n");

        return;

    }

    // flush buffers
    flushAll();

//    printf("destructor BufferManager\n");

}
