#include "server.h"
#include "constant.h"
#include <iostream>
#include <thread>
#include "buffer_manager.h"
#include "file_manager.h"
#include "util.h"
#include "gc_manager.h"
#include "thread_pool_manager.h"
#include "statistics_manager.h"

dfdb::Server * dfdb::Server::_instance = nullptr;
std::mutex dfdb::Server::_instance_mutex;

namespace dfdb{
bool Server::put(const string &key, const string &value) {

    string _key = validateKey(key);
    if (_key == INVALID_KEY) {
        return false;
    }

//    lruList->put(_key, new string(value));

    BufferManager *bufferManager = BufferManager::getInstance();

    // 先给 buffer 上锁
    bufferManager->mutex.lock();

    // 先放到 buffer 里
    int flushGroupId = bufferManager->put(_key, value);

    if (flushGroupId == -1) {
        bufferManager->mutex.unlock();
        return true;
    }

    bool ret = bufferManager->flush(flushGroupId);

    bufferManager->mutex.unlock();

    return ret;

}

bool Server::get(const string &key, string &value) {

    string _key = validateKey(key);
    if (_key == INVALID_KEY) {
        return false;
    }

//    string *p = lruList->get(_key);
//    if (p != nullptr) {
//        value = *p;
//        if (value == DELETED_VALUE) {
//            return false;
//        }
//        return true;
//    }

    BufferManager *bufferManager = BufferManager::getInstance();
    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();
    ValueLog *valueLog = ValueLog::getInstance();
    FileManager *fileManager = FileManager::getInstance();

    // 先在 buffer 里找
    bool exist = bufferManager->get(_key, value);
    if (exist) {
//        printf("get from buffer\n");
        return true;
    }

    // 如果 buffer 里没有，那么到 lsm 里找 kv 的 position
    ValueLayout layout;
    int groupId;
    while (true) {
        layout = levelDbKeyManager->get(_key);
        if (!layout.getPositionInfo().valid) {
            return false;
        }
        groupId = layout.getPositionInfo().groupId;
        fileManager->operateFileMutex(groupId, LOCK);
        // 在上锁之后，再 get 一次 layout，看是否相同
        ValueLayout temp = levelDbKeyManager->get(_key);
        if (temp.layoutCompare(layout)) {
            break;
        } else {
            fileManager->operateFileMutex(groupId, UNLOCK);
        }
    }

//    printf("file in group %d locked!!\n", layout.getPositionInfo().groupId);

    // 根据 position 到 disk 里找 value
    valueLog->assignValueInfo(_key, layout);

    fileManager->operateFileMutex(groupId, UNLOCK);

    if (!layout.getValueInfo().valid) {
        return false;
    }

    value = layout.getValueInfo().value;

//    lruList->put(_key, new string(value));

//    printf("get from disk\n");
    return true;

}

void Server::getRange(const string &startingKey, int numKeys, vector<string> &keys,
                      vector<string> &values) {

//    printf("startingKey = %s, numKeys = %d\n", startingKey.c_str(), numKeys);

    string _startingKey = validateKey(startingKey);
    if (_startingKey == INVALID_KEY) {
        return;
    }

    BufferManager *bufferManager = BufferManager::getInstance();

    // 如果连分组都还没生成，那么直接到 initialBuffer 里 getRange
    if (!bufferManager->pivotsGenerated()) {
        printf("initialGetRange\n");
        bufferManager->initialGetRange(_startingKey, numKeys, keys, values);
        return;
    }

    StatisticsManager *statisticsManager = StatisticsManager::getInstance();

    int randomNumber = statisticsManager->startTimer();

    // 把 buffer 全部落盘
    bufferManager->flushAll();

    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();
    ValueLog *valueLog = ValueLog::getInstance();
    FileManager *fileManager = FileManager::getInstance();

    // 先把所有 group 都锁住，因为不知道 range 里的 key 会涉及哪些 group
    for (int i = 0; i < GROUP_NUM; ++i) {
        fileManager->operateFileMutex(i, LOCK);
    }

    // 从 lsm 中得到所有的 key 及其 location
    vector<ValueLayout> valueLayouts;
    levelDbKeyManager->getKeys(_startingKey, numKeys, keys, valueLayouts);

//    printf("first = %s, last = %s\n", keys[0].c_str(), keys[keys.size() - 1].c_str());

    // 看这些 key 涉及到哪些 group，未涉及的 group 可以解锁了
    unordered_set<int> involvingGroups;
    for (int i = 0; i < keys.size(); ++i) {
        involvingGroups.insert(valueLayouts[i].getPositionInfo().groupId);
    }
    for (int i = 0; i < GROUP_NUM; ++i) {
        if (involvingGroups.find(i) == involvingGroups.end()) {
            fileManager->operateFileMutex(i, UNLOCK);
        }
    }

    // 现在要去 group 内找这些 position 处的 value
    valueLog->assignValueInfo(keys, valueLayouts);

    // 把 values 提出来即可，此外还要把 key 给变回来（插入的时候是 validate 了的）
    for (int i = 0; i < valueLayouts.size(); ++i) {
        keys[i] = trim(keys[i]);
        values.push_back(valueLayouts[i].getValueInfo().value);
    }

    // 可以解锁 group 了
    for (int i = 0; i < GROUP_NUM; ++i) {
        if (involvingGroups.find(i) != involvingGroups.end()) {
            fileManager->operateFileMutex(i, UNLOCK);
        }
    }

    statisticsManager->stopTimer(RANGE_QUERY_TIME_COST, randomNumber);

//    printf("get range res:\n");
//    for (int i = 0; i < values.size(); ++i) {
//        printf("key = %s, value = %s\n", keys[i].c_str(), values[i].c_str());
//    }

}

// gc 使用
void Server::getRange(const std::string &startingKey, const std::string &endingKey, std::vector<std::string> &keys,
                      std::vector<std::string> &values) {

    BufferManager *bufferManager = BufferManager::getInstance();

    // 把 buffer 全部落盘
    bufferManager->flushAll();

    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();
    ValueLog *valueLog = ValueLog::getInstance();
    FileManager *fileManager = FileManager::getInstance();

    // 先把所有 group 都锁住，因为不知道 range 里的 key 会涉及哪些 group
    for (int i = 0; i < GROUP_NUM; ++i) {
        fileManager->operateFileMutex(i, LOCK);
    }

    // 从 lsm 中得到所有的 key 及其 location
    vector<ValueLayout> valueLayouts;
    levelDbKeyManager->getKeys(startingKey, endingKey, keys, valueLayouts);

//    printf("first = %s, last = %s\n", keys[0].c_str(), keys[keys.size() - 1].c_str());

    // 看这些 key 涉及到哪些 group，未涉及的 group 可以解锁了
    unordered_set<int> involvingGroups;
    for (int i = 0; i < keys.size(); ++i) {
        involvingGroups.insert(valueLayouts[i].getPositionInfo().groupId);
    }
    for (int i = 0; i < GROUP_NUM; ++i) {
        if (involvingGroups.find(i) == involvingGroups.end()) {
            fileManager->operateFileMutex(i, UNLOCK);
        }
    }

    // 现在要去 group 内找这些 position 处的 value
    valueLog->assignValueInfo(keys, valueLayouts, true);

    // 把 values 提出来即可，注意这里不要把 key 给变回来
    for (int i = 0; i < valueLayouts.size(); ++i) {
        values.push_back(valueLayouts[i].getValueInfo().value);
    }

    // 可以解锁 group 了
    for (int i = 0; i < GROUP_NUM; ++i) {
        if (involvingGroups.find(i) != involvingGroups.end()) {
            fileManager->operateFileMutex(i, UNLOCK);
        }
    }

//    printf("get range res:\n");
//    for (int i = 0; i < values.size(); ++i) {
//        printf("key = %s, value = %s\n", keys[i].c_str(), values[i].c_str());
//    }

}

bool Server::del(const string &key) {

    string _key = validateKey(key);
    if (_key == INVALID_KEY) {
        return false;
    }

    BufferManager *bufferManager = BufferManager::getInstance();
    bufferManager->del(_key);

    LevelDBKeyManager *levelDbKeyManager = LevelDBKeyManager::getInstance();
    bool ret = levelDbKeyManager->deleteKey(_key);

    return ret;

}

Server::Server(const char* config) {
//    lruList = new LruList<string, string *>(SERVER_LRU_CAPACITY);
    // 先构造后析构，注意顺序不能乱
    ConfigManager::getInstance().setConfigPath(config);
    StatisticsManager::getInstance();
    // ThreadPoolManager::getInstance();
    FileManager::getInstance();
    ValueLog::getInstance();
    BufferManager::getInstance();
    LevelDBKeyManager::getInstance();
    GcManager::getInstance();
}

void Server::test() {

    BufferManager *bufferManager = BufferManager::getInstance();
    GcManager *gcManager = GcManager::getInstance();

    unordered_map<string, string> insertedPairs;

    printf("test begin\n");

    int size = 150000;
    int keyLength = 24;
    int valLength = 1024 - KEY_LENGTH - 4;

    // 进行若干次 insert，key 的长度设置的短一点可以 cover 重复 key 的 case
    for (int i = 0; i < size; i++) {
        string key = randStr(keyLength);
        string val = randStr(valLength);
//        if (insertedPairs.find(key) != insertedPairs.end()) {
//            printf("inserting duplicate key: %s\n", key.c_str());
//        }
        bool success = put(key, val);
        if (!success) {
            printf("ggg0!! insert fail!!\n");
        } else {
            insertedPairs[key] = val;
        }
    }
    printf("phase1 end\n");

    // flush 前进行全部 kv 的查找
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        if (!exist) {
            printf("ggg1! key %s not exist\n", key.c_str());
        } else {
            if (value != it->second) {
                printf("ggg2! key = %s, true value is %s, but get %s\n", key.c_str(), it->second.c_str(),
                       value.c_str());
                return;
            }
        }
    }
    printf("phase2 end\n");

    bufferManager->flushAll();

    // flush 后 gc 前进行全部 kv 的查找
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        if (!exist) {
            printf("ggg3! key %s not exist\n", key.c_str());
        } else {
            if (value != it->second) {
                printf("ggg4! key = %s, group = %d, true value is %s, but get %s\n", key.c_str(),
                       bufferManager->getBelongingGroup(key), it->second.c_str(),
                       value.c_str());
                return;
            }
//            printf("get success, group = %d\n", bufferManager->getBelongingGroup(key));
        }
    }
    printf("phase3 end\n");

    gcManager->gcAll();

    // gc 后进行全部 kv 的查找，由于此时没有进行过删除，所以理论上是可以全部找到的
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        if (!exist) {
            printf("ggg5! key %s not exist\n", key.c_str());
        } else {
            if (value != it->second) {
                printf("ggg6! key = %s, true value is %s, but get %s\n", key.c_str(), it->second.c_str(),
                       value.c_str());
            }
        }
    }
    printf("phase4 end\n");

    unordered_set<string> deletedKeys;

    // 进行若干次 delete，随机的 key 里面肯定会有 db 中存在的
    for (int i = 0; i < size / 2; i++) {
        string key = randStr(keyLength);
        bool success = del(key);
        if (!success) {
            printf("ggg7!! delete fail!!\n");
        } else {
            deletedKeys.insert(key);
        }
    }
    printf("phase5 end\n");

    // flush 前
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        bool deleted = false;
        if (deletedKeys.find(key) != deletedKeys.end()) {
            deleted = true;
        }
        if (!exist) {
            if (!deleted) {
                printf("ggg8! key = %s, value not exist\n", key.c_str());
            }
        } else {
            if (deleted) {
                printf("ggg9! key = %s, delete fail\n", key.c_str());
                continue;
            }
            if (value != it->second) {
                printf("ggg10! key = %s, true value is %s, but get %s\n", key.c_str(), it->second.c_str(),
                       value.c_str());
            }
        }
    }
    printf("phase6 end\n");

    bufferManager->flushAll();

    // flush 后 gc 前
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        bool deleted = false;
        if (deletedKeys.find(key) != deletedKeys.end()) {
            deleted = true;
        }
        if (!exist) {
            if (!deleted) {
                printf("ggg11! key = %s, value not exist\n", key.c_str());
            }
        } else {
            if (deleted) {
                printf("ggg12! key = %s, delete fail\n", key.c_str());
                continue;
            }
            if (value != it->second) {
                printf("ggg13! key = %s, true value is %s, but get %s\n", key.c_str(), it->second.c_str(),
                       value.c_str());
            }
        }
    }
    printf("phase7 end\n");

    gcManager->gcAll();

    // gc 后
    for (auto it = insertedPairs.begin(); it != insertedPairs.end(); it++) {
        string key = it->first;
        string value = "shit";
        bool exist = get(key, value);
        bool deleted = false;
        if (deletedKeys.find(key) != deletedKeys.end()) {
            deleted = true;
        }
        if (!exist) {
            if (!deleted) {
                printf("ggg14! key = %s, value not exist\n", key.c_str());
            }
        } else {
            if (deleted) {
                printf("ggg15! key = %s, delete fail, value = %s\n", key.c_str(), value.c_str());
                continue;
            }
            if (value != it->second) {
                printf("ggg16! key = %s, true value is %s, but get %s\n", key.c_str(), it->second.c_str(),
                       value.c_str());
            }
        }
    }
    printf("phase8 end\n");

    // 最后测试一下 getRange
    for (int i = 0; i < 50; i++) {
        string key = randStr(keyLength);
        vector<string> keys;
        vector<string> values;
        getRange(key, 100, keys, values);
        for (int j = 0; j < keys.size(); ++j) {
            bool deleted = false;
            if (deletedKeys.find(keys[j]) != deletedKeys.end()) {
                deleted = true;
            }
            if (deleted) {
                printf("ggg17! key = %s should has been deleted\n", keys[j].c_str());
            } else {
                if (insertedPairs[keys[j]] != values[j]) {
                    printf("ggg18! key = %s, value not match, true is %s, but get %s\n", keys[j].c_str(),
                           insertedPairs[keys[j]].c_str(), values[j].c_str());
                }
            }
        }
    }
    printf("phase9 end\n");

}

Server::~Server() {

}

}//namespace dfdb