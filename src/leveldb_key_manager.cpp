#include "leveldb_key_manager.h"
#include "leveldb/write_batch.h"
#include <iostream>
#include <boost/bind.hpp>
#include "constant.h"

// LevelDBKeyManager* LevelDBKeyManager::instance = nullptr;
// std::mutex LevelDBKeyManager::instance_mutex;

LevelDBKeyManager::LevelDBKeyManager(const char *lsm_dir) {
    lruList = new LruList<string, string *>(LEVELDB_LRU_CAPACITY);
    specialKeys = {PIVOTS_KEY};
    // init thread pool
    pool.size_controller().resize(POOL_THREADS_NUM);
    // init db
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::CompressionType::kNoCompression;
    leveldb::Status status = leveldb::DB::Open(options, lsm_dir, &_lsm);
    // report error if fails to open leveldb
    if (!status.ok()) {
        fprintf(stderr, "Error on DB open %s\n", status.ToString().c_str());
        assert(status.ok());
    }
}

LevelDBKeyManager::~LevelDBKeyManager() {
    delete lruList;
    delete _lsm;
//    printf("destructor LevelDBKeyManager\n");
}

bool LevelDBKeyManager::put(ValueLayout &valueLayout) {

    auto *positionInfo = new string(valueLayout.serializePosition());

    leveldb::WriteOptions wopt;
    wopt.sync = false;

    lock_guard<recursive_mutex> lockGuard(mutex);

//    printf("put position: %s\n", valueLayout.serializePosition().c_str());

    lruList->put(valueLayout.getValueInfo().key, positionInfo);

    bool ret = _lsm->Put(wopt, leveldb::Slice(valueLayout.getValueInfo().key), leveldb::Slice(*positionInfo)).ok();

    return ret;

}

bool LevelDBKeyManager::batchPut(vector<ValueLayout> &valueLayouts) {

    if (valueLayouts.empty())
        return true;

    leveldb::WriteBatch batch;
    for (auto &valueLayout: valueLayouts) {
//        printf("put position: %s\n", valueLayouts[i].serializePosition().c_str());
        batch.Put(leveldb::Slice(valueLayout.getValueInfo().key),
                  leveldb::Slice(valueLayout.serializePosition()));
    }

    leveldb::WriteOptions wopt;
    wopt.sync = false;

    lock_guard<recursive_mutex> lockGuard(mutex);

    for (auto &valueLayout: valueLayouts) {
        auto *positionInfo = new string(valueLayout.serializePosition());
        lruList->put(valueLayout.getValueInfo().key, positionInfo);
    }

    bool ret = _lsm->Write(wopt, &batch).ok();

    return ret;

}

ValueLayout LevelDBKeyManager::get(const string &key, bool needLock) {

    unique_lock<recursive_mutex> uniqueLock(mutex, defer_lock);
    if (needLock) {
        uniqueLock.lock();
    }

    string *ptr = lruList->get(key);
    string positionStr;
    ValueLayout valueLayout;

    if (ptr != nullptr) {
        positionStr = *ptr;
//        printf("positionStr = %s, from lru\n", positionStr.c_str());
        valueLayout.deserializePosition(positionStr);
    } else {
        leveldb::Status status = _lsm->Get(leveldb::ReadOptions(), leveldb::Slice(key), &positionStr);
        if (status.ok()) {
//            printf("positionStr = %s\n", positionStr.c_str());
            valueLayout.deserializePosition(positionStr);
            lruList->put(key, new string(positionStr));
        }
    }

    return valueLayout;

}

void LevelDBKeyManager::getKeys(string &startingKey, int num, vector<string> &keys,
                                vector<ValueLayout> &valueLocations) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    leveldb::Iterator *it = _lsm->NewIterator(leveldb::ReadOptions());
    // seek 得到第一个大于等于 target 的位置
    it->Seek(leveldb::Slice(startingKey));

    string key;
    ValueLayout valueLocation;

    for (int i = 0; i < num && it->Valid(); i++, it->Next()) {
        // key
        key = it->key().ToString();
        if (specialKeys.find(key) != specialKeys.end()) {
            i--;
            continue;
        }
        keys.push_back(key);
        // valueLocation
        valueLocation.deserializePosition(it->value().ToString());
        valueLocations.push_back(valueLocation);
    }

    delete it;

}

void LevelDBKeyManager::getKeys(const string &startingKey, const string &endingKey, vector<string> &keys,
                                vector<ValueLayout> &valueLocations) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    leveldb::Iterator *it = _lsm->NewIterator(leveldb::ReadOptions());

    if (startingKey == INF_LOWER_BOUND) {
        // 先定位到最右侧
        it->Seek(leveldb::Slice(endingKey));
        // 如果发现 seek 到的不是 endingKey，说明 endingKey 不存在，所以再往左挪一个
        if (it->key().ToString() > endingKey) {
            it->Prev();
        }
        // 从当前位置开始一直往左读，直到读完
        string key;
        ValueLayout valueLocation;
        while (it->Valid()) {
            // key
            key = it->key().ToString();
            if (specialKeys.find(key) != specialKeys.end()) {
                it->Prev();
                continue;
            }
            keys.push_back(key);
            // valueLocation
            valueLocation.deserializePosition(it->value().ToString());
            valueLocations.push_back(valueLocation);
            // prev
            it->Prev();
        }
        // 现在 keys 和 valueLocations 是反着的，逆转一下
        reverse(keys.begin(), keys.end());
        reverse(valueLocations.begin(), valueLocations.end());
        // return
        delete it;
        return;
    }

    // 定位到最左侧
    it->Seek(leveldb::Slice(startingKey));
    if (it->key().ToString() == startingKey) {
        it->Next();
    }

    string key;
    ValueLayout valueLocation;

    // 不断往右读
    while (it->Valid()) {

        // key
        key = it->key().ToString();

        if (specialKeys.find(key) != specialKeys.end()) {
            it->Next();
            continue;
        }

        // 判断有没有读过头，过了就可以结束了
        if (endingKey != INF_UPPER_BOUND && key > endingKey) {
            break;
        }

        keys.push_back(key);

        // valueLocation
        valueLocation.deserializePosition(it->value().ToString());
        valueLocations.push_back(valueLocation);

        it->Next();

    }

    delete it;

}

bool LevelDBKeyManager::deleteKey(const string &key) {

    leveldb::WriteOptions wopt;
    wopt.sync = false;

    lock_guard<recursive_mutex> lockGuard(mutex);

    lruList->del(key);

    return _lsm->Delete(wopt, leveldb::Slice(key)).ok();

}

bool LevelDBKeyManager::writeMeta(const string &key, const string &value) {

    leveldb::WriteOptions wopt;
    wopt.sync = false;

    lock_guard<recursive_mutex> lockGuard(mutex);

    bool ret = _lsm->Put(wopt, leveldb::Slice(key), leveldb::Slice(value)).ok();

    return ret;

}

bool LevelDBKeyManager::getMeta(const string &key, string &value) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    leveldb::Status status = _lsm->Get(leveldb::ReadOptions(), leveldb::Slice(key), &value);

    if (status.ok()) {
        return true;
    }

    return false;

}



