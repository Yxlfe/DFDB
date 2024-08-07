//
// Created by apple on 2023/1/13.
//

#ifndef WISCKEY_LEVELDB_KEY_MANAGER_H
#define WISCKEY_LEVELDB_KEY_MANAGER_H


#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <string>
#include <vector>
#include <mutex>
#include "leveldb/db.h"
#include "value_layout.h"
#include "value_log.h"
#include <threadpool.hpp>
#include "lru_list.h"
#include "configManager.h"

using namespace std;

class ValueLayout;

class LevelDBKeyManager {

private:

    leveldb::DB *_lsm;

    LevelDBKeyManager(const char *lsm_dir);

    boost::threadpool::pool pool;

    LruList<string, string *> *lruList;

    // 一些特殊的 key 记录在这里，range 时需要忽略这些 key
    unordered_set<string> specialKeys;

    // static std::mutex instance_mutex;

    // static LevelDBKeyManager * instance;

public:

    recursive_mutex mutex;

    // static LevelDBKeyManager *getInstance() {
    //     if (instance == nullptr) {
    //         std::lock_guard<std::mutex> lock(instance_mutex);
    //         if (instance == nullptr) {
    //             instance = new LevelDBKeyManager(ConfigManager::getInstance().getLSMTreeDir().c_str());
    //         }
    //     }
    //     return instance;
    // }
    
    static LevelDBKeyManager *getInstance() {
        static LevelDBKeyManager instance(ConfigManager::getInstance().getLSMTreeDir().c_str());
        return &instance;
    }

    ~LevelDBKeyManager();

    bool put(ValueLayout &valueLayout);

    bool batchPut(vector<ValueLayout> &valueLayouts);

    ValueLayout get(const string &key, bool needLock = true);

    void getKeys(string &startingKey, int num, vector<string> &keys,
                 vector<ValueLayout> &valueLocations);

    void getKeys(const string &startingKey, const string &endingKey, vector<string> &keys,
                 vector<ValueLayout> &valueLocations);

    bool deleteKey(const string &key);

    bool writeMeta(const string &key, const string &value);

    bool getMeta(const string &key, string &value);

};

#endif //WISCKEY_LEVELDB_KEY_MANAGER_H
