//
// Created by apple on 2023/1/13.
//

#ifndef WISCKEY_VALUE_LOG_H
#define WISCKEY_VALUE_LOG_H

#include <vector>
#include <cstdio>
#include <string>
#include <mutex>
#include "value_layout.h"
#include "leveldb_key_manager.h"
#include "group.h"

using namespace std;

class ValueLog {

private:

    // 各个 group 的增量
    // 比如 group1 在 gc 后 increments[1] 为 0，后续往 group1 里又放了 10 个 kv，那么 increments[1] 为 10
    vector<int> increments;

    // 整个数据库的大小，不用很精确，差不多就可以
    size_t totalDbSize;

    mutex m;

    ValueLog();

    Group getGroup(int groupId);

public:

    virtual ~ValueLog();

    static ValueLog *getInstance() {
        static ValueLog instance;
        return &instance;
    }

    size_t getTotalDbSize();

    void groupBatchPut(unordered_map<string, string> &buffer, size_t bufferSize, int groupId,
                       vector<ValueLayout> &valueLayouts);

    size_t groupRewrite(vector<string> &keys, vector<string> &values, int groupId,
                        vector<ValueLayout> &valueLayouts);

    void readGroupAndReset(int groupId, unordered_map<string, ValueLayout> &layouts);

    bool assignValueInfo(const string &key, ValueLayout &valueLayout);

    void assignValueInfo(vector<string> &keys, vector<ValueLayout> &valueLayouts, bool isGc = false);

    int getGroupWithMaxIncr();

};


#endif //WISCKEY_VALUE_LOG_H
