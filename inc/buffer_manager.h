//
// Created by apple on 2023/1/15.
//

#ifndef WISCKEY_BUFFER_MANAGER_H
#define WISCKEY_BUFFER_MANAGER_H

#include <vector>
#include <unordered_map>
#include <string>
#include <mutex>
#include "value_layout.h"
#include "threadpool/pool.hpp"

using namespace std;

class BufferManager {

private:

    unordered_map<string, string> initialBuffer;
    size_t initialBufferSize;

    vector<string> pivots;

    vector<unordered_map<string, string>> buffers;
    vector<size_t> bufferSizes;

    boost::threadpool::pool _flushThreadPool;

    BufferManager();

public:

    virtual ~BufferManager();

    recursive_mutex mutex;

    // static BufferManager instance;

    static BufferManager *getInstance() {
        static BufferManager instance;
        return &instance;
    }

    // return -1 if not need flush
    int put(const string &key, const string &value);

    bool get(const string &key, string &value);

    void del(const string &key);

    bool flush(int idx, bool needLock = true);

    void flushAll();

    bool pivotsGenerated();

    void initialGetRange(std::string &startingKey, int numKeys, std::vector<std::string> &keys,
                         std::vector<std::string> &values);

    int getBelongingGroup(const string &key);

    vector<string> getGroupBound(int groupId);

};


#endif //WISCKEY_BUFFER_MANAGER_H
