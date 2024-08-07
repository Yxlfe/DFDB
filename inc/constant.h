//
// Created by apple on 2023/1/13.
//

#ifndef WISCKEY_CONSTANT_H
#define WISCKEY_CONSTANT_H

static const int POOL_THREADS_NUM = 8;

// 40GB
static const size_t DISK_SIZE = 40L * 1024 * 1024 * 1024;

// 每 5s 进行一次 gc check
static const int GC_CHECK_INTERVAL = 999999999;

static const int KEY_LENGTH = 32;

// initial buffer 的大小，用键值对的数量即可
static const int MAX_INITIAL_BUFFER_AMOUNT = 10000;

// 4MB，各个 group 的 buffer 的大小
static const int MAX_BUFFER_SIZE = 4 * 1024 * 1024;

static const int GROUP_NUM = 256;

// 各个 lru 的容量
static const int LEVELDB_LRU_CAPACITY = 20000;
static const int FILE_LRU_CAPACITY = 250;

static const bool LOCK = true;
static const bool UNLOCK = false;

static const std::string PIVOTS_KEY = "+(!%)$*";

static const std::string DELETED_VALUE = "+(!@$*";

static const std::string INVALID_KEY = "+(!@$*G!B";

static const std::string INF_LOWER_BOUND = "+(!B";
static const std::string INF_UPPER_BOUND = "!@!B";

static const int INITIAL_GROUP_ID = -1;

static const int INVALID_GROUP_ID = INT32_MAX;

static const int RANGE_QUERY_TIME_COST = 0;
static const int RANGE_QUERY_RANDOM_READ = 1;
static const int GC_TIME_COST = 2;
static const int GC_WRITE_BYTES = 3;

#endif //WISCKEY_CONSTANT_H
