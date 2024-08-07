#include "value_log.h"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <stdio.h>
#include "constant.h"
#include "file_manager.h"
#include "util.h"
#include "thread_pool_manager.h"
#include "statistics_manager.h"

void ValueLog::groupBatchPut(unordered_map<string, string> &buffer, size_t bufferSize, int groupId,
                             vector<ValueLayout> &valueLayouts) {
    getGroup(groupId).batchPut(buffer, bufferSize, valueLayouts);
    if (groupId != INITIAL_GROUP_ID) {
        m.lock();
        increments[groupId] += buffer.size();
        totalDbSize += bufferSize;
        m.unlock();
    }
}

size_t ValueLog::groupRewrite(vector<std::string> &keys, vector<std::string> &values, int groupId,
                              vector<ValueLayout> &valueLayouts) {
    size_t oldSize = FileManager::getInstance()->getFileSize(groupId);
    size_t rewriteSize = getGroup(groupId).rewrite(keys, values, valueLayouts);
    if (groupId != INITIAL_GROUP_ID) {
        m.lock();
        totalDbSize -= oldSize;
        totalDbSize += rewriteSize;
        increments[groupId] = 0;
        m.unlock();
    }
    return rewriteSize;
}

// fencekv 里似乎只有 initialBuffer 会使用，所以不涉及 totalDbSize
void ValueLog::readGroupAndReset(int groupId, unordered_map<string, ValueLayout> &layouts) {
    getGroup(groupId).readAndReset(layouts);
}

bool ValueLog::assignValueInfo(const string &key, ValueLayout &valueLayout) {

    PositionInfo positionInfo = valueLayout.getPositionInfo();

    if (!positionInfo.valid) {
        printf("position not specified!\n");
        return false;
    }

    FileManager *fileManager = FileManager::getInstance();

    FILE *fp = fileManager->openFile(positionInfo.groupId);

//    printf("key = %s\n", key.c_str());
//    printf("positionInfo.groupId = %d\n", positionInfo.groupId);
//    printf("positionInfo.offset = %d\n", positionInfo.offset);
//    printf("positionInfo.length = %d\n", positionInfo.length);

    void *data = malloc(positionInfo.length);

    fileManager->operateFileMutex(positionInfo.groupId, LOCK);
    fseek(fp, positionInfo.offset, SEEK_SET);
    fread(data, 1, positionInfo.length, fp);
    fileManager->operateFileMutex(positionInfo.groupId, UNLOCK);

    uint32_t valueSize;
    string value;

    auto *ptr = (uint8_t *) data;

    memcpy(&valueSize, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    ptr += KEY_LENGTH;

    value.resize(valueSize);

//    printf("valueSize = %d\n", (int) valueSize);
    memcpy((void *) value.c_str(), ptr, valueSize);

    free(data);

    valueLayout.setValueInfo(valueSize, key, value);

    return true;

}

// 获取 group 的锁后使用
void ValueLog::assignValueInfo(vector<string> &keys, vector<ValueLayout> &valueLayouts, bool isGc) {

//    printf("assignValueInfo\n");

    if (keys.empty()) {
        return;
    }

    StatisticsManager *statisticsManager = StatisticsManager::getInstance();

    // 一个 group 中实际要进行 io 的那些 offset 和 length
    vector<size_t> offsets;
    vector<size_t> lengths;
    vector<ValueLayout *> layouts;

    size_t preOffset = SIZE_MAX;
    size_t preLength = SIZE_MAX;

    // 逐个 group 进行操作
    int currentGroup = valueLayouts[0].getPositionInfo().groupId;

    size_t totalRandomReadCount = 0;

    for (auto &layout: valueLayouts) {

        int group = layout.getPositionInfo().groupId;
        size_t offset = layout.getPositionInfo().offset;
        size_t length = layout.getPositionInfo().length;

        if (group != currentGroup) {

            offsets.emplace_back(preOffset);
            lengths.emplace_back(preLength);

//            printf("group %d collect end,involving %d random reads, containing %d address\n", currentGroup,
//                   offsets.size(), layouts.size());
            totalRandomReadCount += offsets.size();

            //这里考虑加一个并发操作
            // 到 group 里去读 value
            getGroup(currentGroup).read(offsets, lengths, layouts);

            offsets.clear();
            lengths.clear();
            layouts.clear();
            preOffset = SIZE_MAX;
            preLength = SIZE_MAX;
            currentGroup = group;

        }

        layouts.emplace_back(&layout);

        // 初始情况
        if (preOffset == SIZE_MAX) {
            preOffset = offset;
            preLength = length;
            continue;
        }

        // 如果当前的 address 和上一个 address 是连起来的，那么拼到一起
        if (preOffset + preLength == offset) {
            preLength += length;
            continue;
        }

        // 否则，把上一个存起来
        offsets.emplace_back(preOffset);
        lengths.emplace_back(preLength);

        preOffset = offset;
        preLength = length;

    }

    offsets.emplace_back(preOffset);
    lengths.emplace_back(preLength);

//    printf("group %d collect end,involving %d random reads, containing %d address\n", currentGroup,
//           offsets.size(), layouts.size());
    totalRandomReadCount += offsets.size();

    // 只统计 range query 阶段的随机读次数
    if (!isGc) statisticsManager->addCount(RANGE_QUERY_RANDOM_READ, totalRandomReadCount);

    // 到 group 里去读 value
    getGroup(currentGroup).read(offsets, lengths, layouts);

}

int ValueLog::getGroupWithMaxIncr() {
    int max = -1;
    int idx = -1;
    for (int i = 0; i < GROUP_NUM; ++i) {
        if (increments[i] > max) {
            max = increments[i];
            idx = i;
        }
    }
    return idx;
}

Group ValueLog::getGroup(int groupId) {
    return Group(groupId);
}

ValueLog::ValueLog() {

    FileManager *fileManager = FileManager::getInstance();

    increments.resize(GROUP_NUM);

    for (int i = 0; i < GROUP_NUM; ++i) {
        size_t size = fileManager->getFileSize(i);
        totalDbSize += size;
    }

}

ValueLog::~ValueLog() {
//    printf("destructor ValueLog\n");
}

size_t ValueLog::getTotalDbSize() {
    lock_guard<mutex> lockGuard(m);
    return totalDbSize;
}

