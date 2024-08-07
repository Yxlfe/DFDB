#include "group.h"
#include "file_manager.h"
#include "constant.h"
#include <numeric>

Group::Group(int groupId) : groupId(groupId) {}

void Group::batchPut(unordered_map<string, string> &pairs, size_t totalSize, vector<ValueLayout> &valueLayouts) {

//    cout << "===========groupBatchPut begin===========" << endl;

    FileManager *fileManager = FileManager::getInstance();

    // 最终要落盘的文件
    FILE *fp = fileManager->openFile(groupId);
    fileManager->operateFileMutex(groupId, LOCK);

    // 先把 kv 都写到 data 里
    void *data = malloc(totalSize);
    auto *ptr = (uint8_t *) data;

    fseek(fp, 0, SEEK_END);
    size_t writeFrom = ftell(fp);

    for (auto &pair: pairs) {

        string key = pair.first;
        string value = pair.second;
        uint32_t valueSize = value.length();

        ValueLayout valueLayout;
        valueLayout.setValueInfo(valueSize, key, value);
        valueLayout.setPositionInfo(groupId, writeFrom + ptr - (uint8_t *) data,
                                    sizeof(uint32_t) + key.length() + value.length());
        valueLayouts.push_back(valueLayout);

        // value size, 4B
        memcpy(ptr, &valueSize, sizeof(uint32_t));
//        printf("put valueSize = %d\n", valueSize);
        ptr += sizeof(uint32_t);

        // key
        memcpy(ptr, key.c_str(), key.size());
        ptr += key.size();

        // value
        memcpy(ptr, value.c_str(), value.size());
        ptr += value.size();

    }

    fwrite(data, 1, totalSize, fp);

    fileManager->operateFileMutex(groupId, UNLOCK);

    free(data);

//    cout << "===========groupBatchPut end===========" << endl;

}

size_t
Group::rewrite(vector<std::string> &keys, vector<std::string> &values, vector<ValueLayout> &valueLayouts) {

    size_t totalSize = accumulate(values.begin(), values.end(), 0, [](size_t sum, const std::string &value) {
        return sum + (sizeof(uint32_t) + KEY_LENGTH + value.length());
    });

    FileManager *fileManager = FileManager::getInstance();

    fileManager->operateFileMutex(groupId, LOCK);

    FILE *fp = fileManager->resetFile(groupId);

    // 先把 kv 都写到 data 里
    void *data = malloc(totalSize);
    auto *ptr = (uint8_t *) data;

//    fseek(fp, 0, SEEK_SET);
//    int writeFrom = ftell(fp);
//    if (writeFrom != 0) {
//        printf("writeFrom should be 0\n");
//    }

    for (int i = 0; i < keys.size(); ++i) {

        string key = keys[i];
        string value = values[i];
        uint32_t valueSize = value.length();

        ValueLayout valueLayout;
        valueLayout.setValueInfo(valueSize, key, value);
        valueLayout.setPositionInfo(groupId, ptr - (uint8_t *) data,
                                    sizeof(uint32_t) + key.length() + value.length());
        valueLayouts.push_back(valueLayout);

        // value size, 4B
        memcpy(ptr, &valueSize, sizeof(uint32_t));
//        printf("put valueSize = %d\n", valueSize);
        ptr += sizeof(uint32_t);

        // key
        memcpy(ptr, key.c_str(), key.size());
        ptr += key.size();

        // value
        memcpy(ptr, value.c_str(), value.size());
        ptr += value.size();

    }

    fwrite(data, 1, totalSize, fp);

    fileManager->operateFileMutex(groupId, UNLOCK);

    free(data);

    return totalSize;

}

// 一个 offset 和 length 里可能会对应多个 valueLayout
void Group::read(vector<size_t> &offsets, vector<size_t> &lengths, vector<ValueLayout *> &valueLayouts) {

    // 处理到哪个 valueLayout 了
    int layoutPtr = 0;

    FileManager *fileManager = FileManager::getInstance();

    for (int i = 0; i < offsets.size(); ++i) {

        size_t offset = offsets[i];
        size_t length = lengths[i];

        void *data = malloc(length);

        FILE *fp = fileManager->openFile(groupId);
        fileManager->operateFileMutex(groupId, LOCK);
        fseek(fp, offset, SEEK_SET);
        fread(data, 1, length, fp);
        fileManager->operateFileMutex(groupId, UNLOCK);

        auto *ptr = (uint8_t *) data;

        while (ptr - (uint8_t *) data < length) {

            uint32_t valueSize;
            string key;
            string value;

            memcpy(&valueSize, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);

            key.resize(KEY_LENGTH);
            value.resize(valueSize);

            memcpy((void *) key.c_str(), ptr, KEY_LENGTH);
            ptr += KEY_LENGTH;

            memcpy((void *) value.c_str(), ptr, valueSize);
            ptr += valueSize;

            valueLayouts[layoutPtr++]->setValueInfo(valueSize, key, value);

        }

        free(data);

    }

}

// 只有 gc 才会用到这个方法，而 gc 肯定是已经获取了对应 group 的写锁的，因此方法内部不再需要考虑锁的事情
void Group::readAndReset(unordered_map<string, ValueLayout> &layouts) {

    FileManager *fileManager = FileManager::getInstance();

    FILE *fp = fileManager->openFile(groupId);

    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);

    if (size == 0) {
        return;
    }

    void *data = malloc(size);

    fileManager->operateFileMutex(groupId, LOCK);
    fseek(fp, 0, SEEK_SET);
    fread(data, 1, size, fp);
    fileManager->operateFileMutex(groupId, UNLOCK);

    auto *ptr = (uint8_t *) data;

    while (ptr - (uint8_t *) data < size) {

        uint32_t valueSize;
        string key;
        string value;

        memcpy(&valueSize, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);

        key.resize(KEY_LENGTH);
        value.resize(valueSize);

        memcpy((void *) key.c_str(), ptr, KEY_LENGTH);
        ptr += KEY_LENGTH;

        memcpy((void *) value.c_str(), ptr, valueSize);
        ptr += valueSize;

        layouts[key].setValueInfo(valueSize, key, value);

    }

    free(data);

    fileManager->resetFile(groupId);

}

// 文件统一在 FileManager 中关闭
Group::~Group() {
//    FileManager *fileManager = FileManager::getInstance();
//    string filename = fileManager->getFilename(groupId, LINEAR_NODE_ID);
//    fileManager->closeFile(filename);
}
