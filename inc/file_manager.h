//
// Created by apple on 2023/1/26.
//

#ifndef TREEKV_FILE_MANAGER_H
#define TREEKV_FILE_MANAGER_H

#include <string>
#include <unordered_map>
#include <mutex>
#include <boost/thread.hpp>
#include "lru_list.h"
#include "configManager.h"

using namespace std;

// 原本的 FILE* 作为 lru 的 ValueType 的话没法 fclose，因此包一层
class FileWrapper {
public:
    FILE *fp;

    explicit FileWrapper(FILE *fp);

    virtual ~FileWrapper();
};

class FileManager {

private:

    LruList<int, FileWrapper *> *openedFiles;

    unordered_map<int, recursive_mutex *> fileMutexes;

    FileManager(const char *val_dir);

    recursive_mutex *getFileMutex(int groupId);
    // static FileManager* instance;
    // static std::mutex instance_mutex;

public:

    recursive_mutex mutex;

    virtual ~FileManager();

    // static FileManager *getInstance() {
    //     if(nullptr == instance)
    //     {
    //         std::lock_guard<std::mutex> lock(instance_mutex);
    //         if(nullptr == instance)
    //         {
    //             instance = new FileManager(ConfigManager::getInstance().getLSMTreeDir().c_str());
    //         }
    //     }
    //     return instance;
    // }

    static FileManager *getInstance() {
        static FileManager instance(ConfigManager::getInstance().getLSMTreeDir().c_str());
        return &instance;
    }

    string getFilename(int groupId);

    FILE *openFile(int groupId);

    FILE *resetFile(int groupId);

    void operateFileMutex(int groupId, bool lock);

    size_t getFileSize(int groupId);

};

#endif //TREEKV_FILE_MANAGER_H
