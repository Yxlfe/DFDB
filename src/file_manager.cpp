//
// Created by apple on 2023/1/26.
//

#include "file_manager.h"
#include "constant.h"
#include <unistd.h>
#include <boost/filesystem.hpp>

// FileManager* FileManager::instance = nullptr;
// std::mutex FileManager::instance_mutex;

FILE *FileManager::openFile(int groupId) {

    lock_guard<recursive_mutex> lockGuard(mutex);

    FileWrapper *wrapper = openedFiles->get(groupId);

    if (wrapper != nullptr) {
        return wrapper->fp;
    }

    FILE *fp;

    string filename = getFilename(groupId);
    if (access(filename.c_str(), F_OK) == 0) {
        fp = fopen(filename.c_str(), "rb+");
    } else {
        fp = fopen(filename.c_str(), "w+");
    }

    if (!fp) {
        printf("open file fail, file path: %s\n", filename.c_str());
    }

    openedFiles->put(groupId, new FileWrapper(fp));
    fileMutexes[groupId] = new recursive_mutex();

//    printf("open file success, file path: %s\n", filename);

    return fp;

}

// gc 使用，不需要管文件相关的锁
FILE *FileManager::resetFile(int groupId) {

//    printf("reset begin\n");

    lock_guard<recursive_mutex> lockGuard(mutex);

    FILE *fp = openFile(groupId);

    FileWrapper *wrapper = openedFiles->get(groupId);

    fclose(wrapper->fp);

    wrapper->fp = fopen(getFilename(groupId).c_str(), "w+");

//    printf("reset success\n");

    return wrapper->fp;

}

recursive_mutex *FileManager::getFileMutex(int groupId) {
    lock_guard<recursive_mutex> lockGuard(mutex);
    if (fileMutexes.find(groupId) == fileMutexes.end()) {
        return nullptr;
    }
    return fileMutexes[groupId];
}

void FileManager::operateFileMutex(int groupId, bool lock) {

    recursive_mutex *fileMutex = getFileMutex(groupId);

    if (lock) {
        fileMutex->lock();
//        printf("get file lock in group %d\n", groupId);
    } else {
        fileMutex->unlock();
//        printf("get file unlock in group %d\n", groupId);
    }

}

string FileManager::getFilename(int groupId) {
    std:: string val_dir = ConfigManager::getInstance().getVALDir();
    return val_dir + "/group@" + to_string(groupId) + "@";
}

FileManager::FileManager(const char *val_dir) {

    this->openedFiles = new LruList<int, FileWrapper *>(FILE_LRU_CAPACITY);

    boost::filesystem::path path;
    path += boost::filesystem::path(val_dir);
    if (boost::filesystem::exists(path) && boost::filesystem::is_directory(path)) {
        printf("path %s already exist\n", path.c_str());
    } else {
        boost::filesystem::create_directory(path);
        printf("path %s generate success\n", path.c_str());
    }

}

FileManager::~FileManager() {
    delete openedFiles;
    for (auto &fileMutex: fileMutexes) {
        delete fileMutex.second;
    }
//    printf("destructor FileManager\n");
}

size_t FileManager::getFileSize(int groupId) {

    boost::filesystem::path filePath(getFilename(groupId));

    if (!exists(filePath)) {
        return 0;
    }

    return file_size(filePath);

//    // 下面是如何遍历一个文件夹的案例，留着不删了
//    boost::filesystem::path dirPath("../data/group@" + to_string(groupId) + "@");
//
//    boost::filesystem::directory_iterator it(dirPath);
//    boost::filesystem::directory_iterator itEnd;
//
//    int dirSize = 0;
//
//    while (it != itEnd) {
//        dirSize += (file_size(it->path()));
//        it++;
//    }
//
//    return dirSize;

}

FileWrapper::FileWrapper(FILE *fp) : fp(fp) {}

FileWrapper::~FileWrapper() {
    fclose(fp);
}
