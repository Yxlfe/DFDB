//
// Created by apple on 2023/1/13.
//

#ifndef WISCKEY_SERVER_H
#define WISCKEY_SERVER_H

#include <string>
#include <vector>
#include "value_log.h"
#include "leveldb_key_manager.h"
#include "configManager.h"

namespace dfdb{
// todo 目前没有考虑 crash 的处理
class Server {

private:
//    LruList<string, string *> *lruList;

    Server(const char* config);
    static Server * _instance;
    static std::mutex _instance_mutex;
public:

    static Server *getInstance(const char* config) {
        if(nullptr == _instance)
        {
            std::lock_guard<std::mutex> lock(_instance_mutex);
            if(nullptr == _instance)
            {
                _instance = new Server(config);
            }
        }
        return _instance;
    }

    static void releaseInstance() {
        std::lock_guard<std::mutex> lock(_instance_mutex);
        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }
    // static Server *getInstance(const char* config) {
    //     static Server instance(config);
    //     return &instance ;
    // }

    static Server *getInstance() {
        std::lock_guard<std::mutex> lock(_instance_mutex);
        return _instance;
    }

    virtual ~Server();

    bool put(const string &key, const std::string &value);

    bool get(const string &key, std::string &value);

    void getRange(const string &startingKey, int numKeys, std::vector<std::string> &keys,
                  std::vector<std::string> &values);

    void getRange(const string &startingKey, const string &endingKey, std::vector<std::string> &keys,
                  std::vector<std::string> &values);

    bool del(const string &key);

    void test();

};



}//namespace dfdb


#endif //WISCKEY_SERVER_H
