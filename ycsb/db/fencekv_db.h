//
// Created by apple on 2023/2/20.
//

#ifndef TREEKV_TREEKV_DB_H
#define TREEKV_TREEKV_DB_H

#include "../core/db.h"
#include <iostream>
#include <string>
#include <mutex>
#include "../core/properties.h"
#include "../../server.h"

using std::cout;
using std::endl;

namespace ycsbc {

    class FenceKV : public DB {

    private:
        std::mutex mutex_;
        Server *server;

    public:

        FenceKV() {
            server = Server::getInstance();
        }

        int Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                 std::vector<KVPair> &result) {

            std::lock_guard<std::mutex> lock(mutex_);

            string value;
            // 只操作，不管正确性
            server->get(key, value);

            return 0;

        }

        int Scan(const std::string &table, const std::string &key, const std::string &max_key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result) {

            std::lock_guard<std::mutex> lock(mutex_);

            vector<string> keys;
            vector<string> values;
            server->getRange(key, len, keys, values);

            return 0;

        }

        int Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {

            std::lock_guard<std::mutex> lock(mutex_);

            // 具体插入的 value 随便选一个就行
            server->put(key, values[0].second);

            return 0;

        }

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values) {

            std::lock_guard<std::mutex> lock(mutex_);

            server->put(key, values[0].second);

            return 0;

        }

        int Delete(const std::string &table, const std::string &key) {

            std::lock_guard<std::mutex> lock(mutex_);

            server->del(key);

            return 0;

        }

    };

} // ycsbc


#endif //TREEKV_TREEKV_DB_H
