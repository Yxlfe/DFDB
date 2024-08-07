//
// Created by apple on 2023/1/17.
//

#ifndef WISCKEY_GC_MANAGER_H
#define WISCKEY_GC_MANAGER_H

#include <thread>
#include <vector>
#include <mutex>
#include <boost/asio/io_service.hpp>
#include <boost/asio/steady_timer.hpp>

using namespace std;

class GcManager {

private:

    // boost::asio::io_service &ctx;
    // boost::asio::steady_timer timer;
    // std::thread thread;

    // explicit GcManager(boost::asio::io_service &_ctx);
    explicit GcManager();

    // void startTimer();

public:

    static GcManager *getInstance() {
        // static boost::asio::io_service _ctx;
        // static GcManager instance(_ctx);
        static GcManager instance;        
        return &instance;
    }

    void gc(int groupId);

    void gcAll();

    virtual ~GcManager();

};


#endif //WISCKEY_GC_MANAGER_H
