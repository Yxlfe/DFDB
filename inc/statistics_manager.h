//
// Created by apple on 2023/3/29.
//

#ifndef TREEKV_STATISTICS_MANAGER_H
#define TREEKV_STATISTICS_MANAGER_H

#include <vector>
#include <unordered_map>
#include <chrono>
#include <random>
#include <mutex>

using namespace std;

class StatisticsManager {

private:

    mutex m;

    // 随机种子
    random_device rd;

    // 每次 timer 启动，会生成一个随机数作为 key，并将启动时间作为 value 存到这里
    unordered_map<int, chrono::high_resolution_clock::time_point> startMap;

    unordered_map<int, vector<size_t>> statisticsMap;

    StatisticsManager();

    void printStatistics();

public:

    static StatisticsManager *getInstance() {
        static StatisticsManager instance;
        return &instance;
    }

    virtual ~StatisticsManager();

    int startTimer();

    void stopTimer(int type, int randomNumber);

    void addCount(int type, size_t amount);

};


#endif //TREEKV_STATISTICS_MANAGER_H
