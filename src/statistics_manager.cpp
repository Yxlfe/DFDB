#include "statistics_manager.h"
#include "constant.h"

StatisticsManager::StatisticsManager() {}

int StatisticsManager::startTimer() {

    lock_guard<mutex> lockGuard(m);

    auto startTime = chrono::high_resolution_clock::now();

    mt19937 gen(rd());
    uniform_int_distribution<> dis(numeric_limits<int>::min(), numeric_limits<int>::max());

    int randomNumber = dis(gen);
    while (startMap.find(randomNumber) != startMap.end()) {
        randomNumber = dis(gen);
    }

    startMap[randomNumber] = startTime;

    return randomNumber;

}

void StatisticsManager::stopTimer(int type, int randomNumber) {

    lock_guard<mutex> lockGuard(m);

    auto it = startMap.find(randomNumber);
    if (it == startMap.end()) {
        printf("random number not exist!!\n");
        return;
    }

    auto startTime = it->second;

    auto stopTime = chrono::high_resolution_clock::now();

    auto duration = chrono::duration_cast<chrono::microseconds>(stopTime - startTime);

    size_t cost = duration.count();

    if (statisticsMap.find(type) == statisticsMap.end()) {
        vector<size_t> v{cost};
        statisticsMap[type] = v;
    } else {
        statisticsMap[type].emplace_back(cost);
    }

    startMap.erase(it);

}

void StatisticsManager::addCount(int type, size_t amount) {
    lock_guard<mutex> lockGuard(m);
    if (statisticsMap.find(type) == statisticsMap.end()) {
        vector<size_t> v{amount};
        statisticsMap[type] = v;
    } else {
        statisticsMap[type].emplace_back(amount);
    }
}

void StatisticsManager::printStatistics() {

    printf("===================== print statistics =====================\n");

    printf("=============== range query =====================\n");

    // 1.
    vector<size_t> rangeQueryTimeCosts = statisticsMap[RANGE_QUERY_TIME_COST];

    int rangeQueryCount = rangeQueryTimeCosts.size();
    printf("range query # %d\n", rangeQueryCount);

    double totalQueryTime = accumulate(rangeQueryTimeCosts.begin(), rangeQueryTimeCosts.end(), 0,
                                       [](size_t sum, size_t cost) {
                                           return sum + cost;
                                       }) / 1000.0;
    printf("totalQueryTime = %f ms\n", totalQueryTime);

    double averageQueryTime = totalQueryTime / rangeQueryTimeCosts.size();
    printf("averageQueryTime = %f ms\n", averageQueryTime);

    // 2.
    vector<size_t> randomReadCounts = statisticsMap[RANGE_QUERY_RANDOM_READ];

    int totalRandomReadCount = accumulate(randomReadCounts.begin(), randomReadCounts.end(), 0,
                                          [](int sum, int _count) {
                                              return sum + _count;
                                          });
    printf("totalRandomReadCount = %d\n", totalRandomReadCount);

    double averageRandomReadCount = totalRandomReadCount * 1.0 / randomReadCounts.size();
    printf("averageRandomReadCount = %f\n", averageRandomReadCount);

    printf("=============== gc =====================\n");

    // 3.
    vector<size_t> gcTimeCosts = statisticsMap[GC_TIME_COST];

    int gcCount = gcTimeCosts.size();
    printf("gc # %d\n", gcCount);

    double totalGcTime = accumulate(gcTimeCosts.begin(), gcTimeCosts.end(), 0,
                                    [](size_t sum, size_t cost) {
                                        return sum + cost;
                                    }) / 1000.0;
    printf("totalGcTime = %f ms\n", totalGcTime);

    double averageGcTime = totalGcTime / gcTimeCosts.size();
    printf("averageGcTime = %f ms\n", averageGcTime);

    string gcTimeStr = "";
    for (size_t i = 0; i < gcTimeCosts.size(); ++i) {
        gcTimeStr += (to_string(gcTimeCosts[i] / 1000.0) + ",");
    }
    printf("gcTimeStr(ms) = %s\n", gcTimeStr.c_str());

    // 4.
    vector<size_t> writeBytes = statisticsMap[GC_WRITE_BYTES];

    double totalWriteCount = accumulate(writeBytes.begin(), writeBytes.end(), 0,
                                        [](double sum, size_t bytes) {
                                            return sum + bytes / 1024.0;
                                        });
    printf("totalWriteCount = %f KB\n", totalWriteCount);

    double averageWriteCount = totalWriteCount / writeBytes.size();
    printf("averageWriteCount = %f KB\n", averageWriteCount);

    string gcWriteBytesStr = "";
    for (size_t i = 0; i < writeBytes.size(); ++i) {
        gcWriteBytesStr += (to_string(writeBytes[i] / 1024.0) + ",");
    }
    printf("gcWriteBytesStr(KB) = %s\n", gcWriteBytesStr.c_str());

    printf("===================== print statistics end =====================\n");

}

StatisticsManager::~StatisticsManager() {
    printStatistics();
}
