//
// Created by apple on 2023/3/4.
//

#ifndef TREEKV_LINEAR_PART_H
#define TREEKV_LINEAR_PART_H

#include <vector>
#include <string>
#include <unordered_map>
#include "value_layout.h"
#include <mutex>

using namespace std;

class Group {

public:
    int groupId;

    explicit Group(int groupId);

    virtual ~Group();

    void batchPut(unordered_map<string, string> &pairs, size_t totalSize, vector<ValueLayout> &valueLayouts);

    size_t rewrite(vector<string> &keys, vector<string> &values, vector<ValueLayout> &valueLayouts);

    void read(vector<size_t> &offsets, vector<size_t> &lengths, vector<ValueLayout *> &valueLayouts);

    void readAndReset(unordered_map<string, ValueLayout> &layouts);

};


#endif //TREEKV_LINEAR_PART_H
