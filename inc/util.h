//
// Created by apple on 2023/1/25.
//

#ifndef TREEKV_UTIL_H
#define TREEKV_UTIL_H

#include <string>
#include <vector>
#include <unordered_map>
#include "constant.h"

using namespace std;

vector<string> split(const string &s, const string &delimiter);

string validateKey(const string &key);

string randStr(const int len);

string trim(const string &str);

//template<typename KeyType, typename ValueType>
//vector<ValueType> getKeyVector(unordered_map<KeyType, ValueType> map);


#endif //TREEKV_UTIL_H
