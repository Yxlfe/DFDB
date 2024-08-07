//
// Created by apple on 2023/1/13.
//

#ifndef WISCKEY_VALUE_LOCATION_H
#define WISCKEY_VALUE_LOCATION_H

#include <string>
#include <ostream>

using namespace std;

typedef struct PositionInfo {
    int groupId;
    size_t offset;
    size_t length;
    bool valid;
} PositionInfo;

typedef struct ValueInfo {
    uint32_t valueSize;
    string key;
    string value;
    bool valid;
} ValueInfo;

class ValueLayout {

private:

    ValueInfo valueInfo;

    PositionInfo positionInfo;

public:

    ValueLayout();

    void setValueInfo(uint32_t valueSize, const string &key, const string &value);

    void setPositionInfo(int groupId, size_t offset, size_t length);

    const ValueInfo &getValueInfo() const;

    const PositionInfo &getPositionInfo() const;

    std::string serializePosition();

    bool deserializePosition(const string &str);

    bool layoutCompare(const ValueLayout &rhs);

};


#endif //WISCKEY_VALUE_LOCATION_H
