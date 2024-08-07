#include "value_layout.h"
#include "util.h"

ValueLayout::ValueLayout() {
    valueInfo.valid = false;
    positionInfo.valid = false;
}

void ValueLayout::setValueInfo(uint32_t valueSize, const string &key, const string &value) {
    valueInfo.valueSize = valueSize;
    valueInfo.key = key;
    valueInfo.value = value;
    valueInfo.valid = true;
}

void ValueLayout::setPositionInfo(int groupId, size_t offset, size_t length) {
    positionInfo.groupId = groupId;
    positionInfo.offset = offset;
    positionInfo.length = length;
    positionInfo.valid = true;
}

std::string ValueLayout::serializePosition() {
    return std::to_string(positionInfo.groupId) + "," +
           std::to_string(positionInfo.offset) + "," + std::to_string(positionInfo.length);
}

bool ValueLayout::deserializePosition(const string &str) {
    vector<string> v = split(str, ",");
    char *useless;
//    printf("v[0]=%s\n", v[0].c_str());
//    printf("v[1]=%s\n", v[1].c_str());
//    printf("v[2]=%s\n", v[2].c_str());
    setPositionInfo(atoi(v[0].c_str()), strtoul(v[1].c_str(), &useless, 10), strtoul(v[2].c_str(), &useless, 10));
    return true;
}

const ValueInfo &ValueLayout::getValueInfo() const {
    return valueInfo;
}

const PositionInfo &ValueLayout::getPositionInfo() const {
    return positionInfo;
}

bool ValueLayout::layoutCompare(const ValueLayout &rhs) {
    return positionInfo.groupId == rhs.positionInfo.groupId &&
           positionInfo.offset == rhs.positionInfo.offset && positionInfo.length == rhs.positionInfo.length;
}
