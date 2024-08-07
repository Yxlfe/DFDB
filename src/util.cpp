#include "util.h"
#include <algorithm>
#include <cctype>

vector<string> split(const string &s, const string &delimiter) {
    vector<string> res;
    string::size_type pos1, pos2;
    size_t len = s.length();
    pos2 = s.find(delimiter);
    pos1 = 0;
    while (string::npos != pos2) {
        res.push_back(s.substr(pos1, pos2 - pos1));
        pos1 = pos2 + delimiter.size();
        pos2 = s.find(delimiter, pos1);
    }
    if (pos1 != len) {
        res.push_back(s.substr(pos1));
    }

    return res;
}

// key 的最大长度为 24B，且不能存在空格
string validateKey(const string &key) {

    if (key.size() > KEY_LENGTH) {
        printf("max key length is %d, your key is %s\n", KEY_LENGTH, key.c_str());
        return INVALID_KEY;
    }

    for (int i = 0; i < key.size(); ++i) {
        if (key[i] == ' ') {
            printf("blank exist in the key, invalid, your key is %s\n", key.c_str());
            return INVALID_KEY;
        }
    }

    string res = key;

    while (res.size() < KEY_LENGTH) {
        res += " ";
    }

    return res;

}

string randStr(const int len) {
    string str;
    char c;
    for (int idx = 0; idx < len; idx++) {
        c = 'a' + rand() % 26;
        str.push_back(c);
    }
    return str;
}

string trim(const string &str) {
    string temp;
    for (int i = 0; i < str.length(); i++) {
        if (str[i] != ' ') {
            temp += str[i];
        }
    }
    return temp;
}











