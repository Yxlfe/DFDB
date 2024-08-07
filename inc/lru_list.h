//
// Created by apple on 2023/2/18.
//

#ifndef TREEKV_LRU_LIST_H
#define TREEKV_LRU_LIST_H

#include <unordered_map>
#include "linked_node.h"
#include "constant.h"
#include <mutex>

using namespace std;

// KeyType 保险起见只用 int string
// ValueType 必须是指针
template<class KeyType, class ValueType>
class LruList {

private:
    unordered_map<KeyType, DLinkedNode<KeyType, ValueType> *> cache;
    DLinkedNode<KeyType, ValueType> *head;
    DLinkedNode<KeyType, ValueType> *tail;
    int size;
    int capacity;
    mutex m;

    void addToHead(DLinkedNode<KeyType, ValueType> *node);

    void removeNode(DLinkedNode<KeyType, ValueType> *node);

    void moveToHead(DLinkedNode<KeyType, ValueType> *node);

    DLinkedNode<KeyType, ValueType> *removeTail();

public:
    LruList(int _capacity);

    virtual ~LruList();

    ValueType get(KeyType key);

    void put(KeyType key, ValueType value);

    void del(KeyType key);

};

template<class KeyType, class ValueType>
LruList<KeyType, ValueType>::LruList(int _capacity) {
    capacity = _capacity;
    size = 0;
    // 使用伪头部和伪尾部节点
    head = new DLinkedNode<KeyType, ValueType>();
    tail = new DLinkedNode<KeyType, ValueType>();
    head->next = tail;
    tail->prev = head;
}

template<class KeyType, class ValueType>
ValueType LruList<KeyType, ValueType>::get(KeyType key) {
    lock_guard<mutex> lockGuard(m);
    if (cache.find(key) == cache.end()) {
        return nullptr;
    }
    // 如果 key 存在，先通过哈希表定位，再移到头部
    DLinkedNode<KeyType, ValueType> *node = cache[key];
    moveToHead(node);
    return node->value;
}

template<class KeyType, class ValueType>
void LruList<KeyType, ValueType>::put(KeyType key, ValueType value) {
    lock_guard<mutex> lockGuard(m);
    if (cache.find(key) == cache.end()) {
        // 如果 key 不存在，创建一个新的节点
        auto *node = new DLinkedNode<KeyType, ValueType>(key, value);
        // 添加进哈希表
        cache[key] = node;
        // 添加至双向链表的头部
        addToHead(node);
        ++size;
        if (size > capacity) {
            // 如果超出容量，删除双向链表的尾部节点
            DLinkedNode<KeyType, ValueType> *removed = removeTail();
            // 删除哈希表中对应的项
            cache.erase(removed->key);
            // 防止内存泄漏
            delete removed;
            --size;
        }
    } else {
        // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
        DLinkedNode<KeyType, ValueType> *node = cache[key];
        // 这里应该得把原来的 value 给 delete 掉
        delete node->value;
        node->value = value;
        moveToHead(node);
    }
}

template<class KeyType, class ValueType>
void LruList<KeyType, ValueType>::del(KeyType key) {
    lock_guard<mutex> lockGuard(m);
    if (cache.find(key) != cache.end()) {
        DLinkedNode<KeyType, ValueType> *node = cache[key];
        cache.erase(key);
        removeNode(node);
        delete node;
        size--;
    }
}

template<class KeyType, class ValueType>
void LruList<KeyType, ValueType>::addToHead(DLinkedNode<KeyType, ValueType> *node) {
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;
}

template<class KeyType, class ValueType>
void LruList<KeyType, ValueType>::removeNode(DLinkedNode<KeyType, ValueType> *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

template<class KeyType, class ValueType>
DLinkedNode<KeyType, ValueType> *LruList<KeyType, ValueType>::removeTail() {
    DLinkedNode<KeyType, ValueType> *node = tail->prev;
    removeNode(node);
    return node;
}

template<class KeyType, class ValueType>
void LruList<KeyType, ValueType>::moveToHead(DLinkedNode<KeyType, ValueType> *node) {
    removeNode(node);
    addToHead(node);
}

template<class KeyType, class ValueType>
LruList<KeyType, ValueType>::~LruList() {
    delete head;
    delete tail;
    for (auto it = cache.begin(); it != cache.end(); it++) {
        delete it->second;
    }
}


#endif //TREEKV_LRU_LIST_H
