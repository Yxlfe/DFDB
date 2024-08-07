//
// Created by apple on 2023/2/18.
//

#ifndef TREEKV_LINKED_NODE_H
#define TREEKV_LINKED_NODE_H

template<class KeyType, class ValueType>
class DLinkedNode {

public:

    KeyType key;
    ValueType value;
    DLinkedNode *prev;
    DLinkedNode *next;

    DLinkedNode();

    DLinkedNode(KeyType _key, ValueType _value);

    virtual ~DLinkedNode();

};

template<class KeyType, class ValueType>
DLinkedNode<KeyType, ValueType>::DLinkedNode() : value(nullptr), prev(nullptr), next(nullptr) {}

template<class KeyType, class ValueType>
DLinkedNode<KeyType, ValueType>::DLinkedNode(KeyType _key, ValueType _value) : key(_key), value(_value),
                                                                               prev(nullptr),
                                                                               next(nullptr) {}

template<class KeyType, class ValueType>
DLinkedNode<KeyType, ValueType>::~DLinkedNode() {
    if (value != nullptr) {
        delete value;
    }
}


#endif //TREEKV_LINKED_NODE_H
