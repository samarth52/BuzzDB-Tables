#ifndef BUZZDB_HASH_INDEX_H
#define BUZZDB_HASH_INDEX_H

#include <vector>

class HashIndex {
private:
    struct HashEntry {
        int key;
        int value;
        int position;
        bool exists;

        HashEntry();
        HashEntry(int k, int v, int pos);
    };

    static const std::size_t capacity = 100;
    HashEntry hashTable[capacity];

    std::size_t hashFunction(int key) const;

public:
    HashIndex();
    void insertOrUpdate(int key, int value);
    int getValue(int key) const;
    std::vector<int> rangeQuery(int lowerBound, int upperBound) const;
    void print() const;
};

#endif // BUZZDB_HASH_INDEX_H
