#ifndef BUZZDB_H
#define BUZZDB_H

#include "index/HashIndex.h"
#include "storage/BufferManager.h"

class BuzzDB {
public:
    BuzzDB();
    void insert(int key, int value);
    void executeQueries();

private:
    HashIndex hash_index;
    BufferManager buffer_manager;
    size_t max_number_of_tuples;
    size_t tuple_insertion_attempt_counter;
};

#endif // BUZZDB_H
