#ifndef BUZZDB_TUPLE_H
#define BUZZDB_TUPLE_H

#include "Field.h"
#include <vector>
#include <memory>
#include <string>

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field);
    size_t getSize() const;

    std::string serialize();
    void serialize(std::ofstream& out);
    static std::unique_ptr<Tuple> deserialize(std::istream& in);

    std::unique_ptr<Tuple> clone() const;
    void print() const;
};

#endif // BUZZDB_TUPLE_H
