#include "../../include/core/Tuple.h"
#include <sstream>
#include <iostream>
#include <fstream>

void Tuple::addField(std::unique_ptr<Field> field) {
    fields.push_back(std::move(field));
}

size_t Tuple::getSize() const {
    size_t size = 0;
    for (const auto& field : fields) {
        size += field->data_length;
    }
    return size;
}

std::string Tuple::serialize() {
    std::stringstream buffer;
    buffer << fields.size() << ' ';
    for (const auto& field : fields) {
        buffer << field->serialize();
    }
    return buffer.str();
}

void Tuple::serialize(std::ofstream& out) {
    std::string serializedData = this->serialize();
    out << serializedData;
}

std::unique_ptr<Tuple> Tuple::deserialize(std::istream& in) {
    auto tuple = std::make_unique<Tuple>();
    size_t fieldCount; in >> fieldCount;
    for (size_t i = 0; i < fieldCount; ++i) {
        tuple->addField(Field::deserialize(in));
    }
    return tuple;
}

std::unique_ptr<Tuple> Tuple::clone() const {
    auto clonedTuple = std::make_unique<Tuple>();
    for (const auto& field : fields) {
        clonedTuple->addField(field->clone());
    }
    return clonedTuple;
}

void Tuple::print() const {
    for (const auto& field : fields) {
        field->print();
        std::cout << " ";
    }
    std::cout << "\n";
}
