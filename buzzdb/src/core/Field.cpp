#include "../../include/core/Field.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstring>
#include <cassert>

Field::Field(int i) : type(INT) { 
    data_length = sizeof(int);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &i, data_length);
}

Field::Field(float f) : type(FLOAT) { 
    data_length = sizeof(float);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &f, data_length);
}

Field::Field(const std::string& s) : type(STRING) {
    data_length = s.size() + 1;  // include null-terminator
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), s.c_str(), data_length);
}

Field& Field::operator=(const Field& other) {
    if (&other == this) {
        return *this;
    }
    type = other.type;
    data_length = other.data_length;
    std::memcpy(data.get(), other.data.get(), data_length);
    return *this;
}

Field::Field(const Field& other) : type(other.type), data_length(other.data_length), data(new char[data_length]) {
    std::memcpy(data.get(), other.data.get(), data_length);
}

Field::Field(Field&& other) noexcept : type(other.type), data_length(other.data_length), data(std::move(other.data)) {}

FieldType Field::getType() const { return type; }

int Field::asInt() const { 
    return *reinterpret_cast<int*>(data.get());
}

float Field::asFloat() const { 
    return *reinterpret_cast<float*>(data.get());
}

std::string Field::asString() const { 
    return std::string(data.get());
}

std::string Field::serialize() {
    std::stringstream buffer;
    buffer << type << ' ' << data_length << ' ';
    if (type == STRING) {
        buffer << data.get() << ' ';
    } else if (type == INT) {
        buffer << *reinterpret_cast<int*>(data.get()) << ' ';
    } else if (type == FLOAT) {
        buffer << *reinterpret_cast<float*>(data.get()) << ' ';
    }
    return buffer.str();
}

void Field::serialize(std::ofstream& out) {
    std::string serializedData = this->serialize();
    out << serializedData;
}

std::unique_ptr<Field> Field::deserialize(std::istream& in) {
    int type; in >> type;
    size_t length; in >> length;
    if (type == STRING) {
        std::string val; in >> val;
        return std::make_unique<Field>(val);
    } else if (type == INT) {
        int val; in >> val;
        return std::make_unique<Field>(val);
    } else if (type == FLOAT) {
        float val; in >> val;
        return std::make_unique<Field>(val);
    }
    return nullptr;
}

std::unique_ptr<Field> Field::clone() const {
    return std::make_unique<Field>(*this);
}

void Field::print() const {
    switch(getType()){
        case INT: std::cout << asInt(); break;
        case FLOAT: std::cout << asFloat(); break;
        case STRING: std::cout << asString(); break;
    }
}

bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false;

    switch (lhs.type) {
        case INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}
