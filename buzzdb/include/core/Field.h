#ifndef BUZZDB_FIELD_H
#define BUZZDB_FIELD_H

#include <memory>
#include <string>

enum FieldType { INT, FLOAT, STRING };

class Field {
public:
    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

    Field(int i);
    Field(float f);
    Field(const std::string& s);

    Field& operator=(const Field& other);
    Field(const Field& other);
    Field(Field&& other) noexcept;

    FieldType getType() const;
    int asInt() const;
    float asFloat() const;
    std::string asString() const;

    std::string serialize();
    void serialize(std::ofstream& out);
    static std::unique_ptr<Field> deserialize(std::istream& in);

    std::unique_ptr<Field> clone() const;
    void print() const;
};

bool operator==(const Field& lhs, const Field& rhs);

#endif // BUZZDB_FIELD_H
