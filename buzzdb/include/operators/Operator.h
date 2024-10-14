#ifndef BUZZDB_OPERATOR_H
#define BUZZDB_OPERATOR_H

#include "../core/Field.h"
#include <vector>
#include <memory>

class Operator {
public:
    virtual ~Operator() = default;
    virtual void open() = 0;
    virtual bool next() = 0;
    virtual void close() = 0;
    virtual std::vector<std::unique_ptr<Field>> getOutput() = 0;
};

class UnaryOperator : public Operator {
protected:
    Operator* input;

public:
    explicit UnaryOperator(Operator& input) : input(&input) {}
    ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
protected:
    Operator* input_left;
    Operator* input_right;

public:
    explicit BinaryOperator(Operator& input_left, Operator& input_right)
        : input_left(&input_left), input_right(&input_right) {}
    ~BinaryOperator() override = default;
};

#endif // BUZZDB_OPERATOR_H
