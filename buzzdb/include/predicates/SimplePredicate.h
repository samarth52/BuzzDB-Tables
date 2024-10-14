#ifndef BUZZDB_SIMPLE_PREDICATE_H
#define BUZZDB_SIMPLE_PREDICATE_H

#include "IPredicate.h"

class SimplePredicate : public IPredicate {
public:
    enum OperandType { DIRECT, INDIRECT };
    enum ComparisonOperator { EQ, NE, GT, GE, LT, LE };

    struct Operand {
        std::unique_ptr<Field> directValue;
        size_t index;
        OperandType type;

        Operand(std::unique_ptr<Field> value);
        Operand(size_t idx);
    };

    SimplePredicate(Operand left, Operand right, ComparisonOperator op);
    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const override;

private:
    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    template<typename T>
    bool compare(const T& left_val, const T& right_val) const;
};

#endif // BUZZDB_SIMPLE_PREDICATE_H
