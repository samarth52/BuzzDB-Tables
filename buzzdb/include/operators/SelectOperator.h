#ifndef BUZZDB_SELECT_OPERATOR_H
#define BUZZDB_SELECT_OPERATOR_H

#include "Operator.h"
#include "../predicates/IPredicate.h"

class SelectOperator : public UnaryOperator {
public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::unique_ptr<Field>> getOutput() override;

private:
    std::unique_ptr<IPredicate> predicate;
    bool has_next;
    std::vector<std::unique_ptr<Field>> currentOutput;
};

#endif // BUZZDB_SELECT_OPERATOR_H
