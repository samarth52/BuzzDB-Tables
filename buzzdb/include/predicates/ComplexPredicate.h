#ifndef BUZZDB_COMPLEX_PREDICATE_H
#define BUZZDB_COMPLEX_PREDICATE_H

#include "IPredicate.h"
#include <vector>

class ComplexPredicate : public IPredicate {
public:
    enum LogicOperator { AND, OR };

    explicit ComplexPredicate(LogicOperator op);
    void addPredicate(std::unique_ptr<IPredicate> predicate);
    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const override;

private:
    std::vector<std::unique_ptr<IPredicate>> predicates;
    LogicOperator logic_operator;
};

#endif // BUZZDB_COMPLEX_PREDICATE_H
