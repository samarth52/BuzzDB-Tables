#ifndef BUZZDB_HASH_AGGREGATION_OPERATOR_H
#define BUZZDB_HASH_AGGREGATION_OPERATOR_H

#include "Operator.h"
#include "../core/Tuple.h"
#include <vector>
#include <unordered_map>

enum class AggrFuncType { COUNT, MAX, MIN, SUM };

struct AggrFunc {
    AggrFuncType func;
    size_t attr_index;
};

class HashAggregationOperator : public UnaryOperator {
public:
    HashAggregationOperator(Operator& input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::unique_ptr<Field>> getOutput() override;

private:
    std::vector<size_t> group_by_attrs;
    std::vector<AggrFunc> aggr_funcs;
    std::vector<Tuple> output_tuples;
    size_t output_tuples_index;

    Field updateAggregate(const AggrFunc& aggrFunc, const Field& currentAggr, const Field& newValue);
};

#endif // BUZZDB_HASH_AGGREGATION_OPERATOR_H
