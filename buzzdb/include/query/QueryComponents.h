#ifndef BUZZDB_QUERY_COMPONENTS_H
#define BUZZDB_QUERY_COMPONENTS_H

#include <vector>
#include <limits>

struct QueryComponents {
    std::vector<int> selectAttributes;
    bool sumOperation = false;
    int sumAttributeIndex = -1;
    bool groupBy = false;
    int groupByAttributeIndex = -1;
    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();
};

#endif // BUZZDB_QUERY_COMPONENTS_H
