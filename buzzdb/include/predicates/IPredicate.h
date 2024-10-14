#ifndef BUZZDB_IPREDICATE_H
#define BUZZDB_IPREDICATE_H

#include "../core/Field.h"
#include <vector>
#include <memory>

class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const = 0;
};

#endif // BUZZDB_IPREDICATE_H
