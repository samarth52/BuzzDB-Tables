#ifndef BUZZDB_SCAN_OPERATOR_H
#define BUZZDB_SCAN_OPERATOR_H

#include "Operator.h"
#include "../storage/BufferManager.h"
#include "../core/Tuple.h"

class ScanOperator : public Operator {
public:
    explicit ScanOperator(BufferManager& manager);
    void open() override;
    bool next() override;
    void close() override;
    std::vector<std::unique_ptr<Field>> getOutput() override;

private:
    BufferManager& bufferManager;
    size_t currentPageIndex;
    size_t currentSlotIndex;
    std::unique_ptr<Tuple> currentTuple;
    size_t tuple_count;

    void loadNextTuple();
};

#endif // BUZZDB_SCAN_OPERATOR_H
