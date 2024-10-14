#ifndef BUZZDB_BUFFER_MANAGER_H
#define BUZZDB_BUFFER_MANAGER_H

#include "StorageManager.h"
#include "SlottedPage.h"
#include <unordered_map>
#include <memory>

class Policy;

class BufferManager {
public:
    BufferManager();
    std::unique_ptr<SlottedPage>& getPage(int page_id);
    void flushPage(int page_id);
    void extend();
    size_t getNumPages() const;

private:
    StorageManager storage_manager;
    std::unordered_map<int, std::unique_ptr<SlottedPage>> pageMap;
    std::unique_ptr<Policy> policy;
    static const size_t MAX_PAGES_IN_MEMORY = 10;
};

#endif // BUZZDB_BUFFER_MANAGER_H
