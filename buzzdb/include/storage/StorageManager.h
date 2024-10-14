#ifndef BUZZDB_STORAGE_MANAGER_H
#define BUZZDB_STORAGE_MANAGER_H

#include "SlottedPage.h"
#include <fstream>
#include <string>

class StorageManager {
public:
    StorageManager();
    ~StorageManager();

    std::unique_ptr<SlottedPage> load(uint16_t page_id);
    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page);
    void extend();
    size_t getNumPages() const;

private:
    std::fstream fileStream;
    size_t num_pages;
    static const std::string database_filename;
};

#endif // BUZZDB_STORAGE_MANAGER_H
