#include <algorithm>
#include <iostream>
#include <vector>
#include <fstream>
#include <iostream>
#include <chrono>
#include <cassert>

#include <list>
#include <unordered_map>
#include <iostream>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <optional>
#include <regex>
#include <stdexcept>

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

public:
    Field(int i) : type(INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

   // Copy constructor
    Field(const Field& other) : type(other.type), data_length(other.data_length), data(new char[data_length]) {
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    // Move constructor - If you already have one, ensure it's correctly implemented
    Field(Field&& other) noexcept : type(other.type), data_length(other.data_length), data(std::move(other.data)) {
        // Optionally reset other's state if needed
    }

    FieldType getType() const { return type; }
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        return std::string(data.get());
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    // Clone method
    std::unique_ptr<Field> clone() const {
        // Use the copy constructor
        return std::make_unique<Field>(*this);
    }

    void print() const{
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false; // Different types are never equal

    switch (lhs.type) {
        case INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    // Clone method
    std::unique_ptr<Tuple> clone() const {
        auto clonedTuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            clonedTuple->addField(field->clone());
        }
        return clonedTuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                 // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        // std::cout << "Tuple size: " << tuple_size << " bytes\n";
        // assert(tuple_size == 38);

        // Check for first slot with enough space
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty && slot_array[slot_itr].length >= tuple_size) {
                break;
            }
        }
        if (slot_itr == MAX_SLOTS){
            std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;
        if (slot_array[slot_itr].offset == INVALID_VALUE){
            if(slot_itr != 0){
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            }
            else{
                offset = metadata_size;
            }

            slot_array[slot_itr].offset = offset;
        }
        else{
            offset = slot_array[slot_itr].offset;
        }

        if(offset + tuple_size >= PAGE_SIZE){
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        if (slot_array[slot_itr].length == INVALID_VALUE){
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized data into the page
        std::memcpy(page_data.get() + offset, 
                    serializedTuple.c_str(), 
                    tuple_size);

        return true;
    }

    bool updateTuple(size_t index, std::unique_ptr<Tuple> tuple) {
        assert(index < MAX_SLOTS);
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());

        auto serialized_tuple = tuple->serialize();
        auto tuple_size = serialized_tuple.length();
        if (!slot_array[index].empty && tuple_size <= slot_array[index].length) {
            // Copy serialized data into the page
            std::memcpy(page_data.get() + slot_array[index].offset, 
                    serialized_tuple.c_str(), 
                    tuple_size);
            return true;
        }

        bool add_tuple_res = addTuple(std::move(tuple));
        if (add_tuple_res) {
            deleteTuple(index);
        }
        return add_tuple_res;
    }

    void deleteTuple(size_t index) {
        assert(index < MAX_SLOTS);
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());

        if (!slot_array[index].empty){
            slot_array[index].empty = true;
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

template <typename T>
void printList(std::string list_name, const std::list<T>& myList) {
        std::cout << list_name << " :: ";
        for (const T& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

template <typename T>
class Policy {
public:
    virtual bool touch(T page_id) = 0;
    virtual T evict() = 0;
    virtual ~Policy() = default;
};

template <typename T>
class LruPolicy : public Policy<T> {
private:
    // List to keep track of the order of use
    std::list<T> lruList;

    // Default invalid value
    const T invalid_value;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<T, typename std::list<T>::iterator> map;

    size_t cacheSize;

public:

    LruPolicy(size_t cacheSize, T invalid_value) : cacheSize(cacheSize), invalid_value(invalid_value) {}

    bool touch(T id) override {
        //printList("LRU", lruList);

        bool found = false;
        // If id already in the list, remove it
        if (map.find(id) != map.end()) {
            found = true;
            lruList.erase(map[id]);
            map.erase(id);            
        }

        // If cache is full, evict
        if(lruList.size() == cacheSize){
            evict();
        }

        if(lruList.size() < cacheSize){
            // Add the id to the front of the list
            lruList.emplace_front(id);
            map[id] = lruList.begin();
        }

        return found;
    }

    T evict() override {
        // Evict the least recently used id
        T evictedId = invalid_value;
        if(lruList.size() != 0){
            evictedId = lruList.back();
            map.erase(evictedId);
            lruList.pop_back();
        }
        return evictedId;
    }
};

using TableID = uint16_t;
using PageID = uint16_t;
constexpr size_t MAX_FILE_STREAMS_IN_MEMORY = 2 << 10;

class StorageManager {
private:
    std::unordered_map<TableID, std::fstream> file_stream_map;
    std::unordered_map<TableID, size_t> num_pages_map;
    std::unique_ptr<LruPolicy<TableID>> policy;

private:
    std::string get_database_filename(TableID table_id) {
        return "database_files/" + std::to_string(table_id) + ".dat";
    }

    std::fstream& open_stream(TableID table_id) {
        std::fstream file_stream;
        std::string database_filename = get_database_filename(table_id);
        file_stream.open(database_filename, std::ios::in | std::ios::out);
        if (!file_stream) {
            // If file does not exist, create it
            file_stream.clear(); // Reset the state
            file_stream.open(database_filename, std::ios::out);
        }
        file_stream.close(); 
        file_stream.open(database_filename, std::ios::in | std::ios::out); 
        file_stream.seekg(0, std::ios::end);
        TableID num_pages = file_stream.tellg() / PAGE_SIZE;

        file_stream_map[table_id] = std::move(file_stream);
        num_pages_map[table_id] = num_pages;

        std::cout << "Storage Manager :: Table ID: " << table_id << " :: Num pages: " << num_pages << "\n";        
        if(num_pages == 0){
            extend(table_id);
        }
        return file_stream_map[table_id];
    }

    void close_stream(TableID table_id) {
        if (file_stream_map.find(table_id) != file_stream_map.end()) {
            if (file_stream_map[table_id].is_open()) {
                file_stream_map[table_id].close();
            }
            file_stream_map.erase(table_id);
        }
        if (num_pages_map.find(table_id) != num_pages_map.end()) {
            num_pages_map.erase(table_id);
        }
    }

    std::fstream& check_and_load_stream(TableID table_id) {
        if (file_stream_map.find(table_id) == file_stream_map.end()) {
            if (file_stream_map.size() >= MAX_FILE_STREAMS_IN_MEMORY) {
                TableID evicted_table_id = policy->evict();
                if (evicted_table_id != INVALID_VALUE) {
                    close_stream(evicted_table_id);
                }
            }
            open_stream(table_id);
        }
        policy->touch(table_id);
        return file_stream_map[table_id];
    }

public:
    StorageManager() : policy(std::make_unique<LruPolicy<TableID>>(MAX_FILE_STREAMS_IN_MEMORY, INVALID_VALUE)) {
    }

    ~StorageManager() {
        for (auto it = file_stream_map.begin(); it != file_stream_map.end(); it++) {
            if (it->second.is_open()) {
                it->second.close();
            }
        }
        file_stream_map.clear();
        num_pages_map.clear();
    }

    bool file_exists(TableID table_id) {
        std::string database_filename = get_database_filename(table_id);
        std::fstream file_stream(database_filename);

        bool res = file_stream.good();
        file_stream.close();
        return res;
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(TableID table_id, PageID page_id) {
        std::fstream& file_stream = check_and_load_stream(table_id);
        file_stream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(file_stream.read(page->page_data.get(), PAGE_SIZE)) {
            //std::cout << "Page read successfully from file." << std::endl;
        } else {
            std::cerr << "Error: Unable to read data from the file :: Table ID: " << table_id << ". \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(TableID table_id, PageID page_id, const std::unique_ptr<SlottedPage>& page) {
        std::fstream& file_stream = check_and_load_stream(table_id);
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        file_stream.seekp(page_offset, std::ios::beg);
        file_stream.write(page->page_data.get(), PAGE_SIZE);        
        file_stream.flush();
    }

    // Extend database file by one page
    void extend(TableID table_id) {
        std::cout << "Extending database file :: Table ID: " << table_id << "\n";
        std::fstream& file_stream = check_and_load_stream(table_id);

        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Move the write pointer
        file_stream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        file_stream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        file_stream.flush();

        // Update number of pages
        num_pages_map[table_id] += 1;
    }

    size_t get_num_pages(TableID table_id) {
        check_and_load_stream(table_id);
        return num_pages_map[table_id];
    }

};

struct TablePageIDs {
    TableID table_id;
    PageID page_id;

    TablePageIDs(TableID table_id, PageID page_id): table_id(table_id), page_id(page_id) {};
    bool operator==(const TablePageIDs& other) const {
        return table_id == other.table_id && page_id == other.page_id;
    }
};
namespace std {
    template <>
    struct hash<TablePageIDs> {
        std::size_t operator()(const TablePageIDs& tp_ids) const {
            std::size_t seed = 0;
            std::hash<size_t> hasher;
            seed ^= hasher(tp_ids.table_id) + 0x9e3779b9 + (seed<<6) + (seed>>2);
            seed ^= hasher(tp_ids.page_id) + 0x9e3779b9 + (seed<<6) + (seed>>2);
            return seed;
        }
    };
}

constexpr size_t MAX_PAGES_IN_MEMORY = 2 << 16;
class BufferManager {
private:

    using PageMap = std::unordered_map<TablePageIDs, std::unique_ptr<SlottedPage>>;
    StorageManager storage_manager;
    PageMap pageMap;
    std::unique_ptr<Policy<TablePageIDs>> policy;

public:
    BufferManager(): 
    policy(std::make_unique<LruPolicy<TablePageIDs>>(MAX_PAGES_IN_MEMORY, TablePageIDs(INVALID_VALUE, INVALID_VALUE))) {}

    bool fileExists(TableID table_id) {
        return storage_manager.file_exists(table_id);
    }

    std::unique_ptr<SlottedPage>& getPage(TableID table_id, PageID page_id) {
        TablePageIDs tp_ids(table_id, page_id);

        auto it = pageMap.find(tp_ids);
        if (it != pageMap.end()) {
            policy->touch(tp_ids);
            return pageMap.find(tp_ids)->second;
        }

        if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
            auto evicted_tp_ids = policy->evict();
            if(evicted_tp_ids.table_id != INVALID_VALUE){
                std::cout << "Evicting Table ID: " << evicted_tp_ids.table_id << " :: Page ID: " << evicted_tp_ids.page_id << "\n";
                storage_manager.flush(evicted_tp_ids.table_id, evicted_tp_ids.page_id, pageMap[evicted_tp_ids]);
                pageMap[evicted_tp_ids].reset();
                pageMap.erase(evicted_tp_ids);
            }
        }

        auto page = storage_manager.load(table_id, page_id);
        policy->touch(tp_ids);
        std::cout << "Loading Table ID: " << table_id << " :: Page ID: " << page_id <<  "\n";
        pageMap[tp_ids] = std::move(page);
        return pageMap[tp_ids];
    }

    void flushPage(TableID table_id, PageID page_id) {
        //std::cout << "Flush page " << page_id << "\n";
        storage_manager.flush(table_id, page_id, pageMap[TablePageIDs(table_id, page_id)]);
    }

    void extend(TableID table_id) {
        storage_manager.extend(table_id);
    }
    
    size_t getNumPages(TableID table_id) {
        return storage_manager.get_num_pages(table_id);
    }

    // std::unique_ptr<SlottedPage>& getPage(PageID page_id) {
    //     return getPage(0, page_id);
    // }
    // void flushPage(PageID page_id) {
    //     flushPage(0, page_id);
    // }
    // void extend() {
    //     extend(0);
    // }
    // size_t getNumPages() {
    //     return getNumPages(0);
    // }
};

class HashIndex {
private:
    struct HashEntry {
        int key;
        int value;
        int position; // Final position within the array
        bool exists; // Flag to check if entry exists

        // Default constructor
        HashEntry() : key(0), value(0), position(-1), exists(false) {}

        // Constructor for initializing with key, value, and exists flag
        HashEntry(int k, int v, int pos) : key(k), value(v), position(pos), exists(true) {}    
    };

    static const size_t capacity = 100; // Hard-coded capacity
    HashEntry hashTable[capacity]; // Static-sized array

    size_t hashFunction(int key) const {
        return key % capacity; // Simple modulo hash function
    }

public:
    HashIndex() {
        // Initialize all entries as non-existing by default
        for (size_t i = 0; i < capacity; ++i) {
            hashTable[i] = HashEntry();
        }
    }

    void insertOrUpdate(int key, int value) {
        size_t index = hashFunction(key);
        size_t originalIndex = index;
        bool inserted = false;
        int i = 0; // Attempt counter

        do {
            if (!hashTable[index].exists) {
                hashTable[index] = HashEntry(key, value, true);
                hashTable[index].position = index;
                inserted = true;
                break;
            } else if (hashTable[index].key == key) {
                hashTable[index].value += value;
                hashTable[index].position = index;
                inserted = true;
                break;
            }
            i++;
            index = (originalIndex + i*i) % capacity; // Quadratic probing
        } while (index != originalIndex && !inserted);

        if (!inserted) {
            std::cerr << "HashTable is full or cannot insert key: " << key << std::endl;
        }
    }

   int getValue(int key) const {
        size_t index = hashFunction(key);
        size_t originalIndex = index;

        do {
            if (hashTable[index].exists && hashTable[index].key == key) {
                return hashTable[index].value;
            }
            if (!hashTable[index].exists) {
                break; // Stop if we find a slot that has never been used
            }
            index = (index + 1) % capacity;
        } while (index != originalIndex);

        return -1; // Key not found
    }

    // This method is not efficient for range queries 
    // as this is an unordered index
    // but is included for comparison
    std::vector<int> rangeQuery(int lowerBound, int upperBound) const {
        std::vector<int> values;
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists && hashTable[i].key >= lowerBound && hashTable[i].key <= upperBound) {
                std::cout << "Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
                values.push_back(hashTable[i].value);
            }
        }
        return values;
    }

    void print() const {
        for (size_t i = 0; i < capacity; ++i) {
            if (hashTable[i].exists) {
                std::cout << "Position: " << hashTable[i].position << 
                ", Key: " << hashTable[i].key << 
                ", Value: " << hashTable[i].value << std::endl;
            }
        }
    }
};

class Operator {
    public:
    virtual ~Operator() = default;

    /// Initializes the operator.
    virtual void open() = 0;

    /// Tries to generate the next tuple. Return true when a new tuple is
    /// available.
    virtual bool next() = 0;

    /// Destroys the operator.
    virtual void close() = 0;

    /// This returns the pointers to the Fields of the generated tuple. When
    /// `next()` returns true, the Fields will contain the values for the
    /// next tuple. Each `Field` pointer in the vector stands for one attribute of the tuple.
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

class ScanOperator : public Operator {
private:
    BufferManager& bufferManager;
    TableID table_id;
    size_t currentPageIndex = 0;
    size_t currentSlotIndex = 0;
    std::unique_ptr<Tuple> currentTuple;
    size_t tuple_count = 0;

public:
    ScanOperator(BufferManager& manager, TableID table_id) : bufferManager(manager), table_id(table_id) {}

    void open() override {
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset(); // Ensure currentTuple is reset
        tuple_count = 0;
        // loadNextTuple();
    }

    bool next() override {
        // if (!currentTuple) return false; // No more tuples available

        loadNextTuple();
        return currentTuple != nullptr;
    }

    void close() override {
        std::cout << "Scan Operator tuple_count: " << tuple_count << "\n";
        currentPageIndex = 0;
        currentSlotIndex = 0;
        currentTuple.reset();
        tuple_count = 0;
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (currentTuple) {
            return std::move(currentTuple->fields);
        }
        return {}; // Return an empty vector if no tuple is available
    }

private:
    void loadNextTuple() {
        while (currentPageIndex < bufferManager.getNumPages(table_id)) {
            auto& currentPage = bufferManager.getPage(table_id, currentPageIndex);
            if (!currentPage || currentSlotIndex >= MAX_SLOTS) {
                currentSlotIndex = 0; // Reset slot index when moving to a new page
            }

            char* page_buffer = currentPage->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);

            while (currentSlotIndex < MAX_SLOTS) {
                if (!slot_array[currentSlotIndex].empty) {
                    assert(slot_array[currentSlotIndex].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[currentSlotIndex].offset;
                    std::istringstream iss(std::string(tuple_data, slot_array[currentSlotIndex].length));
                    currentTuple = Tuple::deserialize(iss);
                    currentSlotIndex++; // Move to the next slot for the next call
                    tuple_count++;
                    return; // Tuple loaded successfully
                }
                currentSlotIndex++;
            }

            // Increment page index after exhausting current page
            currentPageIndex++;
        }

        // No more tuples are available
        currentTuple.reset();
    }
};

class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const = 0;
};

void printTuple(const std::vector<std::unique_ptr<Field>>& tupleFields) {
    std::cout << "Tuple: [";
    for (const auto& field : tupleFields) {
        field->print(); // Assuming `print()` is a method that prints field content
        std::cout << " ";
    }
    std::cout << "]";
}

class SimplePredicate: public IPredicate {
public:
    enum OperandType { DIRECT, INDIRECT };
    enum ComparisonOperator { EQ, NE, GT, GE, LT, LE }; // Renamed from PredicateType

    struct Operand {
        std::unique_ptr<Field> directValue;
        size_t index;
        OperandType type;

        Operand(std::unique_ptr<Field> value) : directValue(std::move(value)), type(DIRECT) {}
        Operand(size_t idx) : index(idx), type(INDIRECT) {}
    };

    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand(std::move(left)), right_operand(std::move(right)), comparison_operator(op) {}

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        const Field* leftField = nullptr;
        const Field* rightField = nullptr;

        if (left_operand.type == DIRECT) {
            leftField = left_operand.directValue.get();
        } else if (left_operand.type == INDIRECT) {
            leftField = tupleFields[left_operand.index].get();
        }

        if (right_operand.type == DIRECT) {
            rightField = right_operand.directValue.get();
        } else if (right_operand.type == INDIRECT) {
            rightField = tupleFields[right_operand.index].get();
        }

        if (leftField == nullptr || rightField == nullptr) {
            std::cerr << "Error: Invalid field reference.\n";
            return false;
        }

        if (leftField->getType() != rightField->getType()) {
            std::cerr << "Error: Comparing fields of different types.\n";
            return false;
        }

        // Perform comparison based on field type
        switch (leftField->getType()) {
            case FieldType::INT: {
                int left_val = leftField->asInt();
                int right_val = rightField->asInt();
                return compare(left_val, right_val);
            }
            case FieldType::FLOAT: {
                float left_val = leftField->asFloat();
                float right_val = rightField->asFloat();
                return compare(left_val, right_val);
            }
            case FieldType::STRING: {
                std::string left_val = leftField->asString();
                std::string right_val = rightField->asString();
                return compare(left_val, right_val);
            }
            default:
                std::cerr << "Invalid field type\n";
                return false;
        }
    }


private:

    // Compares two values of the same type
    template<typename T>
    bool compare(const T& left_val, const T& right_val) const {
        switch (comparison_operator) {
            case ComparisonOperator::EQ: return left_val == right_val;
            case ComparisonOperator::NE: return left_val != right_val;
            case ComparisonOperator::GT: return left_val > right_val;
            case ComparisonOperator::GE: return left_val >= right_val;
            case ComparisonOperator::LT: return left_val < right_val;
            case ComparisonOperator::LE: return left_val <= right_val;
            default: std::cerr << "Invalid predicate type\n"; return false;
        }
    }
};

class ComplexPredicate : public IPredicate {
public:
    enum LogicOperator { AND, OR };

private:
    std::vector<std::unique_ptr<IPredicate>> predicates;
    LogicOperator logic_operator;

public:
    ComplexPredicate(LogicOperator op) : logic_operator(op) {}

    void addPredicate(std::unique_ptr<IPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    bool check(const std::vector<std::unique_ptr<Field>>& tupleFields) const {
        
        if (logic_operator == AND) {
            for (const auto& pred : predicates) {
                if (!pred->check(tupleFields)) {
                    return false; // If any predicate fails, the AND condition fails
                }
            }
            return true; // All predicates passed
        } else if (logic_operator == OR) {
            for (const auto& pred : predicates) {
                if (pred->check(tupleFields)) {
                    return true; // If any predicate passes, the OR condition passes
                }
            }
            return false; // No predicates passed
        }
        return false;
    }


};


class SelectOperator : public UnaryOperator {
private:
    std::unique_ptr<IPredicate> predicate;
    bool has_next;
    std::vector<std::unique_ptr<Field>> currentOutput; // Store the current output here

public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(input), predicate(std::move(predicate)), has_next(false) {}

    void open() override {
        input->open();
        has_next = false;
        currentOutput.clear(); // Ensure currentOutput is cleared at the beginning
    }

    bool next() override {
        while (input->next()) {
            const auto& output = input->getOutput(); // Temporarily hold the output
            if (predicate->check(output)) {
                // If the predicate is satisfied, store the output in the member variable
                currentOutput.clear(); // Clear previous output
                for (const auto& field : output) {
                    // Assuming Field class has a clone method or copy constructor to duplicate fields
                    currentOutput.push_back(field->clone());
                }
                has_next = true;
                return true;
            }
        }
        has_next = false;
        currentOutput.clear(); // Clear output if no more tuples satisfy the predicate
        return false;
    }

    void close() override {
        input->close();
        currentOutput.clear(); // Ensure currentOutput is cleared at the end
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (has_next) {
            // Since currentOutput already holds the desired output, simply return it
            // Need to create a deep copy to return since we're returning by value
            std::vector<std::unique_ptr<Field>> outputCopy;
            for (const auto& field : currentOutput) {
                outputCopy.push_back(field->clone()); // Clone each field
            }
            return outputCopy;
        } else {
            return {}; // Return an empty vector if no matching tuple is found
        }
    }
};

enum class AggrFuncType { COUNT, MAX, MIN, SUM };

struct AggrFunc {
    AggrFuncType func;
    size_t attr_index; // Index of the attribute to aggregate
};

class HashAggregationOperator : public UnaryOperator {
private:
    std::vector<size_t> group_by_attrs;
    std::vector<AggrFunc> aggr_funcs;
    std::vector<Tuple> output_tuples; // Use your Tuple class for output
    size_t output_tuples_index = 0;

    struct FieldVectorHasher {
        std::size_t operator()(const std::vector<Field>& fields) const {
            std::size_t hash = 0;
            for (const auto& field : fields) {
                std::hash<std::string> hasher;
                std::size_t fieldHash = 0;

                // Depending on the type, hash the corresponding data
                switch (field.type) {
                    case INT: {
                        // Convert integer data to string and hash
                        int value = *reinterpret_cast<const int*>(field.data.get());
                        fieldHash = hasher(std::to_string(value));
                        break;
                    }
                    case FLOAT: {
                        // Convert float data to string and hash
                        float value = *reinterpret_cast<const float*>(field.data.get());
                        fieldHash = hasher(std::to_string(value));
                        break;
                    }
                    case STRING: {
                        // Directly hash the string data
                        std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                        fieldHash = hasher(value);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported field type for hashing.");
                }

                // Combine the hash of the current field with the hash so far
                hash ^= fieldHash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };


public:
    HashAggregationOperator(Operator& input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs)
        : UnaryOperator(input), group_by_attrs(group_by_attrs), aggr_funcs(aggr_funcs) {}

    void open() override {
        input->open(); // Ensure the input operator is opened
        output_tuples_index = 0;
        output_tuples.clear();

        // Assume a hash map to aggregate tuples based on group_by_attrs
        std::unordered_map<std::vector<Field>, std::vector<Field>, FieldVectorHasher> hash_table;

        while (input->next()) {
            const auto& tuple = input->getOutput(); // Assume getOutput returns a reference to the current tuple

            // Extract group keys and initialize aggregation values
            std::vector<Field> group_keys;
            for (auto& index : group_by_attrs) {
                group_keys.push_back(*tuple[index]); // Deep copy the Field object for group key
            }

            // Process aggregation functions
            if (!hash_table.count(group_keys)) {
                // Initialize aggregate values for a new group
                std::vector<Field> aggr_values(aggr_funcs.size(), Field(0)); // Assuming Field(int) initializes an integer Field
                hash_table[group_keys] = aggr_values;
            }

            // Update aggregate values
            auto& aggr_values = hash_table[group_keys];
            for (size_t i = 0; i < aggr_funcs.size(); ++i) {
                // Simplified update logic for demonstration
                // You'll need to implement actual aggregation logic here
                aggr_values[i] = updateAggregate(aggr_funcs[i], aggr_values[i], *tuple[aggr_funcs[i].attr_index]);
            }
        }

        // Prepare output tuples from the hash table
        for (const auto& entry : hash_table) {
            const auto& group_keys = entry.first;
            const auto& aggr_values = entry.second;
            Tuple output_tuple;
            // Assuming Tuple has a method to add Fields
            for (const auto& key : group_keys) {
                output_tuple.addField(std::make_unique<Field>(key)); // Add group keys to the tuple
            }
            for (const auto& value : aggr_values) {
                output_tuple.addField(std::make_unique<Field>(value)); // Add aggregated values to the tuple
            }
            output_tuples.push_back(std::move(output_tuple));
        }
    }

    bool next() override {
        if (output_tuples_index < output_tuples.size()) {
            output_tuples_index++;
            return true;
        }
        return false;
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        std::vector<std::unique_ptr<Field>> outputCopy;

        if (output_tuples_index == 0 || output_tuples_index > output_tuples.size()) {
            // If there is no current tuple because next() hasn't been called yet or we're past the last tuple,
            // return an empty vector.
            return outputCopy; // This will be an empty vector
        }

        // Assuming that output_tuples stores Tuple objects and each Tuple has a vector of Field objects or similar
        const auto& currentTuple = output_tuples[output_tuples_index - 1]; // Adjust for 0-based indexing after increment in next()

        // Assuming the Tuple class provides a way to access its fields, e.g., a method or a public member
        for (const auto& field : currentTuple.fields) {
            outputCopy.push_back(field->clone()); // Use the clone method to create a deep copy of each field
        }

        return outputCopy;
    }


private:

    Field updateAggregate(const AggrFunc& aggrFunc, const Field& currentAggr, const Field& newValue) {
        if (currentAggr.getType() != newValue.getType()) {
            throw std::runtime_error("Mismatched Field types in aggregation.");
        }

        switch (aggrFunc.func) {
            case AggrFuncType::COUNT: {
                if (currentAggr.getType() == FieldType::INT) {
                    // For COUNT, simply increment the integer value
                    int count = currentAggr.asInt() + 1;
                    return Field(count);
                }
                break;
            }
            case AggrFuncType::SUM: {
                if (currentAggr.getType() == FieldType::INT) {
                    int sum = currentAggr.asInt() + newValue.asInt();
                    return Field(sum);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float sum = currentAggr.asFloat() + newValue.asFloat();
                    return Field(sum);
                }
                break;
            }
            case AggrFuncType::MAX: {
                if (currentAggr.getType() == FieldType::INT) {
                    int max = std::max(currentAggr.asInt(), newValue.asInt());
                    return Field(max);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float max = std::max(currentAggr.asFloat(), newValue.asFloat());
                    return Field(max);
                }
                break;
            }
            case AggrFuncType::MIN: {
                if (currentAggr.getType() == FieldType::INT) {
                    int min = std::min(currentAggr.asInt(), newValue.asInt());
                    return Field(min);
                } else if (currentAggr.getType() == FieldType::FLOAT) {
                    float min = std::min(currentAggr.asFloat(), newValue.asFloat());
                    return Field(min);
                }
                break;
            }
            default:
                throw std::runtime_error("Unsupported aggregation function.");
        }

        // Default case for unsupported operations or types
        throw std::runtime_error(
            "Invalid operation or unsupported Field type.");
    }

};

class InsertOperator : public Operator {
private:
    BufferManager& bufferManager;
    TableID table_id;
    std::unique_ptr<Tuple> tupleToInsert;

public:
    InsertOperator(BufferManager& manager, TableID table_id) : bufferManager(manager), table_id(table_id) {}

    // Set the tuple to be inserted by this operator.
    void setTupleToInsert(std::unique_ptr<Tuple> tuple) {
        tupleToInsert = std::move(tuple);
    }

    void open() override {
        // Not used in this context
    }

    bool next() override {
        if (!tupleToInsert) return false; // No tuple to insert

        for (size_t pageId = 0; pageId < bufferManager.getNumPages(table_id); ++pageId) {
            auto& page = bufferManager.getPage(table_id, pageId);
            // Attempt to insert the tuple
            if (page->addTuple(tupleToInsert->clone())) { 
                // Flush the page to disk after insertion
                bufferManager.flushPage(table_id, pageId); 
                return true; // Insertion successful
            }
        }

        // If insertion failed in all existing pages, extend the database and try again
        bufferManager.extend(table_id);
        auto& newPage = bufferManager.getPage(table_id, bufferManager.getNumPages(table_id) - 1);
        if (newPage->addTuple(tupleToInsert->clone())) {
            bufferManager.flushPage(table_id, bufferManager.getNumPages(table_id) - 1);
            return true; // Insertion successful after extending the database
        }

        return false; // Insertion failed even after extending the database
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};

class DeleteOperator : public Operator {
private:
    BufferManager& bufferManager;
    TableID tableId;
    size_t pageId;
    size_t tupleId;

public:
    DeleteOperator(BufferManager& manager, TableID tableId, size_t pageId, size_t tupleId) 
        : bufferManager(manager), tableId(tableId), pageId(pageId), tupleId(tupleId) {}

    void open() override {
        // Not used in this context
    }

    bool next() override {
        auto& page = bufferManager.getPage(tableId, pageId);
        if (!page) {
            std::cerr << "Page not found." << std::endl;
            return false;
        }

        page->deleteTuple(tupleId); // Perform deletion
        bufferManager.flushPage(tableId, pageId); // Flush the page to disk after deletion
        return true;
    }

    void close() override {
        // Not used in this context
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return {}; // Return empty vector
    }
};

using ColumnID = uint16_t;
class TableColumn {
public:
    std::string name;
    ColumnID idx;
    FieldType type;

public:
    TableColumn(std::string name, ColumnID idx, FieldType type) : name(name), idx(idx), type(type) {}

    friend std::ostream& operator<<(std::ostream& os, TableColumn const& column) {
        return os << "Column idx=" << column.idx << " :: name=" << column.name << " :: type=" << column.type;
    }
};
using TableColumns = std::list<std::unique_ptr<TableColumn>>;

class TableSchema {
private:
    void init() {
        this->columns.sort([&](const std::unique_ptr<TableColumn>& column1, const std::unique_ptr<TableColumn>& column2) {
            return column1->idx < column2->idx;
        });
        for (auto it = this->columns.begin(); it != this->columns.end(); it++) {
            columns_map[it->get()->name] = it;
        }
    }

public:
    std::string name;
    TableID id;
    TableColumns columns;

    std::unordered_map<std::string, TableColumns::iterator> columns_map;

public:
    TableSchema(std::string name): name(name), id(std::numeric_limits<TableID>::max()) {}
    TableSchema(std::string name, TableID id): name(name), id(id) {}
    TableSchema(std::string name, TableColumns columns): name(name), id(std::numeric_limits<TableID>::max()), columns(std::move(columns)) {
        init();
    }
    TableSchema(std::string name, TableID id, TableColumns columns): name(name), id(id), columns(std::move(columns)) {
        init();
    }

    void add_column(std::unique_ptr<TableColumn> column) {
        std::string column_name = column->name;
        auto it = std::upper_bound(columns.begin(), columns.end(), column->idx, [&](ColumnID idx, const std::unique_ptr<TableColumn>& c) {
            return idx < c->idx;
        });
        auto new_it = columns.insert(it, std::move(column));
        columns_map[column_name] = new_it;
    }

    ColumnID find_column_idx(std::string name) {
        if (columns_map.find(name) == columns_map.end()) {
            return INVALID_VALUE;
        }
        return columns_map[name]->get()->idx;
    }

    friend std::ostream& operator<<(std::ostream& os, TableSchema const& table) {
        os << "Table id=" << table.id << " :: name=" << table.name << std::endl;
        os << "Columns:" << std::endl;
        for (auto& column : table.columns) {
            os << *column << std::endl;
        }
        return os;
    }
};

static const TableID SYSTEM_NEXT_TABLE_ID = 0;
static const TableID SYSTEM_CLASS_TABLE_ID = 1;
static const TableID SYSTEM_COLUMN_TABLE_ID = 2;

static const std::shared_ptr<TableSchema> SYSTEM_CLASS_SCHEMA = std::make_shared<TableSchema>("system_class", SYSTEM_CLASS_TABLE_ID,
    ([]{
        TableColumns columns;
        columns.push_back(std::make_unique<TableColumn>("id", 0, FieldType::INT));
        columns.push_back(std::make_unique<TableColumn>("name", 1, FieldType::STRING));
        return columns;
    })()
);
static const std::shared_ptr<TableSchema> SYSTEM_COLUMN_SCHEMA = std::make_shared<TableSchema>("system_column", SYSTEM_COLUMN_TABLE_ID,
    ([]{
        TableColumns columns;
        columns.push_back(std::make_unique<TableColumn>("table_id", 0, FieldType::INT));
        columns.push_back(std::make_unique<TableColumn>("name", 1, FieldType::STRING));
        columns.push_back(std::make_unique<TableColumn>("idx", 2, FieldType::INT));
        columns.push_back(std::make_unique<TableColumn>("data_type", 3, FieldType::INT));
        return columns;
    })()
);

constexpr size_t MAX_SCHEMAS_IN_MEMORY = 2 << 8;
constexpr size_t MAX_NAMES_IN_MEMORY = 2 << 12;
class TableManager {
private:
    BufferManager& buffer_manager;
    TableID next_table_id;

    std::unique_ptr<LruPolicy<TableID>> schema_policy;
    std::unordered_map<TableID, std::shared_ptr<TableSchema>> schema_map;

    std::unique_ptr<LruPolicy<std::string>> name_policy;
    std::unordered_map<std::string, TableID> name_map;

    static const TableID NUM_SYSTEM_TABLES = 3;

    void bootstrap_system_tables() {
        bootstrap_system_next_table_id();
        bootstrap_system_class_table();
        bootstrap_system_column_table();
    }

    void bootstrap_system_next_table_id() {
        auto& next_table_id_page = buffer_manager.getPage(SYSTEM_NEXT_TABLE_ID, 0);
        memset(next_table_id_page->page_data.get(), 0, PAGE_SIZE);
        next_table_id = 1;
    }

    void bootstrap_system_class_table() {
        create_table(SYSTEM_CLASS_SCHEMA, true);
    }

    void bootstrap_system_column_table() {
        create_table(SYSTEM_COLUMN_SCHEMA, true);
    }

public:
    TableManager(BufferManager& buffer_manager): buffer_manager(buffer_manager), 
            schema_policy(std::make_unique<LruPolicy<TableID>>(MAX_SCHEMAS_IN_MEMORY, INVALID_VALUE)),
            name_policy(std::make_unique<LruPolicy<std::string>>(MAX_NAMES_IN_MEMORY, ""))
    {
        schema_map[SYSTEM_CLASS_TABLE_ID] = SYSTEM_CLASS_SCHEMA;
        schema_map[SYSTEM_COLUMN_TABLE_ID] = SYSTEM_COLUMN_SCHEMA;

        // If the system tables do not exist, run the bootstrap process
        if (!buffer_manager.fileExists(SYSTEM_NEXT_TABLE_ID)) {
            bootstrap_system_tables();
            return;
        }

        auto& next_table_id_page = buffer_manager.getPage(SYSTEM_NEXT_TABLE_ID, 0);
        TableID potential_next_table_id = *reinterpret_cast<TableID*>(next_table_id_page->page_data.get());
        next_table_id = potential_next_table_id < NUM_SYSTEM_TABLES ? NUM_SYSTEM_TABLES : potential_next_table_id;
    }

    ~TableManager() {
        schema_map.clear();

        auto& next_table_id_page = buffer_manager.getPage(SYSTEM_NEXT_TABLE_ID, 0);
        TableID* next_table_id_ptr = reinterpret_cast<TableID*>(next_table_id_page->page_data.get());
        *next_table_id_ptr = next_table_id < NUM_SYSTEM_TABLES ? NUM_SYSTEM_TABLES : next_table_id;
    }

    TableID get_table_id(std::string name) {
        if (name_map.find(name) != name_map.end()) {
            std::cout << "Fetched table id from cache: id=" << name_map[name] << " :: name=" << name << std::endl;
            name_policy->touch(name);
            return name_map[name];
        }

        auto table_name_equality_predicate = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(SYSTEM_CLASS_SCHEMA->find_column_idx("name")),
            SimplePredicate::Operand(std::make_unique<Field>(name)),
            SimplePredicate::ComparisonOperator::EQ
        );
        ScanOperator scan_op(buffer_manager, SYSTEM_CLASS_TABLE_ID);
        SelectOperator select_op(scan_op, std::move(table_name_equality_predicate));
        while (select_op.next()) {
            auto tuple = select_op.getOutput();
            TableID table_id = tuple[SYSTEM_CLASS_SCHEMA->find_column_idx("id")]->asInt();

            if (name_map.size() >= MAX_NAMES_IN_MEMORY) {
                auto evicted_name = name_policy->evict();
                if (evicted_name != "") {
                    std::cout << "Evicted table id from cache: id=" << name_map[evicted_name] << " :: name=" << evicted_name << std::endl;
                    name_map.erase(evicted_name);
                }
            }
            name_policy->touch(name);
            name_map[name] = table_id;
            std::cout << "Fetched table id from disk: id=" << name_map[name] << " :: name=" << name << std::endl;
            return table_id;
        }

        return std::numeric_limits<TableID>::max();
    }

    std::shared_ptr<TableSchema> get_table_schema(std::string name) {
        TableID table_id;
        if ((table_id = get_table_id(name)) == std::numeric_limits<TableID>::max()) {
            std::cerr << "Failed to fetch table schema from disk: name=" << name << " :: Table does not exist." << std::endl;
            return nullptr;
        }
        return get_table_schema(table_id);
    }

    std::shared_ptr<TableSchema> get_table_schema(TableID table_id) {
        if (schema_map.find(table_id) != schema_map.end()) {
            std::cout << "Fetched table schema from cache: id=" << table_id << std::endl;
            auto table_schema = schema_map[table_id];
            if (table_id >= NUM_SYSTEM_TABLES) {
                schema_policy->touch(table_id);
            }
            return table_schema;
        }

        // Traverse through system_class and get all metadata for given table_id
        std::shared_ptr<TableSchema> table_schema = nullptr;
        {
            auto table_id_equality_predicate = std::make_unique<SimplePredicate>(
                SimplePredicate::Operand(SYSTEM_CLASS_SCHEMA->find_column_idx("id")),
                SimplePredicate::Operand(std::make_unique<Field>(table_id)),
                SimplePredicate::ComparisonOperator::EQ
            );
            ScanOperator scan_op(buffer_manager, SYSTEM_CLASS_TABLE_ID);
            SelectOperator select_op(scan_op, std::move(table_id_equality_predicate));
            while (select_op.next()) {
                auto tuple = select_op.getOutput();
                std::string name = tuple[SYSTEM_CLASS_SCHEMA->find_column_idx("name")]->asString();
                table_schema = std::make_shared<TableSchema>(name, table_id);
            }
        }

        if (table_schema == nullptr) {
            std::cerr << "Failed to fetch table schema from disk: id=" << table_id << " :: Table does not exist." << std::endl;
            return nullptr;
        }

        // Traverse through system_column and get all column metadata for given table_id
        {
            auto table_id_equality_predicate = std::make_unique<SimplePredicate>(
                SimplePredicate::Operand(SYSTEM_COLUMN_SCHEMA->find_column_idx("table_id")),
                SimplePredicate::Operand(std::make_unique<Field>(table_id)),
                SimplePredicate::ComparisonOperator::EQ
            );
            ScanOperator scan_op(buffer_manager, SYSTEM_COLUMN_TABLE_ID);
            SelectOperator select_op(scan_op, std::move(table_id_equality_predicate));
            while (select_op.next()) {
                auto tuple = select_op.getOutput();
                std::unique_ptr<TableColumn> column = std::make_unique<TableColumn>(
                    tuple[SYSTEM_COLUMN_SCHEMA->find_column_idx("name")]->asString(),
                    tuple[SYSTEM_COLUMN_SCHEMA->find_column_idx("idx")]->asInt(),
                    FieldType(tuple[SYSTEM_COLUMN_SCHEMA->find_column_idx("data_type")]->asInt())
                );
                table_schema->add_column(std::move(column));
            }
        }

        if (schema_map.size() >= MAX_SCHEMAS_IN_MEMORY) {
            TableID evicted_table_id = schema_policy->evict();
            if (evicted_table_id != INVALID_VALUE) {
                std::cout << "Evicted table id from cache: id=" << evicted_table_id << std::endl;
                schema_map.erase(evicted_table_id);
            }
        }
        schema_map[table_id] = table_schema;
        schema_policy->touch(table_id);
        std::cout << "Fetched table schema from disk: id=" << table_id << std::endl;
        return table_schema;
    }

    bool create_table(std::shared_ptr<TableSchema> table_schema, bool is_bootstrap) {
        if (!is_bootstrap) {
            // Check if table exists
            auto table_name_equality_predicate = std::make_unique<SimplePredicate>(
                SimplePredicate::Operand(SYSTEM_CLASS_SCHEMA->find_column_idx("name")),
                SimplePredicate::Operand(std::make_unique<Field>(table_schema->name)),
                SimplePredicate::ComparisonOperator::EQ
            );

            ScanOperator scan_op(buffer_manager, SYSTEM_CLASS_TABLE_ID);
            SelectOperator select_op(scan_op, std::move(table_name_equality_predicate));
            while (select_op.next()) {
                std::cerr << "Table " << table_schema->name << " already exists in the database" << std::endl;
                return false;
            }
        }

        TableID table_id = next_table_id++;

        // Insert table id and name in system_class
        InsertOperator insert_system_class_op(buffer_manager, SYSTEM_CLASS_TABLE_ID);
        std::unique_ptr<Tuple> system_class_tuple = std::make_unique<Tuple>();
        system_class_tuple->addField(std::make_unique<Field>(table_id));
        system_class_tuple->addField(std::make_unique<Field>(table_schema->name));
        insert_system_class_op.setTupleToInsert(std::move(system_class_tuple));
        bool insert_system_class_res = insert_system_class_op.next();
        assert(insert_system_class_res == true);

        // Insert table columns in system_column
        InsertOperator insert_system_column_op(buffer_manager, SYSTEM_COLUMN_TABLE_ID);
        for (auto& column : table_schema->columns) {
            std::unique_ptr<Tuple> system_column_tuple = std::make_unique<Tuple>();
            system_column_tuple->addField(std::make_unique<Field>(table_id));
            system_column_tuple->addField(std::make_unique<Field>(column->name));
            system_column_tuple->addField(std::make_unique<Field>(column->idx));
            system_column_tuple->addField(std::make_unique<Field>(column->type));
            insert_system_column_op.setTupleToInsert(std::move(system_column_tuple));
            bool insert_system_column_res = insert_system_column_op.next();
            assert(insert_system_column_res == true);
        }
        table_schema->id = table_id;

        return true;
    }
};

struct QueryComponents {
    std::vector<int> selectAttributes;
    TableID tableId;
    bool sumOperation = false;
    int sumAttributeIndex = -1;
    bool groupBy = false;
    int groupByAttributeIndex = -1;
    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();
};

QueryComponents parseQuery(TableManager& table_manager, const std::string& query) {
    QueryComponents components;

    // Parse selected attributes
    std::regex selectRegex("\\{(\\d+)\\}(, \\{(\\d+)\\})?");
    std::smatch selectMatches;
    std::string::const_iterator queryStart(query.cbegin());
    while (std::regex_search(queryStart, query.cend(), selectMatches, selectRegex)) {
        for (size_t i = 1; i < selectMatches.size(); i += 2) {
            if (!selectMatches[i].str().empty()) {
                components.selectAttributes.push_back(std::stoi(selectMatches[i]) - 1);
            }
        }
        queryStart = selectMatches.suffix().first;
    }

    // Parse table name
    std::regex tableRegex("FROM (\\w+)");
    std::smatch tableMatch;
    if (std::regex_search(query, tableMatch, tableRegex)) {
        std::cout << "Table Name=" << tableMatch[1] << std::endl;
        components.tableId = table_manager.get_table_id(tableMatch[1]);
        std::cout << "Table Id=" << components.tableId << std::endl;
    } else {
        throw std::invalid_argument("Could not find the table name in query");
    }

    // Check for SUM operation
    std::regex sumRegex("SUM\\{(\\d+)\\}");
    std::smatch sumMatches;
    if (std::regex_search(query, sumMatches, sumRegex)) {
        components.sumOperation = true;
        components.sumAttributeIndex = std::stoi(sumMatches[1]) - 1;
    }

    // Check for GROUP BY clause
    std::regex groupByRegex("GROUP BY \\{(\\d+)\\}");
    std::smatch groupByMatches;
    if (std::regex_search(query, groupByMatches, groupByRegex)) {
        components.groupBy = true;
        components.groupByAttributeIndex = std::stoi(groupByMatches[1]) - 1;
    }

    // Extract WHERE conditions more accurately
    std::regex whereRegex("\\{(\\d+)\\} > (\\d+) and \\{(\\d+)\\} < (\\d+)");
    std::smatch whereMatches;
    if (std::regex_search(query, whereMatches, whereRegex)) {
        components.whereCondition = true;
        // Correctly identify the attribute index for the WHERE condition
        components.whereAttributeIndex = std::stoi(whereMatches[1]) - 1;
        components.lowerBound = std::stoi(whereMatches[2]);
        // Ensure the same attribute is used for both conditions
        if (std::stoi(whereMatches[3]) - 1 == components.whereAttributeIndex) {
            components.upperBound = std::stoi(whereMatches[4]);
        } else {
            std::cerr << "Error: WHERE clause conditions apply to different attributes." << std::endl;
            // Handle error or set components.whereCondition = false;
        }
    }

    return components;
}

void prettyPrint(const QueryComponents& components) {
    std::cout << "Query Components:\n";
    std::cout << "  Selected Attributes: ";
    for (auto attr : components.selectAttributes) {
        std::cout << "{" << attr + 1 << "} "; // Convert back to 1-based indexing for display
    }
    std::cout << "\n  SUM Operation: " << (components.sumOperation ? "Yes" : "No");
    if (components.sumOperation) {
        std::cout << " on {" << components.sumAttributeIndex + 1 << "}";
    }
    std::cout << "\n  GROUP BY: " << (components.groupBy ? "Yes" : "No");
    if (components.groupBy) {
        std::cout << " on {" << components.groupByAttributeIndex + 1 << "}";
    }
    std::cout << "\n  WHERE Condition: " << (components.whereCondition ? "Yes" : "No");
    if (components.whereCondition) {
        std::cout << " on {" << components.whereAttributeIndex + 1 << "} > " << components.lowerBound << " and < " << components.upperBound;
    }
    std::cout << std::endl;
}

void executeQuery(const QueryComponents& components, 
                  BufferManager& buffer_manager) {
    // Stack allocation of ScanOperator
    ScanOperator scanOp(buffer_manager, components.tableId);

    // Using a pointer to Operator to handle polymorphism
    Operator* rootOp = &scanOp;

    // Buffer for optional operators to ensure lifetime
    std::optional<SelectOperator> selectOpBuffer;
    std::optional<HashAggregationOperator> hashAggOpBuffer;

    // Apply WHERE conditions
    if (components.whereAttributeIndex != -1) {
        // Create simple predicates with comparison operators
        auto predicate1 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.lowerBound)),
            SimplePredicate::ComparisonOperator::GT
        );

        auto predicate2 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(components.whereAttributeIndex),
            SimplePredicate::Operand(std::make_unique<Field>(components.upperBound)),
            SimplePredicate::ComparisonOperator::LT
        );

        // Combine simple predicates into a complex predicate with logical AND operator
        auto complexPredicate = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
        complexPredicate->addPredicate(std::move(predicate1));
        complexPredicate->addPredicate(std::move(predicate2));

        // Using std::optional to manage the lifetime of SelectOperator
        selectOpBuffer.emplace(*rootOp, std::move(complexPredicate));
        rootOp = &*selectOpBuffer;
    }

    // Apply SUM or GROUP BY operation
    if (components.sumOperation || components.groupBy) {
        std::vector<size_t> groupByAttrs;
        if (components.groupBy) {
            groupByAttrs.push_back(static_cast<size_t>(components.groupByAttributeIndex));
        }
        std::vector<AggrFunc> aggrFuncs{
            {AggrFuncType::SUM, static_cast<size_t>(components.sumAttributeIndex)}
        };

        // Using std::optional to manage the lifetime of HashAggregationOperator
        hashAggOpBuffer.emplace(*rootOp, groupByAttrs, aggrFuncs);
        rootOp = &*hashAggOpBuffer;
    }

    // Execute the Root Operator
    rootOp->open();
    while (rootOp->next()) {
        // Retrieve and print the current tuple
        const auto& output = rootOp->getOutput();
        for (const auto& field : output) {
            field->print();
            std::cout << " ";
        }
        std::cout << std::endl;
    }
    rootOp->close();
    std::cout << std::endl;
}

class BuzzDB {
public:
    HashIndex hash_index;
    BufferManager buffer_manager;
    TableManager table_manager;

public:
    size_t max_number_of_tuples = 5000;
    size_t tuple_insertion_attempt_counter = 0;

    BuzzDB(): table_manager(buffer_manager) {
        // Storage Manager automatically created
    }

    // insert function
    void insert(int key, int value) {
        tuple_insertion_attempt_counter += 1;

        // Create a new tuple with the given key and value
        auto newTuple = std::make_unique<Tuple>();

        auto key_field = std::make_unique<Field>(key);
        auto value_field = std::make_unique<Field>(value);
        float float_val = 132.04;
        auto float_field = std::make_unique<Field>(float_val);
        auto string_field = std::make_unique<Field>("buzzdb");

        newTuple->addField(std::move(key_field));
        newTuple->addField(std::move(value_field));
        newTuple->addField(std::move(float_field));
        newTuple->addField(std::move(string_field));

        InsertOperator insertOp(buffer_manager, 10);
        insertOp.setTupleToInsert(std::move(newTuple));
        bool status = insertOp.next();

        assert(status == true);

        // if (tuple_insertion_attempt_counter % 10 != 0) {
        //     // Assuming you want to delete the first tuple from the first page
        //     DeleteOperator delOp(buffer_manager, 0, 0); 
        //     if (!delOp.next()) {
        //         std::cerr << "Failed to delete tuple." << std::endl;
        //     }
        // }


    }

    void executeQueries() {

        std::vector<std::string> test_queries = {
            "SUM{1} FROM system_class GROUP BY {1} WHERE {1} > 2 and {1} < 6",
            "{0}, {1} FROM system_class",
            "{0}, {1}, {2}, {3} FROM system_column",
        };

        for (const auto& query : test_queries) {
            auto components = parseQuery(table_manager, query);
            //prettyPrint(components);
            executeQuery(components, buffer_manager);
        }

    }
    
};

int main() {

    BuzzDB db;

    std::ifstream inputFile("output.txt");

    if (!inputFile) {
        std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    auto schema1 = db.table_manager.get_table_schema(SYSTEM_CLASS_TABLE_ID);
    auto schema2 = db.table_manager.get_table_schema("system_column");
    std::cout << *schema1 << std::endl;
    std::cout << *schema2 << std::endl;


    std::shared_ptr<TableSchema> new_schema = std::make_shared<TableSchema>("test_table");
    new_schema->add_column(std::make_unique<TableColumn>("hello", 1, FieldType::INT));
    new_schema->add_column(std::make_unique<TableColumn>("there", 0, FieldType::STRING));
    new_schema->add_column(std::make_unique<TableColumn>("buddy", 2, FieldType::FLOAT));
    bool create_table_res = db.table_manager.create_table(new_schema, false);

    // assert(create_table_res == true);

    // std::cout << *new_schema << std::endl;

    auto schema3 = db.table_manager.get_table_schema("test_table");
    auto schema4 = db.table_manager.get_table_schema("test_table");
    // std::cout << "here5: " << /new_schema->find_column_idx("hello") << std::endl;
    // std::cout << "here6: " << schema3->find_column_idx("hello") << std::endl;

    std::cout << *schema3 << std::endl;
    std::cout << *schema4 << std::endl;

    int field1, field2;
    int i = 0;
    while (inputFile >> field1 >> field2) {
        db.insert(field1, field2);
    }

    auto start = std::chrono::high_resolution_clock::now();

    db.executeQueries();

    auto end = std::chrono::high_resolution_clock::now();

    // Calculate and print the elapsed time
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Elapsed time: " << 
    std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count() 
          << " microseconds" << std::endl;

    
    return 0;
}