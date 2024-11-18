#include <algorithm>
#include <iostream>
#include <unordered_set>
#include <vector>
#include <fstream>
#include <iostream>
#include <chrono>
#include <cassert>
#include <format>
#include <cstring>
#include <filesystem>

#include <list>
#include <unordered_map>
#include <iostream>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <regex>
#include <stdexcept>

enum FieldType { NULLV, INT, FLOAT, STRING };

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

    Field() : type(NULLV) {
        data_length = 0;
        data = std::make_unique<char[]>(data_length);
        // std::string val = "NULL";
        // data_length = val.size() + 1;
        // data = std::make_unique<char[]>(data_length);
        // std::memcpy(data.get(), val.c_str(), data_length);
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

    FieldType get_type() const { return type; }
    int as_int() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float as_float() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string as_string() const { 
        return std::string(data.get());
    }
    std::nullptr_t as_null() const {
        return nullptr;
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
        std::string serialized_data = this->serialize();
        out << serialized_data;
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
        } else if (type == NULLV) {
            return std::make_unique<Field>();
        }
        return nullptr;
    }

    // Clone method
    std::unique_ptr<Field> clone() const {
        // Use the copy constructor
        return std::make_unique<Field>(*this);
    }

    void print(std::ostream& os) const{
        switch(get_type()){
            case INT: os << as_int(); break;
            case FLOAT: os << as_float(); break;
            case STRING: os << as_string(); break;
            case NULLV: os << "NULL"; break;
        }
    }

    void print() const{
        print(std::cout);
    }
};

bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.get_type() != rhs.get_type()) return false; // Different types are never equal

    switch (lhs.get_type()) {
        case INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) == *reinterpret_cast<const int*>(rhs.data.get());
        case FLOAT:
            return *reinterpret_cast<const float*>(lhs.data.get()) == *reinterpret_cast<const float*>(rhs.data.get());
        case STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) == std::string(rhs.data.get(), rhs.data_length - 1);
        case NULLV:
            return true;
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void add_field(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t get_size() const {
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
        std::string serialized_data = this->serialize();
        out << serialized_data;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t field_count; in >> field_count;
        for (size_t i = 0; i < field_count; ++i) {
            tuple->add_field(Field::deserialize(in));
        }
        return tuple;
    }

    // Clone method
    std::unique_ptr<Tuple> clone() const {
        auto cloned_tuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            cloned_tuple->add_field(field->clone());
        }
        return cloned_tuple;
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
    bool add_tuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serialized_tuple = tuple->serialize();
        size_t tuple_size = serialized_tuple.size();

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
                    serialized_tuple.c_str(), 
                    tuple_size);

        return true;
    }

    bool update_tuple(size_t index, std::unique_ptr<Tuple> tuple) {
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

        bool add_tuple_res = add_tuple(std::move(tuple));
        if (add_tuple_res) {
            delete_tuple(index);
        }
        return add_tuple_res;
    }

    void delete_tuple(size_t index) {
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
                auto loaded_tuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loaded_tuple->print();
            }
        }
        std::cout << "\n";
    }
};

template <typename T>
void print_list(std::string list_name, const std::list<T>& myList) {
        std::cout << list_name << " :: ";
        for (const T& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

template <typename T>
class Policy {
public:
    virtual bool touch(T id) = 0;
    virtual T evict() = 0;
    virtual bool erase(T id) = 0;
    virtual ~Policy() = default;
};

template <typename T>
class LruPolicy : public Policy<T> {
private:
    // List to keep track of the order of use
    std::list<T> lru_list;

    // Default invalid value
    const T invalid_value;

    // Map to find a page's iterator in the list efficiently
    std::unordered_map<T, typename std::list<T>::iterator> map;

    size_t cache_size;

public:

    LruPolicy(size_t cache_size, T invalid_value) : invalid_value(invalid_value), cache_size(cache_size) {}

    bool touch(T id) override {
        //print_list("LRU", lruList);

        bool found = false;
        // If id already in the list, remove it
        if (map.find(id) != map.end()) {
            found = true;
            lru_list.erase(map[id]);
            map.erase(id);            
        }

        // If cache is full, evict
        if(lru_list.size() == cache_size){
            evict();
        }

        if(lru_list.size() < cache_size){
            // Add the id to the front of the list
            lru_list.emplace_front(id);
            map[id] = lru_list.begin();
        }

        return found;
    }

    T evict() override {
        // Evict the least recently used id
        T evictedId = invalid_value;
        if(lru_list.size() != 0){
            evictedId = lru_list.back();
            map.erase(evictedId);
            lru_list.pop_back();
        }
        return evictedId;
    }

    bool erase(T id) override {
        if (map.find(id) != map.end()) {
            lru_list.erase(map[id]);
            map.erase(id);
            return true;
        }
        return false;
    }
};

using TableID = size_t;
using PageID = size_t;
using SlotID = size_t;
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

        // std::cout << "Storage Manager :: Table ID: " << table_id << " :: Num pages: " << num_pages << "\n";        
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

    bool delete_file(TableID table_id) {
        if (!file_exists(table_id)) {
            return true;
        }

        file_stream_map.erase(table_id);
        num_pages_map.erase(table_id);
        policy->erase(table_id);
        std::string database_filename = get_database_filename(table_id);
        return std::filesystem::remove(database_filename);
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
        // std::cout << "Extending database file :: Table ID: " << table_id << "\n";
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
    PageMap page_map;
    std::unique_ptr<Policy<TablePageIDs>> policy;

public:
    BufferManager(): 
    policy(std::make_unique<LruPolicy<TablePageIDs>>(MAX_PAGES_IN_MEMORY, TablePageIDs(INVALID_VALUE, INVALID_VALUE))) {}

    ~BufferManager() {
        for (auto& [table_page_id, _] : page_map) {
            auto& [table_id, page_id] = table_page_id;
            flush_page(table_id, page_id);
        }
        page_map.clear();
    }

    bool file_exists(TableID table_id) {
        return storage_manager.file_exists(table_id);
    }

    bool delete_file(TableID table_id) {
        std::vector<TablePageIDs> remove_keys;
        for (auto& [key, _] : page_map) {
            if (key.table_id == table_id) {
                remove_keys.push_back(key);
            }
        }

        for (auto& key : remove_keys) {
            page_map.erase(key);
            policy->erase(key);
        }
        return storage_manager.delete_file(table_id);
    }

    std::unique_ptr<SlottedPage>& get_page(TableID table_id, PageID page_id) {
        TablePageIDs tp_ids(table_id, page_id);

        auto it = page_map.find(tp_ids);
        if (it != page_map.end()) {
            policy->touch(tp_ids);
            return page_map.find(tp_ids)->second;
        }

        if (page_map.size() >= MAX_PAGES_IN_MEMORY) {
            auto evicted_tp_ids = policy->evict();
            if(evicted_tp_ids.table_id != INVALID_VALUE){
                // std::cout << "Evicting Table ID: " << evicted_tp_ids.table_id << " :: Page ID: " << evicted_tp_ids.page_id << "\n";
                storage_manager.flush(evicted_tp_ids.table_id, evicted_tp_ids.page_id, page_map[evicted_tp_ids]);
                page_map[evicted_tp_ids].reset();
                page_map.erase(evicted_tp_ids);
            }
        }

        auto page = storage_manager.load(table_id, page_id);
        policy->touch(tp_ids);
        // std::cout << "Loading Table ID: " << table_id << " :: Page ID: " << page_id <<  "\n";
        page_map[tp_ids] = std::move(page);
        return page_map[tp_ids];
    }

    void flush_page(TableID table_id, PageID page_id) {
        //std::cout << "Flush page " << page_id << "\n";
        storage_manager.flush(table_id, page_id, page_map[TablePageIDs(table_id, page_id)]);
    }

    void extend(TableID table_id) {
        storage_manager.extend(table_id);
    }
    
    size_t get_num_pages(TableID table_id) {
        return storage_manager.get_num_pages(table_id);
    }
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
    HashEntry hash_table[capacity]; // Static-sized array

    size_t hash_function(int key) const {
        return key % capacity; // Simple modulo hash function
    }

public:
    HashIndex() {
        // Initialize all entries as non-existing by default
        for (size_t i = 0; i < capacity; ++i) {
            hash_table[i] = HashEntry();
        }
    }

    void insert_or_update(int key, int value) {
        size_t index = hash_function(key);
        size_t originalIndex = index;
        bool inserted = false;
        int i = 0; // Attempt counter

        do {
            if (!hash_table[index].exists) {
                hash_table[index] = HashEntry(key, value, true);
                hash_table[index].position = index;
                inserted = true;
                break;
            } else if (hash_table[index].key == key) {
                hash_table[index].value += value;
                hash_table[index].position = index;
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

   int get_value(int key) const {
        size_t index = hash_function(key);
        size_t originalIndex = index;

        do {
            if (hash_table[index].exists && hash_table[index].key == key) {
                return hash_table[index].value;
            }
            if (!hash_table[index].exists) {
                break; // Stop if we find a slot that has never been used
            }
            index = (index + 1) % capacity;
        } while (index != originalIndex);

        return -1; // Key not found
    }

    // This method is not efficient for range queries 
    // as this is an unordered index
    // but is included for comparison
    std::vector<int> range_query(int lowerBound, int upperBound) const {
        std::vector<int> values;
        for (size_t i = 0; i < capacity; ++i) {
            if (hash_table[i].exists && hash_table[i].key >= lowerBound && hash_table[i].key <= upperBound) {
                std::cout << "Key: " << hash_table[i].key << 
                ", Value: " << hash_table[i].value << std::endl;
                values.push_back(hash_table[i].value);
            }
        }
        return values;
    }

    void print() const {
        for (size_t i = 0; i < capacity; ++i) {
            if (hash_table[i].exists) {
                std::cout << "Position: " << hash_table[i].position << 
                ", Key: " << hash_table[i].key << 
                ", Value: " << hash_table[i].value << std::endl;
            }
        }
    }
};

class Operator {
public:
    struct TupleMetadata {
        TableID table_id;
        PageID page_id;
        SlotID slot_id;

        TupleMetadata(): table_id(std::numeric_limits<TableID>::max()), page_id(0), slot_id(0) {}
        TupleMetadata(TableID table_id, PageID page_id, SlotID slot_id): table_id(table_id), page_id(page_id), slot_id(slot_id) {}
        TupleMetadata(const TupleMetadata& other): table_id(other.table_id), page_id(other.page_id), slot_id(other.slot_id) {}
        TupleMetadata& operator=(const TupleMetadata& other) {
            table_id = other.table_id;
            page_id = other.page_id;
            slot_id = other.slot_id;
            return *this;
        }
    };

    TupleMetadata tuple_metadata;
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
    virtual std::vector<std::unique_ptr<Field>> get_output() = 0;
};

class UnaryOperator : public Operator {
    protected:
    std::unique_ptr<Operator> input;

    public:
    explicit UnaryOperator(std::unique_ptr<Operator> input) : input(std::move(input)) {}

    ~UnaryOperator() override = default;
};

class BinaryOperator : public Operator {
    protected:
    std::unique_ptr<Operator> input_left;
    std::unique_ptr<Operator> input_right;

    public:
    explicit BinaryOperator(std::unique_ptr<Operator> input_left, std::unique_ptr<Operator> input_right)
        : input_left(std::move(input_left)), input_right(std::move(input_right)) {}

    ~BinaryOperator() override = default;
};

class ScanOperator : public Operator {
private:
    BufferManager& buffer_manager;
    TableID table_id;
    size_t current_page_index = 0;
    size_t current_slot_index = 0;
    std::unique_ptr<Tuple> current_tuple;
    size_t tuple_count = 0;

public:
    ScanOperator(BufferManager& manager, TableID table_id) : buffer_manager(manager), table_id(table_id) {}

    void open() override {
        current_page_index = 0;
        current_slot_index = 0;
        current_tuple.reset(); // Ensure current_tuple is reset
        tuple_count = 0;
        // load_next_tuple();
    }

    bool next() override {
        // if (!current_tuple) return false; // No more tuples available

        load_next_tuple();
        return current_tuple != nullptr;
    }

    void close() override {
        // std::cout << "Scan Operator tuple_count: " << tuple_count << "\n";
        current_page_index = 0;
        current_slot_index = 0;
        current_tuple.reset();
        tuple_count = 0;
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        if (current_tuple) {
            std::vector<std::unique_ptr<Field>> output_copy;
            for (auto& field : current_tuple->fields) {
                output_copy.push_back(field->clone());
            }
            return output_copy;
        }
        return {}; // Return an empty vector if no tuple is available
    }

private:
    void load_next_tuple() {
        while (current_page_index < buffer_manager.get_num_pages(table_id)) {
            auto& current_page = buffer_manager.get_page(table_id, current_page_index);
            if (!current_page || current_slot_index >= MAX_SLOTS) {
                current_slot_index = 0; // Reset slot index when moving to a new page
            }

            char* page_buffer = current_page->page_data.get();
            Slot* slot_array = reinterpret_cast<Slot*>(page_buffer);

            while (current_slot_index < MAX_SLOTS) {
                if (!slot_array[current_slot_index].empty) {
                    assert(slot_array[current_slot_index].offset != INVALID_VALUE);
                    const char* tuple_data = page_buffer + slot_array[current_slot_index].offset;
                    std::istringstream iss(std::string(tuple_data, slot_array[current_slot_index].length));
                    current_tuple = Tuple::deserialize(iss);
                    tuple_metadata = {table_id, current_page_index, current_slot_index};
                    current_slot_index++; // Move to the next slot for the next call
                    tuple_count++;
                    return; // Tuple loaded successfully
                }
                current_slot_index++;
            }

            // Increment page index after exhausting current page
            current_page_index++;
        }

        // No more tuples are available
        current_tuple.reset();
    }
};

class IPredicate {
public:
    virtual ~IPredicate() = default;
    virtual bool check(const std::vector<std::unique_ptr<Field>>& tuple_fields) const = 0;
};

void print_tuple(const std::vector<std::unique_ptr<Field>>& tuple_fields) {
    std::cout << "Tuple: [";
    for (const auto& field : tuple_fields) {
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
        std::unique_ptr<Field> direct_value;
        size_t index;
        OperandType type;

        Operand(std::unique_ptr<Field> value) : direct_value(std::move(value)), type(DIRECT) {}
        Operand(size_t idx) : index(idx), type(INDIRECT) {}
    };

    Operand left_operand;
    Operand right_operand;
    ComparisonOperator comparison_operator;

    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand(std::move(left)), right_operand(std::move(right)), comparison_operator(op) {}

    bool check(const std::vector<std::unique_ptr<Field>>& tuple_fields) const {
        const Field* left_field = nullptr;
        const Field* right_field = nullptr;

        if (left_operand.type == DIRECT) {
            left_field = left_operand.direct_value.get();
        } else if (left_operand.type == INDIRECT) {
            left_field = tuple_fields[left_operand.index].get();
        }

        if (right_operand.type == DIRECT) {
            right_field = right_operand.direct_value.get();
        } else if (right_operand.type == INDIRECT) {
            right_field = tuple_fields[right_operand.index].get();
        }

        if (left_field == nullptr || right_field == nullptr) {
            // std::cerr << "Error: Invalid field reference.\n";
            return false;
        }

        if (left_field->get_type() != right_field->get_type()) {
            // std::cerr << "Error: Comparing fields of different types.\n";
            return false;
        }

        // Perform comparison based on field type
        switch (left_field->get_type()) {
            case FieldType::INT: {
                int left_val = left_field->as_int();
                int right_val = right_field->as_int();
                return compare(left_val, right_val);
            }
            case FieldType::FLOAT: {
                float left_val = left_field->as_float();
                float right_val = right_field->as_float();
                return compare(left_val, right_val);
            }
            case FieldType::STRING: {
                std::string left_val = left_field->as_string();
                std::string right_val = right_field->as_string();
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

    void add_predicate(std::unique_ptr<IPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    bool check(const std::vector<std::unique_ptr<Field>>& tuple_fields) const {
        
        if (logic_operator == AND) {
            for (const auto& pred : predicates) {
                if (!pred->check(tuple_fields)) {
                    return false; // If any predicate fails, the AND condition fails
                }
            }
            return true; // All predicates passed
        } else if (logic_operator == OR) {
            for (const auto& pred : predicates) {
                if (pred->check(tuple_fields)) {
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
    std::vector<std::unique_ptr<Field>> current_output; // Store the current output here

public:
    SelectOperator(std::unique_ptr<Operator> input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(std::move(input)), predicate(std::move(predicate)), has_next(false) {}

    void open() override {
        input->open();
        has_next = false;
        current_output.clear(); // Ensure current_output is cleared at the beginning
    }

    bool next() override {
        while (input->next()) {
            const auto& output = input->get_output(); // Temporarily hold the output
            if (predicate->check(output)) {
                // If the predicate is satisfied, store the output in the member variable
                current_output.clear(); // Clear previous output
                for (const auto& field : output) {
                    // Assuming Field class has a clone method or copy constructor to duplicate fields
                    current_output.push_back(field->clone());
                }
                tuple_metadata = input->tuple_metadata;
                has_next = true;
                return true;
            }
        }
        has_next = false;
        current_output.clear(); // Clear output if no more tuples satisfy the predicate
        return false;
    }

    void close() override {
        input->close();
        current_output.clear(); // Ensure current_output is cleared at the end
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        if (has_next) {
            // Since current_output already holds the desired output, simply return it
            // Need to create a deep copy to return since we're returning by value
            std::vector<std::unique_ptr<Field>> output_copy;
            for (const auto& field : current_output) {
                output_copy.push_back(field->clone()); // Clone each field
            }
            return output_copy;
        } else {
            return {}; // Return an empty vector if no matching tuple is found
        }
    }
};

class ProjectOperator : public UnaryOperator {
    private:
        std::vector<size_t> attr_indexes;
        bool has_next;
        std::vector<std::unique_ptr<Field>> current_output;

    public:
        ProjectOperator(std::unique_ptr<Operator> input, std::vector<size_t> attr_indexes)
            : UnaryOperator(std::move(input)), attr_indexes(attr_indexes), has_next(false) {
            }

        ~ProjectOperator() = default;

        void open() override {
            input->open();
            has_next = false;
            current_output.clear();
        }

        bool next() override {
            if (input->next()) {
                const auto output = input->get_output();
                current_output.clear();
                for (const auto& ind: attr_indexes) {
                    current_output.push_back(output[ind]->clone());
                }
                tuple_metadata = input->tuple_metadata;
                has_next = true;
                return true;
            }
            has_next = false;
            current_output.clear();
            return false;
        }

        void close() override {
            input->close();
            has_next = false;
            current_output.clear();
        }

        std::vector<std::unique_ptr<Field>> get_output() override {
            if (has_next) {
                std::vector<std::unique_ptr<Field>> output_copy;
                for (const auto& field : current_output) {
                    output_copy.push_back(field->clone());
                }
                return output_copy;
            } else {
                return {};
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
                std::size_t field_hash = 0;

                // Depending on the type, hash the corresponding data
                switch (field.type) {
                    case INT: {
                        // Convert integer data to string and hash
                        int value = *reinterpret_cast<const int*>(field.data.get());
                        field_hash = hasher(std::to_string(value));
                        break;
                    }
                    case FLOAT: {
                        // Convert float data to string and hash
                        float value = *reinterpret_cast<const float*>(field.data.get());
                        field_hash = hasher(std::to_string(value));
                        break;
                    }
                    case STRING: {
                        // Directly hash the string data
                        std::string value(field.data.get(), field.data_length - 1); // Exclude null-terminator
                        field_hash = hasher(value);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported field type for hashing.");
                }

                // Combine the hash of the current field with the hash so far
                hash ^= field_hash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };


public:
    HashAggregationOperator(std::unique_ptr<Operator> input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs)
        : UnaryOperator(std::move(input)), group_by_attrs(group_by_attrs), aggr_funcs(aggr_funcs) {}

    void open() override {
        input->open(); // Ensure the input operator is opened
        output_tuples_index = 0;
        output_tuples.clear();

        // Assume a hash map to aggregate tuples based on group_by_attrs
        std::unordered_map<std::vector<Field>, std::vector<Field>, FieldVectorHasher> hash_table;

        while (input->next()) {
            const auto& tuple = input->get_output(); // Assume get_output returns a reference to the current tuple

            // Extract group keys and initialize aggregation values
            std::vector<Field> group_keys;
            for (auto& index : group_by_attrs) {
                group_keys.push_back(*tuple[index]); // Deep copy the Field object for group key
            }

            // Process aggregation functions
            if (!hash_table.count(group_keys)) {
                // Initialize aggregate values for a new group
                std::vector<Field> aggr_values(aggr_funcs.size(), Field());
                hash_table[group_keys] = aggr_values;
            }

            // Update aggregate values
            auto& aggr_values = hash_table[group_keys];
            for (size_t i = 0; i < aggr_funcs.size(); ++i) {
                // Simplified update logic for demonstration
                // You'll need to implement actual aggregation logic here
                aggr_values[i] = update_aggregate(aggr_funcs[i], aggr_values[i], *tuple[aggr_funcs[i].attr_index]);
            }
        }

        // Prepare output tuples from the hash table
        for (const auto& entry : hash_table) {
            const auto& group_keys = entry.first;
            const auto& aggr_values = entry.second;
            Tuple output_tuple;
            // Assuming Tuple has a method to add Fields
            for (const auto& key : group_keys) {
                output_tuple.add_field(std::make_unique<Field>(key)); // Add group keys to the tuple
            }
            for (const auto& value : aggr_values) {
                output_tuple.add_field(std::make_unique<Field>(value)); // Add aggregated values to the tuple
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

    std::vector<std::unique_ptr<Field>> get_output() override {
        std::vector<std::unique_ptr<Field>> output_copy;

        if (output_tuples_index == 0 || output_tuples_index > output_tuples.size()) {
            // If there is no current tuple because next() hasn't been called yet or we're past the last tuple,
            // return an empty vector.
            return output_copy; // This will be an empty vector
        }

        // Assuming that output_tuples stores Tuple objects and each Tuple has a vector of Field objects or similar
        const auto& current_tuple = output_tuples[output_tuples_index - 1]; // Adjust for 0-based indexing after increment in next()

        // Assuming the Tuple class provides a way to access its fields, e.g., a method or a public member
        for (const auto& field : current_tuple.fields) {
            output_copy.push_back(field->clone()); // Use the clone method to create a deep copy of each field
        }

        return output_copy;
    }


private:

    Field update_aggregate(const AggrFunc& aggr_func, const Field& current_aggr, const Field& new_value) {
        if (new_value.get_type() == FieldType::NULLV) {
            return *current_aggr.clone();
        }

        if (current_aggr.get_type() == FieldType::NULLV) {
            switch (aggr_func.func) {
                case AggrFuncType::COUNT: {
                    return Field(1);
                }
                case AggrFuncType::SUM: {
                    if (new_value.get_type() == FieldType::STRING) {
                        return Field("");
                    }
                    return *new_value.clone();
                }
                case AggrFuncType::MAX:
                case AggrFuncType::MIN: {
                    return *new_value.clone();
                }
                default:
                    throw std::runtime_error("Unsupported aggregation function.");
            }
        }

        if (current_aggr.get_type() != new_value.get_type()) {
            throw std::runtime_error("Mismatched Field types in aggregation.");
        }

        switch (aggr_func.func) {
            case AggrFuncType::COUNT: {
                if (current_aggr.get_type() == FieldType::INT) {
                    // For COUNT, simply increment the integer value
                    int count = current_aggr.as_int() + 1;
                    return Field(count);
                }
                break;
            }
            case AggrFuncType::SUM: {
                if (current_aggr.get_type() == FieldType::INT) {
                    int sum = current_aggr.as_int() + new_value.as_int();
                    return Field(sum);
                } else if (current_aggr.get_type() == FieldType::FLOAT) {
                    float sum = current_aggr.as_float() + new_value.as_float();
                    return Field(sum);
                } else if (current_aggr.get_type() == FieldType::STRING) {
                    return Field("");
                }
                break;
            }
            case AggrFuncType::MAX: {
                if (current_aggr.get_type() == FieldType::INT) {
                    int max = std::max(current_aggr.as_int(), new_value.as_int());
                    return Field(max);
                } else if (current_aggr.get_type() == FieldType::FLOAT) {
                    float max = std::max(current_aggr.as_float(), new_value.as_float());
                    return Field(max);
                } else if (current_aggr.get_type() == FieldType::STRING) {
                    std::string max = std::max(current_aggr.as_string(), new_value.as_string());
                    return Field(max);
                }
                break;
            }
            case AggrFuncType::MIN: {
                if (current_aggr.get_type() == FieldType::INT) {
                    int min = std::min(current_aggr.as_int(), new_value.as_int());
                    return Field(min);
                } else if (current_aggr.get_type() == FieldType::FLOAT) {
                    float min = std::min(current_aggr.as_float(), new_value.as_float());
                    return Field(min);
                } else if (current_aggr.get_type() == FieldType::STRING) {
                    std::string min = std::min(current_aggr.as_string(), new_value.as_string());
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

class ValueOperator : public Operator {
private:
    std::unique_ptr<Tuple> value_tuple;
    size_t num_times;

    size_t num_times_remaining;
    bool has_output;

public:
    ValueOperator(std::unique_ptr<Tuple> value_tuple) : value_tuple(std::move(value_tuple)), num_times(1) {}
    ValueOperator(std::unique_ptr<Tuple> value_tuple, size_t num_times) : value_tuple(std::move(value_tuple)), num_times(num_times) {}

    void open() override {
        num_times_remaining = num_times;
        has_output = false;
    }

    bool next() override {
        if (num_times_remaining == 0) {
            has_output = false;
            return false;
        }
        num_times_remaining--;
        has_output = true;
        return true;
    }

    void close() override {
        num_times_remaining = num_times;
        has_output = false;
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        if (has_output) {
            std::vector<std::unique_ptr<Field>> output_copy;
            for (auto& field : value_tuple->fields) {
                output_copy.push_back(field->clone());
            }
            return output_copy;
        }
        return {};
    }
};

class ValuesIteratorOperator : public Operator {
private:
    std::vector<std::unique_ptr<Tuple>> value_tuples;

    size_t curr_count;
    bool has_output;

public:
    ValuesIteratorOperator(std::vector<std::unique_ptr<Tuple>>&& value_tuples) : value_tuples(std::move(value_tuples)) {}

    void open() override {
        curr_count = 0;
        has_output = false;
    }

    bool next() override {
        if (curr_count >= value_tuples.size()) {
            has_output = false;
            return false;
        }
        curr_count++;
        has_output = true;
        return true;
    }

    void close() override {
        curr_count = 0;
        has_output = false;
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        if (has_output) {
            std::vector<std::unique_ptr<Field>> output_copy;
            for (auto& field : value_tuples[curr_count - 1]->fields) {
                output_copy.push_back(field->clone());
            }
            return output_copy;
        }
        return {};
    }
};

class InsertOperator : public UnaryOperator {
private:
    BufferManager& buffer_manager;
    TableID table_id;

public:
    InsertOperator(std::unique_ptr<Operator> input, BufferManager& manager, TableID table_id)
        : UnaryOperator(std::move(input)), buffer_manager(manager), table_id(table_id) {}

    void open() override {
        input->open();
    }

    bool next() override {
        if (!input->next()) return false; // No tuple to insert

        std::unique_ptr<Tuple> tuple_to_insert = std::make_unique<Tuple>();
        for (auto& field : input->get_output()) {
            tuple_to_insert->add_field(field->clone());
        }

        for (size_t page_id = 0; page_id < buffer_manager.get_num_pages(table_id); ++page_id) {
            auto& page = buffer_manager.get_page(table_id, page_id);
            // Attempt to insert the tuple
            if (page->add_tuple(tuple_to_insert->clone())) { 
                // Flush the page to disk after insertion
                buffer_manager.flush_page(table_id, page_id); 
                return true; // Insertion successful
            }
        }

        // If insertion failed in all existing pages, extend the database and try again
        buffer_manager.extend(table_id);
        auto& new_page = buffer_manager.get_page(table_id, buffer_manager.get_num_pages(table_id) - 1);
        if (new_page->add_tuple(tuple_to_insert->clone())) {
            buffer_manager.flush_page(table_id, buffer_manager.get_num_pages(table_id) - 1);
            return true; // Insertion successful after extending the database
        }

        return false; // Insertion failed even after extending the database
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        return {}; // Return empty vector
    }
};

class DeleteOperator : public UnaryOperator {
private:
    BufferManager& buffer_manager;

public:
    DeleteOperator(std::unique_ptr<Operator> input, BufferManager& manager) 
        : UnaryOperator(std::move(input)), buffer_manager(manager) {}

    void open() override {
        input->open();
    }

    bool next() override {
        if (input->next()) {
            auto [table_id, page_id, tuple_id] = input->tuple_metadata;
            auto& page = buffer_manager.get_page(table_id, page_id);
            page->delete_tuple(tuple_id); // Perform deletion
            buffer_manager.flush_page(table_id, page_id); // Flush the page to disk after deletion
            return true;
        }
        return false;
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> get_output() override {
        return {}; // Return empty vector
    }
};

class TableManager;
std::vector<std::unique_ptr<Tuple>> plan_and_execute_internal_query(
    BufferManager& buffer_manager,
    TableManager& table_manager,
    const std::string& query
);

using ColumnID = uint16_t;
class TableColumn {
public:
    std::string name;
    ColumnID idx;
    FieldType type;
    bool not_null;

public:
    TableColumn(std::string name, ColumnID idx, FieldType type) : name(name), idx(idx), type(type), not_null(false) {}
    TableColumn(std::string name, ColumnID idx, FieldType type, bool not_null) : name(name), idx(idx), type(type), not_null(not_null) {}

    friend std::ostream& operator<<(std::ostream& os, TableColumn const& column) {
        return os << "Column idx=" << column.idx << " :: name=" << column.name << " :: type=" << column.type << " :: not_null=" << column.not_null;
    }
};
using TableColumns = std::vector<std::unique_ptr<TableColumn>>;

class TableSchema {
private:
    void init() {
        std::sort(this->columns.begin(), this->columns.end(), [&](const std::unique_ptr<TableColumn>& column1, const std::unique_ptr<TableColumn>& column2) {
            return column1->idx < column2->idx;
        });
        for (auto& column : this->columns) {
            columns_map[column->name] = column->idx;
        }
    }

public:
    std::string name;
    TableID id;
    TableColumns columns;

    std::unordered_map<std::string, ColumnID> columns_map;

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
        columns.insert(it, std::move(column));
        columns_map[column_name] = columns.back()->idx;
    }

    ColumnID find_column_idx(std::string name) {
        if (columns_map.find(name) == columns_map.end()) {
            return INVALID_VALUE;
        }
        return columns_map[name];
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
        columns.push_back(std::make_unique<TableColumn>("id", 0, FieldType::INT, true));
        columns.push_back(std::make_unique<TableColumn>("name", 1, FieldType::STRING, true));
        return columns;
    })()
);
static const std::shared_ptr<TableSchema> SYSTEM_COLUMN_SCHEMA = std::make_shared<TableSchema>("system_column", SYSTEM_COLUMN_TABLE_ID,
    ([]{
        TableColumns columns;
        columns.push_back(std::make_unique<TableColumn>("table_id", 0, FieldType::INT, true));
        columns.push_back(std::make_unique<TableColumn>("name", 1, FieldType::STRING, true));
        columns.push_back(std::make_unique<TableColumn>("idx", 2, FieldType::INT, true));
        columns.push_back(std::make_unique<TableColumn>("type", 3, FieldType::INT, true));
        columns.push_back(std::make_unique<TableColumn>("not_null", 4, FieldType::INT, true));
        return columns;
    })()
);

constexpr size_t MAX_SCHEMAS_IN_MEMORY = 2 << 8;
constexpr size_t MAX_NAMES_IN_MEMORY = 2 << 12;
class TableManager {
public:
    static const TableID NUM_SYSTEM_TABLES = 3;
private:
    BufferManager& buffer_manager;
    TableID next_table_id;

    std::unique_ptr<LruPolicy<TableID>> schema_policy;
    std::unordered_map<TableID, std::shared_ptr<TableSchema>> schema_map;

    std::unique_ptr<LruPolicy<std::string>> name_policy;
    std::unordered_map<std::string, TableID> name_map;

    void bootstrap_system_tables() {
        bootstrap_system_next_table_id();
        bootstrap_system_class_table();
        bootstrap_system_column_table();
    }

    void bootstrap_system_next_table_id() {
        auto& next_table_id_page = buffer_manager.get_page(SYSTEM_NEXT_TABLE_ID, 0);
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
        name_map[SYSTEM_CLASS_SCHEMA->name] = SYSTEM_CLASS_TABLE_ID;
        name_map[SYSTEM_COLUMN_SCHEMA->name] = SYSTEM_COLUMN_TABLE_ID;

        // If the system tables do not exist, run the bootstrap process
        if (!buffer_manager.file_exists(SYSTEM_NEXT_TABLE_ID)) {
            bootstrap_system_tables();
            return;
        }

        auto& next_table_id_page = buffer_manager.get_page(SYSTEM_NEXT_TABLE_ID, 0);
        TableID potential_next_table_id = *reinterpret_cast<TableID*>(next_table_id_page->page_data.get());
        next_table_id = potential_next_table_id < NUM_SYSTEM_TABLES ? NUM_SYSTEM_TABLES : potential_next_table_id;
    }

    ~TableManager() {
        schema_map.clear();

        auto& next_table_id_page = buffer_manager.get_page(SYSTEM_NEXT_TABLE_ID, 0);
        TableID* next_table_id_ptr = reinterpret_cast<TableID*>(next_table_id_page->page_data.get());
        *next_table_id_ptr = next_table_id < NUM_SYSTEM_TABLES ? NUM_SYSTEM_TABLES : next_table_id;
    }

    TableID get_table_id(std::string name) {
        if (name_map.find(name) != name_map.end()) {
            // std::cout << "Fetched table id from cache: id=" << name_map[name] << " :: name=" << name << std::endl;
            name_policy->touch(name);
            return name_map[name];
        }

        auto query = std::format("SELECT id FROM system_class WHERE name = '{}'", name);
        auto output_tuples = plan_and_execute_internal_query(buffer_manager, *this, query);
        if (output_tuples.size() == 0) {
            return std::numeric_limits<TableID>::max();
        }

        auto tuple = std::move(output_tuples[0]);
        TableID table_id = tuple->fields[0]->as_int();
        if (name_map.size() >= MAX_NAMES_IN_MEMORY) {
            auto evicted_name = name_policy->evict();
            if (evicted_name != "" && name_map[evicted_name] >= NUM_SYSTEM_TABLES) {
                // std::cout << "Evicted table id from cache: id=" << name_map[evicted_name] << " :: name=" << evicted_name << std::endl;
                name_map.erase(evicted_name);
            }
        }
        name_policy->touch(name);
        name_map[name] = table_id;
        // std::cout << "Fetched table id from disk: id=" << name_map[name] << " :: name=" << name << std::endl;
        return table_id;
    }

    std::shared_ptr<TableSchema> get_table_schema(std::string name) {
        TableID table_id;
        if ((table_id = get_table_id(name)) == std::numeric_limits<TableID>::max()) {
            throw std::invalid_argument("Failed to fetch table schema from disk: name=" + name + " :: Table does not exist.");
        }
        return get_table_schema(table_id);
    }

    std::shared_ptr<TableSchema> get_table_schema(TableID table_id) {
        if (schema_map.find(table_id) != schema_map.end()) {
            // std::cout << "Fetched table schema from cache: id=" << table_id << std::endl;
            auto table_schema = schema_map[table_id];
            if (table_id >= NUM_SYSTEM_TABLES) {
                schema_policy->touch(table_id);
            }
            return table_schema;
        }

        // Traverse through system_class and get all metadata for given table_id
        std::string table_metadata_query = std::format("SELECT name FROM system_class WHERE id = {}", table_id);
        auto table_metadata_tuples = plan_and_execute_internal_query(buffer_manager, *this, table_metadata_query);
        if (table_metadata_tuples.size() == 0) {
            throw std::invalid_argument("Failed to fetch table schema from disk: id=" + std::to_string(table_id) + " :: Table does not exist.");
        }
        std::string name = table_metadata_tuples[0]->fields[0]->as_string();
        std::shared_ptr<TableSchema> table_schema = std::make_shared<TableSchema>(name, table_id);

        // Traverse through system_column and get all column metadata for given table_id
        std::string table_column_query = std::format("SELECT name, idx, type, not_null FROM system_column WHERE table_id = {}", table_id);
        auto table_column_tuples = plan_and_execute_internal_query(buffer_manager, *this, table_column_query);
        for (auto& tuple : table_column_tuples) {
            std::unique_ptr<TableColumn> column = std::make_unique<TableColumn>(
                tuple->fields[0]->as_string(),
                tuple->fields[1]->as_int(),
                FieldType(tuple->fields[2]->as_int()),
                tuple->fields[3]->as_int() == 0 ? false : true
            );
            table_schema->add_column(std::move(column));
        }

        if (schema_map.size() >= MAX_SCHEMAS_IN_MEMORY) {
            TableID evicted_table_id = schema_policy->evict();
            if (evicted_table_id != INVALID_VALUE && evicted_table_id >= NUM_SYSTEM_TABLES) {
                // std::cout << "Evicted table id from cache: id=" << evicted_table_id << std::endl;
                schema_map.erase(evicted_table_id);
            }
        }
        schema_map[table_id] = table_schema;
        schema_policy->touch(table_id);
        // std::cout << "Fetched table schema from disk: id=" << table_id << std::endl;
        return table_schema;
    }

    bool create_table(std::shared_ptr<TableSchema> table_schema, bool is_bootstrap) {
        if (!is_bootstrap) {
            // Check if table exists
            std::string table_name_query = std::format("SELECT * FROM system_class WHERE name = '{}'", table_schema->name);
            auto table_metadata_tuples = plan_and_execute_internal_query(buffer_manager, *this, table_name_query);
            if (table_metadata_tuples.size() > 0) {
                // std::cerr << "Table " << table_schema->name << " already exists in the database" << std::endl;
                return false;
            }
        }

        TableID table_id = next_table_id++;

        // Insert table id and name in system_class
        std::string insert_system_class_query = std::format("INSERT INTO system_class VALUES ({}, {})", table_id, table_schema->name);
        auto insert_system_class_res = plan_and_execute_internal_query(buffer_manager, *this, insert_system_class_query);
        assert(insert_system_class_res.size() == 1);

        // Insert table columns in system_column
        std::stringstream insert_system_column_query;
        std::string separator = "";
        insert_system_column_query << "INSERT INTO system_column VALUES ";
        for (auto& column : table_schema->columns) {
            insert_system_column_query << separator;
            insert_system_column_query << std::format("({}, {}, {}, {}, {})", table_id, column->name, column->idx, (int) column->type, (int) column->not_null);
            separator = ", ";
        }
        auto insert_system_column_res = plan_and_execute_internal_query(buffer_manager, *this, insert_system_column_query.str());
        assert(insert_system_column_res.size() == table_schema->columns.size());
        table_schema->id = table_id;
        buffer_manager.extend(table_id);

        return true;
    }

    bool create_table(std::shared_ptr<TableSchema> table_schema) {
        return create_table(table_schema, false);
    }

    bool drop_table(std::string name) {
        TableID table_id;
        if ((table_id = get_table_id(name)) == std::numeric_limits<TableID>::max()) {
            throw std::invalid_argument("Failed to drop table: name=" + name + " :: Table does not exist.");
        }
        return drop_table(table_id);
    }

    bool drop_table(TableID table_id) {
        // Check if table exists in system_class
        std::string table_metadata_query = std::format("SELECT name FROM system_class WHERE id = {}", table_id);
        auto table_metadata_tuples = plan_and_execute_internal_query(buffer_manager, *this, table_metadata_query);
        if (table_metadata_tuples.size() == 0) {
            throw std::invalid_argument("Failed to drop table: id=" + std::to_string(table_id) + " :: Table does not exist.");
        }
        std::string table_name = table_metadata_tuples[0]->fields[0]->as_string();

        std::string system_class_delete_query = std::format("DELETE FROM system_class WHERE id = {}", table_id);
        plan_and_execute_internal_query(buffer_manager, *this, system_class_delete_query);
        std::string system_column_delete_query = std::format("DELETE FROM system_column WHERE table_id = {}", table_id);
        plan_and_execute_internal_query(buffer_manager, *this, system_column_delete_query);

        name_map.erase(table_name);
        name_policy->erase(table_name);
        schema_map.erase(table_id);
        schema_policy->erase(table_id);
        buffer_manager.delete_file(table_id);
        return true;
    }
};

struct QueryComponents {
    enum class QueryType {
        SELECT,
        INSERT,
        DELETE,
        CREATE_TABLE,
        DROP_TABLE
    };
    QueryType type;

    // Common components
    TableID table_id;
    std::vector<TableColumn> output_columns;
    bool is_ddl = false;

    // SELECT components
    std::vector<size_t> select_attributes;
    
    struct AggregateInfo {
        AggrFuncType func_type;
        size_t attribute_index;
    };
    std::vector<AggregateInfo> aggregates;
    
    std::vector<size_t> group_by_attributes;
    
    struct WhereCondition {
        size_t attribute_index;
        SimplePredicate::ComparisonOperator op;
        std::unique_ptr<Field> value;
    };
    std::vector<WhereCondition> where_conditions;
    
    struct HavingCondition {
        size_t aggregate_index;
        SimplePredicate::ComparisonOperator op;
        std::unique_ptr<Field> value;
    };
    std::vector<HavingCondition> having_conditions;

    // INSERT components
    std::vector<ColumnID> insert_columns;
    std::vector<std::vector<std::unique_ptr<Field>>> insert_values;
    std::unique_ptr<QueryComponents> insert_select_query;

    // CREATE TABLE components
    std::shared_ptr<TableSchema> create_table_schema;
};

QueryComponents parse_query(TableManager& table_manager, const std::string& query, bool is_admin) {
    QueryComponents components;

    // Determine query type
    if (query.substr(0, 6) == "SELECT") {
        components.type = QueryComponents::QueryType::SELECT;
        std::regex table_regex("FROM\\s+(\\w+)");
        std::smatch table_match;
        if (std::regex_search(query, table_match, table_regex)) {
            std::string table_name = table_match[1].str();
            components.table_id = table_manager.get_table_id(table_name);
            if (components.table_id == std::numeric_limits<TableID>::max()) {
                throw std::invalid_argument("Table does not exist: " + table_name);
            }
        } else {
            throw std::invalid_argument("Could not find table name in query");
        }

        auto table_schema = table_manager.get_table_schema(components.table_id);
        std::regex delimiter_regex(", ");

        // Parse GROUP BY
        std::regex group_by_regex("GROUP BY\\s+(\\w+(?:\\s*,\\s*\\w+)*)");
        std::smatch group_by_match;
        if (std::regex_search(query, group_by_match, group_by_regex)) {
            if (!group_by_match[1].str().empty()) {
                std::string match = group_by_match[1].str();
                std::sregex_token_iterator begin(match.begin(), match.end(), delimiter_regex, -1);
                std::sregex_token_iterator end;
                for (auto it = begin; it != end; ++it) {
                    std::string item = it->str();

                    size_t column_idx = table_schema->find_column_idx(item);
                    if (column_idx == INVALID_VALUE) {
                        throw std::invalid_argument("Column not found: " + item);
                    }
                    components.group_by_attributes.push_back(column_idx);
                }
            }
        }

        // Parse SELECT clause
        std::regex select_regex("SELECT\\s+((?:\\w+\\(\\w+\\)|\\w+|\\*)(?:\\s*,\\s*(?:\\w+\\(\\w+\\)|\\w+|\\*))*)");
        std::regex agg_regex("(\\w+)\\((\\w+)\\)");
        std::smatch select_matches;
        std::string::const_iterator query_start(query.cbegin());
        if (std::regex_search(query_start, query.cend(), select_matches, select_regex) && !select_matches[1].str().empty()) {
            std::string match = select_matches[1].str();
            std::sregex_token_iterator begin(match.begin(), match.end(), delimiter_regex, -1);
            std::sregex_token_iterator end;

            std::vector<std::pair<size_t, size_t>> column_ref_idxs;
            for (auto it = begin; it != end; ++it) {
                std::string item = it->str();

                if (item == "*") {
                    for (auto& column : table_schema->columns) {
                        components.output_columns.push_back({column->name, (ColumnID) components.output_columns.size(), column->type});
                    }
                } else {
                    components.output_columns.push_back({item, (ColumnID) components.output_columns.size(), NULLV});
                }

                // Check if it's an aggregate function
                std::smatch agg_match;
                auto& gb_attributes = components.group_by_attributes;
                if (std::regex_match(item, agg_match, agg_regex)) {
                    std::string func_name = agg_match[1].str();
                    std::string col_name = agg_match[2].str();

                    size_t column_idx = table_schema->find_column_idx(col_name);
                    if (column_idx == INVALID_VALUE) {
                        throw std::invalid_argument("Column not found: " + col_name);
                    }

                    AggrFuncType func_type;
                    if (func_name == "SUM") func_type = AggrFuncType::SUM;
                    else if (func_name == "COUNT") func_type = AggrFuncType::COUNT;
                    else if (func_name == "MIN") func_type = AggrFuncType::MIN;
                    else if (func_name == "MAX") func_type = AggrFuncType::MAX;
                    else throw std::invalid_argument("Unknown aggregate function: " + func_name);

                    if (func_type == AggrFuncType::COUNT) {
                        components.output_columns.back().type = INT;
                    } else {
                        components.output_columns.back().type = table_schema->columns[column_idx]->type;
                    }

                    components.aggregates.push_back({func_type, column_idx});
                } else if (item == "*") {
                    // All columns
                    auto column_it = table_schema->columns.begin();
                    for (size_t column_idx = 0; column_idx < table_schema->columns.size(); column_idx++) {
                        if (gb_attributes.size() != 0 && std::find(gb_attributes.begin(), gb_attributes.end(), column_it->get()->idx) == gb_attributes.end()) {
                            throw std::invalid_argument(column_it->get()->name + " is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.");
                        }
                        column_ref_idxs.push_back({column_idx, column_ref_idxs.size() + components.aggregates.size()});
                        components.output_columns.back().type = table_schema->columns[column_idx]->type;
                        column_it++;
                    }
                } else {
                    // Regular column reference
                    size_t column_idx = table_schema->find_column_idx(item);
                    if (gb_attributes.size() != 0 && std::find(gb_attributes.begin(), gb_attributes.end(), column_idx) == gb_attributes.end()) {
                        throw std::invalid_argument(item + " is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.");
                    }
                    if (column_idx == INVALID_VALUE) {
                        throw std::invalid_argument("Column not found: " + item);
                    }
                    column_ref_idxs.push_back({column_idx, column_ref_idxs.size() + components.aggregates.size()});
                    components.output_columns.back().type = table_schema->columns[column_idx]->type;
                }
            }

            size_t i = 0;
            size_t j = 0;
            for (size_t k = 0; k < column_ref_idxs.size() + components.aggregates.size(); k++) {
                if (i < column_ref_idxs.size() && column_ref_idxs[i].second == k) {
                    components.select_attributes.push_back(column_ref_idxs[i].first);
                    i++;
                } else {
                    components.select_attributes.push_back(components.group_by_attributes.size() + j);
                    j++;
                }
            }
        }

        // Parse WHERE conditions
        std::regex where_regex("WHERE\\s+(\\w+\\s*[<>=]+\\s*(?:\\d+|\\d+\\.\\d+|'[^']*')(?:\\s+AND\\s+\\w+\\s*[<>=]+\\s*(?:\\d+|\\d+\\.\\d+|'[^']*'))*)");
        std::smatch where_matches;
        if (std::regex_search(query, where_matches, where_regex) && !where_matches[1].str().empty()) {
            std::string where_clause = where_matches[1].str();
            std::regex cond_regex("(\\w+)\\s*([<>=]+)\\s*(\\d+|\\d+\\.\\d+|'[^']*')");
            std::sregex_iterator begin(where_clause.begin(), where_clause.end(), cond_regex);
            std::sregex_iterator end;

            for (auto it = begin; it != end; ++it) {
                std::string col_name = (*it)[1].str();
                size_t column_idx = table_schema->find_column_idx(col_name);
                if (column_idx == INVALID_VALUE) {
                    throw std::invalid_argument("Column not found: " + col_name);
                }

                std::string op_str = (*it)[2].str();
                std::string value_str = (*it)[3].str();
                
                std::unique_ptr<Field> value;
                if (value_str[0] == '\'') {
                    // String value
                    value = std::make_unique<Field>(value_str.substr(1, value_str.length()-2));
                } else if (value_str.find('.') != std::string::npos) {
                    // Float value
                    value = std::make_unique<Field>(std::stof(value_str));
                } else {
                    // Integer value
                    value = std::make_unique<Field>(std::stoi(value_str));
                }

                if (table_schema->columns[column_idx]->type != value->type) {
                    throw std::invalid_argument("Cannot compare values of different types");
                }

                SimplePredicate::ComparisonOperator op;
                if (op_str == "=") op = SimplePredicate::ComparisonOperator::EQ;
                else if (op_str == "<") op = SimplePredicate::ComparisonOperator::LT;
                else if (op_str == ">") op = SimplePredicate::ComparisonOperator::GT;
                else if (op_str == "<=") op = SimplePredicate::ComparisonOperator::LE;
                else if (op_str == ">=") op = SimplePredicate::ComparisonOperator::GE;
                else if (op_str == "<>") op = SimplePredicate::ComparisonOperator::NE;

                components.where_conditions.push_back({
                    column_idx,
                    op,
                    std::move(value)
                });
            }
        }

        // Parse HAVING conditions
        std::regex having_regex("HAVING\\s+(\\w+\\(\\w+\\)\\s*[<>=]+\\s*\\d+(?:\\s+AND\\s+\\w+\\(\\w+\\)\\s*[<>=]+\\s*\\d+)*)");
        std::smatch having_matches;
        if (std::regex_search(query, having_matches, having_regex) && !having_matches[1].str().empty()) {
            std::string having_clause = having_matches[1].str();
            std::regex cond_regex("(\\w+)\\((\\w+)\\)\\s*([<>=]+)\\s*(\\d+)");
            std::sregex_iterator begin(having_clause.begin(), having_clause.end(), cond_regex);
            std::sregex_iterator end;

            for (auto it = begin; it != end; ++it) {
                std::string func_name = (*it)[1].str();
                std::string col_name = (*it)[2].str();
                size_t column_idx = table_schema->find_column_idx(col_name);
                if (column_idx == INVALID_VALUE) {
                    throw std::invalid_argument("Column not found: " + col_name);
                }

                AggrFuncType func_type;
                if (func_name == "SUM") func_type = AggrFuncType::SUM;
                else if (func_name == "COUNT") func_type = AggrFuncType::COUNT;
                else if (func_name == "MIN") func_type = AggrFuncType::MIN;
                else if (func_name == "MAX") func_type = AggrFuncType::MAX;
                else throw std::invalid_argument("Unknown aggregate function: " + func_name);

                std::string op_str = (*it)[3].str();
                int value = std::stoi((*it)[4].str());

                SimplePredicate::ComparisonOperator op;
                if (op_str == "=") op = SimplePredicate::ComparisonOperator::EQ;
                else if (op_str == "<") op = SimplePredicate::ComparisonOperator::LT;
                else if (op_str == ">") op = SimplePredicate::ComparisonOperator::GT;
                else if (op_str == "<=") op = SimplePredicate::ComparisonOperator::LE;
                else if (op_str == ">=") op = SimplePredicate::ComparisonOperator::GE;
                else if (op_str == "<>") op = SimplePredicate::ComparisonOperator::NE;

                components.aggregates.push_back({func_type, column_idx});

                components.having_conditions.push_back({
                    components.aggregates.size() - 1,
                    op,
                    std::make_unique<Field>(value)
                });
            }
        }
    } else if (query.substr(0, 6) == "INSERT") {
        components.type = QueryComponents::QueryType::INSERT;
        
        // Parse INSERT query
        std::regex insert_regex("INSERT INTO\\s+(\\w+)(?:\\s*\\(((?:\\w+(?:,\\s*\\w+)*)*)\\))?\\s*(VALUES\\s*\\((.*?)\\)(?:\\s*,\\s*\\((.*?)\\))*|SELECT .*)");
        std::smatch insert_match;
        
        if (std::regex_search(query, insert_match, insert_regex)) {
            // Get table name
            std::string table_name = insert_match[1].str();
            components.table_id = table_manager.get_table_id(table_name);
            if (components.table_id == std::numeric_limits<TableID>::max()) {
                throw std::invalid_argument("Table does not exist: " + table_name);
            }
            if (components.table_id < TableManager::NUM_SYSTEM_TABLES && !is_admin) {
                throw std::invalid_argument("Cannot perform INSERT queries on system tables :: Insufficient permissions.");
            }
            auto table_schema = table_manager.get_table_schema(components.table_id);

            // Parse column names if specified
            if (insert_match[2].matched) {
                std::string cols = insert_match[2].str();
                std::regex col_regex("\\w+");
                std::sregex_iterator col_begin(cols.begin(), cols.end(), col_regex);
                std::sregex_iterator col_end;
                for (auto it = col_begin; it != col_end; ++it) {
                    size_t column_idx = table_schema->find_column_idx(it->str());
                    if (column_idx == INVALID_VALUE) {
                        throw std::invalid_argument("Column not found: " + it->str());
                    }
                    if (std::find(components.insert_columns.begin(), components.insert_columns.end(), column_idx) != components.insert_columns.end()) {
                        throw std::invalid_argument(std::format("Cannot have duplicate column '{}' in INSERT query", it->str()));
                    }
                    components.insert_columns.push_back(column_idx);
                }
                if (components.insert_columns.size() == 0) {
                    throw std::invalid_argument("Number of INSERT columns cannot be 0");
                }
            } else {
                for (size_t column_idx = 0; column_idx < table_schema->columns.size(); column_idx++) {
                    components.insert_columns.push_back(column_idx);
                }
            }

            if (insert_match[3].str().substr(0, 6) == "VALUES") {
                // Parse VALUES
                std::string values_str = insert_match[3].str();
                std::regex value_regex("\\((.*?)\\)");
                std::sregex_iterator value_begin(values_str.begin(), values_str.end(), value_regex);
                std::sregex_iterator value_end;

                for (auto it = value_begin; it != value_end; ++it) {
                    std::string tuple_values = (*it)[1].str();
                    std::regex field_regex("([^,]+)");
                    std::sregex_iterator field_begin(tuple_values.begin(), tuple_values.end(), field_regex);
                    std::sregex_iterator field_end;

                    std::vector<std::unique_ptr<Field>> tuple_fields(table_schema->columns.size());
                    for (size_t i = 0; i < tuple_fields.size(); i++) {
                        tuple_fields[i] = std::make_unique<Field>();
                    }
                    size_t field_idx = 0;
                    for (auto field_it = field_begin; field_it != field_end; ++field_it, ++field_idx) {
                        if (field_idx >= components.insert_columns.size()) {
                            throw std::invalid_argument(std::format("Size of new tuple does not match the number of columns ({})", components.insert_columns.size()));
                        }
                        std::string value = field_it->str();
                        // Trim whitespace
                        value.erase(0, value.find_first_not_of(" \t"));
                        value.erase(value.find_last_not_of(" \t") + 1);

                        FieldType field_type = table_schema->columns[components.insert_columns[field_idx]]->type;

                        switch (field_type) {
                            case FieldType::INT:
                                tuple_fields[components.insert_columns[field_idx]] = std::make_unique<Field>(std::stoi(value));
                                break;
                            case FieldType::FLOAT:
                                tuple_fields[components.insert_columns[field_idx]] = std::make_unique<Field>(std::stof(value));
                                break;
                            case FieldType::STRING:
                                // Remove quotes if present
                                if (value.front() == '"' || value.front() == '\'') {
                                    value = value.substr(1, value.length() - 2);
                                }
                                tuple_fields[components.insert_columns[field_idx]] = std::make_unique<Field>(value);
                                break;
                            default:
                                throw std::invalid_argument("Field not implemented");
                        }
                    }
                    if (field_idx != components.insert_columns.size()) {
                        throw std::invalid_argument(std::format("Size of new tuple ({}) does not match the number of columns ({})", field_idx, components.insert_columns.size()));
                    }
                    for (size_t i = 0; i < table_schema->columns.size(); i++) {
                        if (table_schema->columns[i]->not_null && tuple_fields[i]->type == NULLV) {
                            throw std::invalid_argument("Column '" + table_schema->columns[i]->name + "' cannot be NULL");
                        }
                    }
                    components.insert_values.push_back(std::move(tuple_fields));
                }
            } else {
                // Parse SELECT
                std::string select_query = query.substr(query.find("SELECT"));
                components.insert_select_query = std::make_unique<QueryComponents>(parse_query(table_manager, select_query, is_admin));
                if (components.table_id == components.insert_select_query->table_id) {
                    throw std::invalid_argument("Cannot insert tuples from the same table");
                }

                size_t select_query_output_size = components.insert_select_query->output_columns.size();
                if (select_query_output_size != table_schema->columns.size()) {
                    throw std::invalid_argument(std::format("Size of new tuple ({}) does not match the number of columns in schema ({})", select_query_output_size, table_schema->columns.size()));
                }
                for (size_t i = 0; i < table_schema->columns.size(); i++) {
                    if (table_schema->columns[i]->type != components.insert_select_query->output_columns[i].type) {
                        throw std::invalid_argument("Invalid type provided for column '" + table_schema->columns[i]->name + "'.");
                    }
                }
            }
        } else {
            throw std::invalid_argument("Invalid INSERT query syntax");
        }
    } else if (query.substr(0, 6) == "DELETE") {
        components.type = QueryComponents::QueryType::DELETE;

        // Parse DELETE FROM query
        std::regex delete_regex("DELETE FROM\\s+(\\w+)(\\s+WHERE\\s+\\w+\\s*[<>=]+\\s*(?:\\d+|\\d+\\.\\d+|'[^']*')(?:\\s+AND\\s+\\w+\\s*[<>=]+\\s*(?:\\d+|\\d+\\.\\d+|'[^']*'))*)?");
        std::smatch delete_match;

        if (std::regex_search(query, delete_match, delete_regex)) {
            std::string table_name = delete_match[1].str();
            components.table_id = table_manager.get_table_id(table_name);
            if (components.table_id == std::numeric_limits<TableID>::max()) {
                throw std::invalid_argument("Table does not exist: " + table_name);
            }
            if (components.table_id < TableManager::NUM_SYSTEM_TABLES && !is_admin) {
                throw std::invalid_argument("Cannot perform DELETE queries on system tables :: Insufficient permissions.");
            }

            auto table_schema = table_manager.get_table_schema(components.table_id);

            // Parse WHERE clause if it exists
            if (delete_match[2].matched) {
                std::string where_clause = delete_match[2].str();
                std::regex cond_regex("(\\w+)\\s*([<>=]+)\\s*(\\d+|\\d+\\.\\d+|'[^']*')");
                std::sregex_iterator begin(where_clause.begin(), where_clause.end(), cond_regex);
                std::sregex_iterator end;

                for (auto it = begin; it != end; ++it) {
                    std::string col_name = (*it)[1].str();
                    size_t column_idx = table_schema->find_column_idx(col_name);
                    if (column_idx == INVALID_VALUE) {
                        throw std::invalid_argument("Column not found: " + col_name);
                    }

                    std::string op_str = (*it)[2].str();
                    std::string value_str = (*it)[3].str();

                    std::unique_ptr<Field> value;
                    if (value_str[0] == '\'') {
                        // String value
                        value = std::make_unique<Field>(value_str.substr(1, value_str.length()-2));
                    } else if (value_str.find('.') != std::string::npos) {
                        // Float value
                        value = std::make_unique<Field>(std::stof(value_str));
                    } else {
                        // Integer value
                        value = std::make_unique<Field>(std::stoi(value_str));
                    }

                    if (table_schema->columns[column_idx]->type != value->type) {
                        throw std::invalid_argument("Cannot compare values of different types");
                    }

                    SimplePredicate::ComparisonOperator op;
                    if (op_str == "=") op = SimplePredicate::ComparisonOperator::EQ;
                    else if (op_str == "<") op = SimplePredicate::ComparisonOperator::LT;
                    else if (op_str == ">") op = SimplePredicate::ComparisonOperator::GT;
                    else if (op_str == "<=") op = SimplePredicate::ComparisonOperator::LE;
                    else if (op_str == ">=") op = SimplePredicate::ComparisonOperator::GE;
                    else if (op_str == "<>") op = SimplePredicate::ComparisonOperator::NE;

                    components.where_conditions.push_back({
                        column_idx,
                        op,
                        std::move(value)
                    });
                }
            }
        } else {
            throw std::invalid_argument("Invalid DELETE FROM syntax");
        }
    } else if (query.substr(0, 12) == "CREATE TABLE") {
        components.type = QueryComponents::QueryType::CREATE_TABLE;
        components.is_ddl = true;

        // Parse CREATE TABLE query
        std::regex create_table_regex("CREATE TABLE\\s+(\\w+)\\s*\\((.*?)\\)");
        std::smatch create_table_match;

        if (std::regex_search(query, create_table_match, create_table_regex)) {
            std::string table_name = create_table_match[1].str();
            std::string columns_str = create_table_match[2].str();

            // Create new table schema
            auto schema = std::make_shared<TableSchema>(table_name);

            // Parse column definitions
            std::regex column_regex("(\\w+)\\s+(INT|STRING|FLOAT)(?:\\s+NOT NULL)?");
            std::sregex_iterator column_begin(columns_str.begin(), columns_str.end(), column_regex);
            std::sregex_iterator column_end;

            ColumnID idx = 0;
            for (auto it = column_begin; it != column_end; ++it) {
                std::string col_name = (*it)[1].str();
                std::string type_str = (*it)[2].str();
                bool not_null = it->str().find("NOT NULL") != std::string::npos;

                FieldType type;
                if (type_str == "INT") type = FieldType::INT;
                else if (type_str == "STRING") type = FieldType::STRING;
                else if (type_str == "FLOAT") type = FieldType::FLOAT;
                else throw std::invalid_argument("Invalid column type: " + type_str);

                schema->add_column(std::make_unique<TableColumn>(col_name, idx++, type, not_null));
            }
            components.create_table_schema = schema;
        } else {
            throw std::invalid_argument("Invalid CREATE TABLE syntax");
        }
    } else if (query.substr(0, 10) == "DROP TABLE") {
        components.type = QueryComponents::QueryType::DROP_TABLE;
        components.is_ddl = true;

        // Parse DROP TABLE query
        std::regex drop_table_regex("DROP TABLE\\s+(\\w+)");
        std::smatch drop_table_match;

        if (std::regex_search(query, drop_table_match, drop_table_regex)) {
            std::string table_name = drop_table_match[1].str();
            components.table_id = table_manager.get_table_id(table_name);
            if (components.table_id == std::numeric_limits<TableID>::max()) {
                throw std::invalid_argument("Table does not exist: " + table_name);
            }
            if (components.table_id < TableManager::NUM_SYSTEM_TABLES && !is_admin) {
                throw std::invalid_argument("Cannot perform DROP TABLE queries on system tables :: Insufficient permissions.");
            }
        }
    } else {
        throw std::invalid_argument("Invalid query type provided");
    }

    if (query.substr(0, 6) != "SELECT") {
        components.output_columns.push_back({"SUCCESS", 0, NULLV});
    }

    return components;
}

std::unique_ptr<Operator> plan_query(const QueryComponents& components, BufferManager& buffer_manager) {
    if (components.type == QueryComponents::QueryType::SELECT) {
        // Start with scan
        std::unique_ptr<Operator> current_op = std::make_unique<ScanOperator>(buffer_manager, components.table_id);

        // Add WHERE conditions if any
        if (!components.where_conditions.empty()) {
            auto complex_pred = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
            for (const auto& cond : components.where_conditions) {
                auto simple_pred = std::make_unique<SimplePredicate>(
                    SimplePredicate::Operand(cond.attribute_index),
                    SimplePredicate::Operand(std::make_unique<Field>(*cond.value)),
                    cond.op
                );
                complex_pred->add_predicate(std::move(simple_pred));
            }
            current_op = std::make_unique<SelectOperator>(std::move(current_op), std::move(complex_pred));
        }

        // Add aggregation if needed
        if (!components.aggregates.empty() || !components.group_by_attributes.empty()) {
            std::vector<AggrFunc> aggr_funcs;
            for (const auto& agg : components.aggregates) {
                aggr_funcs.push_back({agg.func_type, agg.attribute_index});
            }
            current_op = std::make_unique<HashAggregationOperator>(std::move(current_op), components.group_by_attributes, aggr_funcs);

            // Add HAVING if needed
            if (!components.having_conditions.empty()) {
                auto having_pred = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
                for (const auto& cond : components.having_conditions) {
                    auto simple_pred = std::make_unique<SimplePredicate>(
                        SimplePredicate::Operand(components.group_by_attributes.size() + cond.aggregate_index),
                        SimplePredicate::Operand(std::make_unique<Field>(*cond.value)),
                        cond.op
                    );
                    having_pred->add_predicate(std::move(simple_pred));
                }
                current_op = std::make_unique<SelectOperator>(std::move(current_op), std::move(having_pred));
            }
        }

        // Add final projection
        current_op = std::make_unique<ProjectOperator>(std::move(current_op), components.select_attributes);
        return current_op;
    } else if (components.type == QueryComponents::QueryType::INSERT) {
        std::unique_ptr<Operator> current_op;

        if (components.insert_select_query) {
            // INSERT from SELECT
            std::unique_ptr<Operator> select_query_op = plan_query(*components.insert_select_query, buffer_manager);
            current_op = std::make_unique<InsertOperator>(std::move(select_query_op), buffer_manager, components.table_id);
        } else {
            // INSERT from VALUES
            std::vector<std::unique_ptr<Tuple>> tuples_to_insert;
            for (auto& tuple_fields : components.insert_values) {
                auto tuple = std::make_unique<Tuple>();
                for (auto& field : tuple_fields) {
                    tuple->add_field(field->clone());
                }
                tuples_to_insert.push_back(std::move(tuple));
            }
            std::unique_ptr<ValuesIteratorOperator> values_op = std::make_unique<ValuesIteratorOperator>(std::move(tuples_to_insert));
            current_op = std::make_unique<InsertOperator>(std::move(values_op), buffer_manager, components.table_id);
        }
        return current_op;
    } else if (components.type == QueryComponents::QueryType::DELETE) {
        // Start with scan
        std::unique_ptr<Operator> current_op = std::make_unique<ScanOperator>(buffer_manager, components.table_id);

        // Add WHERE conditions if any
        if (!components.where_conditions.empty()) {
            auto complex_pred = std::make_unique<ComplexPredicate>(ComplexPredicate::LogicOperator::AND);
            for (const auto& cond : components.where_conditions) {
                auto simple_pred = std::make_unique<SimplePredicate>(
                    SimplePredicate::Operand(cond.attribute_index),
                    SimplePredicate::Operand(std::make_unique<Field>(*cond.value)),
                    cond.op
                );
                complex_pred->add_predicate(std::move(simple_pred));
            }
            current_op = std::make_unique<SelectOperator>(std::move(current_op), std::move(complex_pred));
        }

        // Add final DELETE operator
        current_op = std::make_unique<DeleteOperator>(std::move(current_op), buffer_manager);
        return current_op;
    }

    throw std::invalid_argument("Query type cannot be planned.");
}

void execute_query(TableManager& table_manager, BufferManager& buffer_manager, const QueryComponents& components) {
    std::vector<size_t> col_widths;
    std::vector<std::vector<std::string>> rows;

    for (const auto& column : components.output_columns) {
        col_widths.push_back(column.name.length());
    }

    if (components.is_ddl) {
        switch (components.type) {
            case QueryComponents::QueryType::CREATE_TABLE: {
                bool res = table_manager.create_table(components.create_table_schema);
                if (!res) {
                    throw std::invalid_argument("Table '" + components.create_table_schema->name + "' already exists in the database");
                }
                break;
            }
            case QueryComponents::QueryType::DROP_TABLE:
                table_manager.drop_table(components.table_id);
                break;
            default:
                throw std::invalid_argument("Query is not of DDL type.");
        }
    } else {
        std::unique_ptr<Operator> root_op = plan_query(components, buffer_manager);
        root_op->open();
        while (root_op->next()) {
            const auto& output = root_op->get_output();
            std::vector<std::string> row;

            // Convert each field to string and track max width
            int i = 0;
            for (const auto& field : output) {
                std::stringstream ss;
                field->print(ss);
                std::string val = ss.str();
                row.push_back(val);

                col_widths[i] = std::max(col_widths[i], val.length());
                i++;
            }
            rows.push_back(std::move(row));
        }
        root_op->close();
    }

    const auto& print_row_separator = [&] {
        for (size_t width : col_widths) {
            std::cout << '+' << std::string(width + 2, '-');
        }
        std::cout << "+\n";
    };

    const auto& print_row = [&] (const std::vector<std::string>& row) {
        for (size_t i = 0; i < row.size(); i++) {
            std::cout << "| " << std::left << std::setw(col_widths[i]) << row[i] << ' ';
        }
        std::cout << "|\n";
    };

    std::vector<std::string> output_column_names;
    std::for_each(components.output_columns.begin(), components.output_columns.end(), [&](const TableColumn& col) { output_column_names.push_back(col.name); });

    print_row_separator();
    print_row(output_column_names);
    print_row_separator();
    if (components.type == QueryComponents::QueryType::SELECT && rows.size() > 0) {
        for (const auto& row : rows) {
            print_row(row);
        }
        print_row_separator();
    }
    std::cout << std::format("({} row{})\n\n", rows.size(), rows.size() == 1 ? "" : "s");
}

std::vector<std::unique_ptr<Tuple>> plan_and_execute_internal_query(
    BufferManager& buffer_manager,
    TableManager& table_manager,
    const std::string& query
) {
    // std::cout << "Executing sub-query :: " << query << std::endl;
    auto components = parse_query(table_manager, query, true);
    auto root_op = plan_query(components, buffer_manager);
    std::vector<std::unique_ptr<Tuple>> output_copy;

    root_op->open();
    while (root_op->next()) {
        const auto& output = root_op->get_output();
        std::unique_ptr<Tuple> tuple_copy = std::make_unique<Tuple>();
        for (const auto& field : output) {
            tuple_copy->add_field(field->clone());
        }
        output_copy.push_back(std::move(tuple_copy));
    }
    root_op->close();
    return output_copy;
}

// void pretty_print(const QueryComponents& components) {
//     std::cout << "Query Components:\n";
//     std::cout << "  Selected Attributes: ";
//     for (auto attr : components.select_attributes) {
//         std::cout << "{" << attr + 1 << "} "; // Convert back to 1-based indexing for display
//     }
//     std::cout << "\n  SUM Operation: " << (components.sum_operation ? "Yes" : "No");
//     if (components.sum_operation) {
//         std::cout << " on {" << components.sum_attribute_index + 1 << "}";
//     }
//     std::cout << "\n  GROUP BY: " << (components.group_by ? "Yes" : "No");
//     if (components.group_by) {
//         std::cout << " on {" << components.group_by_attribute_index + 1 << "}";
//     }
//     std::cout << "\n  WHERE Condition: " << (components.where_condition ? "Yes" : "No");
//     if (components.where_condition) {
//         std::cout << " on {" << components.where_attribute_index + 1 << "} > " << components.lower_bound << " and < " << components.upper_bound;
//     }
//     std::cout << std::endl;
// }

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

    void execute_queries() {

        std::vector<std::string> test_queries = {
            // "SELECT * FROM system_class WHERE id > 2 and id < 6 GROUP BY id",
            "SELECT * FROM test_table_2",
            "SELECT * FROM system_class",
            "INSERT INTO test_table_2 SELECT * FROM system_class",
            "SELECT * FROM test_table_2",
            "SELECT * FROM system_column WHERE name = 'name'",
            "SELECT id, name FROM system_class",
            "INSERT INTO test_table_2 VALUES (5, 'hello')",
            "INSERT INTO test_table_2 VALUES (6, 'bye'), (7, 'oh'), (8, 'yo')",
            "INSERT INTO test_table_2 (id) VALUES (9), (10)",
            // "INSERT INTO test_table_2 VALUES (9, 'hello'), (10, 'bye'), (11)",
            "SELECT name FROM system_class",
            "SELECT SUM(id) FROM system_class WHERE id > 2 and id < 6 GROUP BY id",
            "SELECT table_id, name, idx, type, * FROM system_column",
            "SELECT * FROM system_column",
            "SELECT SUM(idx), table_id, COUNT(type) FROM system_column GROUP BY table_id",
            "SELECT id, name FROM system_class WHERE id <> 1",
            "SELECT id, name FROM system_class WHERE id > 1 AND id < 3",
            "SELECT SUM(idx), table_id, COUNT(type) FROM system_column GROUP BY table_id HAVING SUM(idx) < 6",
            "SELECT * FROM test_table_2",
            "SELECT * FROM test_table_2 WHERE name = '2'",
        };

        for (const auto& query : test_queries) {
            std::cout << "Executing query :: " << query << std::endl;
            auto components = parse_query(table_manager, query, false);
            //pretty_print(components);
            execute_query(table_manager, buffer_manager, components);
        }
    }

    void start_cli() {
        std::cout << "Welcome to BuzzDB CLI!\n";
        std::cout << "Enter SQL queries or type 'exit' to quit.\n";
        std::cout << "Example queries:\n";
        std::cout << "  SELECT * FROM system_class;\n";
        std::cout << "  INSERT INTO test_table_2 VALUES (5, 'hello');\n";
        std::cout << "  SELECT id, name FROM system_class WHERE id > 1;\n\n";

        std::string query;
        std::string line;
        bool is_new_query = true;
        while (true) {
            if (is_new_query) {
                std::cout << "buzzdb> ";
                is_new_query = false;
            } else {
                std::cout << "     -> "; // Show continuation prompts
            }
            std::getline(std::cin, line);

            if (line == "exit" || line == "q" || line == "quit") {
                break;
            }

            if (line.empty()) {
                continue;
            }

            query += " " + line;

            // Check if query ends with semicolon
            if (line.find(';') != std::string::npos) {
                // Remove leading whitespace
                query = query.substr(query.find_first_not_of(" \t\n\r\f\v"));
                // Remove the semicolon before parsing
                query = query.substr(0, query.find_first_of(";"));

                try {
                    auto components = parse_query(table_manager, query, false);
                    execute_query(table_manager, buffer_manager, components);
                } catch (const std::exception& e) {
                    std::cerr << "Error: " << e.what() << "\n\n";
                }
                query.clear(); // Reset for next query
                is_new_query = true;
            }
        }
        std::cout << "Goodbye!\n";
    }
};

int main() {
    BuzzDB db;

    auto schema1 = db.table_manager.get_table_schema(SYSTEM_CLASS_TABLE_ID);
    auto schema2 = db.table_manager.get_table_schema("system_column");
    // std::cout << *schema1 << std::endl;
    // std::cout << *schema2 << std::endl;

    std::shared_ptr<TableSchema> new_schema = std::make_shared<TableSchema>("test_table");
    new_schema->add_column(std::make_unique<TableColumn>("col1", 0, FieldType::INT, true));
    new_schema->add_column(std::make_unique<TableColumn>("col2", 1, FieldType::STRING, true));
    new_schema->add_column(std::make_unique<TableColumn>("col3", 2, FieldType::FLOAT, true));
    db.table_manager.create_table(new_schema, false);

    std::shared_ptr<TableSchema> new_schema_2 = std::make_shared<TableSchema>("test_table_2");
    new_schema_2->add_column(std::make_unique<TableColumn>("id", 0, FieldType::INT, true));
    new_schema_2->add_column(std::make_unique<TableColumn>("name", 1, FieldType::STRING, true));
    db.table_manager.create_table(new_schema_2, false);

    // assert(create_table_res == true);

    // auto schema3 = db.table_manager.get_table_schema("test_table");
    // auto schema4 = db.table_manager.get_table_schema("test_table");

    // std::cout << *schema3 << std::endl;
    // std::cout << *schema4 << std::endl;

    // auto start = std::chrono::high_resolution_clock::now();

    // db.execute_queries();

    // auto end = std::chrono::high_resolution_clock::now();

    // // Calculate and print the elapsed time
    // std::chrono::duration<double> elapsed = end - start;
    // std::cout << "Elapsed time: " << 
    // std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count() 
    //       << " microseconds" << std::endl;
    
    db.start_cli();
    
    return 0;
}