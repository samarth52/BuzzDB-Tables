#include "../../include/storage/SlottedPage.h"
#include <cstring>
#include <iostream>
#include <sstream>
#include <cassert>

SlottedPage::SlottedPage() : page_data(std::make_unique<char[]>(PAGE_SIZE)), metadata_size(sizeof(Slot) * MAX_SLOTS) {
    // Initialize slot array inside page
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
        slot_array[slot_itr].empty = true;
        slot_array[slot_itr].offset = INVALID_VALUE;
        slot_array[slot_itr].length = INVALID_VALUE;
    }
}

bool SlottedPage::addTuple(std::unique_ptr<Tuple> tuple) {
    // Serialize the tuple into a char array
    auto serializedTuple = tuple->serialize();
    size_t tuple_size = serializedTuple.size();

    assert(tuple_size == 38);

    // Check for first slot with enough space
    size_t slot_itr = 0;
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());        
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
        if (slot_array[slot_itr].empty == true and 
            slot_array[slot_itr].length >= tuple_size) {
            break;
        }
    }
    if (slot_itr == MAX_SLOTS){
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

void SlottedPage::deleteTuple(size_t index) {
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    size_t slot_itr = 0;
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
        if(slot_itr == index and
           slot_array[slot_itr].empty == false){
            slot_array[slot_itr].empty = true;
            break;
           }
    }
}

void SlottedPage::print() const {
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
