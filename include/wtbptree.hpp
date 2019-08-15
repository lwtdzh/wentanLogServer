/**
 * Wentan B+ Tree. HPP file. Only for WT log system. Cannot be used for other system.
 * Date: 2019/7/29.
 * Author: LiWentan.
 */

#include "wtbptree.h"

namespace {

const bool debug = false; // DEBUG USED.
const bool d2 = false; // DEBUG USED.
const uint32_t LEAF_CAPACITY = 200; // The amount of elements of the leaf.
const uint32_t VAL_CAPACITY = 30; // The amount of elements of the value.
const char ZERO = 0;
char g_buff[1000000]; // 1MB.
char g_buff2[1000000];
char g_buff3[1000000];
char g_buff4[1000000];

void wtmemcpy(void* des, void* src, size_t size2) {
    if (size2 > 1000000) {
        toscreen << "COPY OVER DATA./n";
        sleep(1000);
    }
    static char buffer[1000000];
    memcpy(buffer, src, size2);
    memcpy(des, buffer, size2);
}

int wtfwrite(void* src, size_t size, size_t num, FILE* file) {
    if (fwrite(src, size, num, file) != num) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }
    return num;
}

int wtfread(void* des, size_t size, size_t num, FILE* file) {
    return fread(des, size, num ,file);
}

int wtfseek(FILE* file, long int offset, int whence) {
    return fseeko64(file, offset, whence);
}

} // End anoynomous namespace.

namespace wt {

int Compare::compare(const int& lhs, const int& rhs) {
    return lhs - rhs;
}

int Compare::compare(const short& lhs, const short& rhs) {
    return lhs - rhs;
}

int Compare::compare(const int64_t& lhs, const int64_t& rhs) {
    return lhs - rhs;
}

int Compare::compare(const string& lhs, const string& rhs) {
    return strcmp(lhs.c_str(), rhs.c_str());
}

template <typename K, typename V, typename CMP>
BPTree<K, V, CMP>::BPTree() : _file(NULL) {}

template <typename K, typename V, typename CMP>
bool BPTree<K, V, CMP>::init(const string& file_path, uint32_t floor_num) {
    char buffer[1024];
    // Open file.
    _file = fopen(file_path.c_str(), "r+b");
    if (_file == NULL) {
        _file = fopen(file_path.c_str(), "a+b");
        fclose(_file);
        _file = fopen(file_path.c_str(), "r+b");
        if (_file == NULL) {
            toscreen << "[ERROR]OPEN FILE FAILED.\n";
            return false;
        }
    }
    if (fseeko64(_file, 0, SEEK_SET) != 0) {
        toscreen << "[ERROR]FSEEK FAILED.\n";
        return false;
    }
    // Read f_number.
    int ret = wtfread(buffer, 20, 1, _file);
    if (ret != 1) {
        // Empty file. Set to head.
        ret = fseeko64(_file, 0, SEEK_SET);
        if (ret != 0) {
            toscreen << "[ERROR]Set file pointer to head failed.\n";
            fclose(_file);
            _file = NULL;
            return false;
        }
        // Set size and f_num.
        _size = 0;
        _f_num = floor_num;
        _floor_size = 16 + _f_num * sizeof(K) + (_f_num + 1) * 8;
        _value_size = VAL_CAPACITY * sizeof(V) + 12;
        _leaf_size = LEAF_CAPACITY * (_value_size + sizeof(K)) + 20;
        if (debug) {
            toscreen << "LEAF SIZE: " << _leaf_size << ".\n";
            toscreen << "VALUE SIZE: " << _value_size << ".\n";
        }
        // Write f_num.
        ret = wtfwrite(&_f_num, sizeof(uint32_t), 1, _file);
        if (ret != 1) {
            toscreen << "[ERROR]Write floor_num failed.\n";
            fclose(_file);
            _file = NULL;
            return false;
        }
        // Write size.
        ret = wtfwrite(&_size, sizeof(uint64_t), 1, _file);
        if (ret != 1) {
            toscreen << "[ERROR]Write size failed.\n";
            fclose(_file);
            _file = NULL;
            return false;
        }
        // Write head position.
        _head = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
        ret = wtfwrite(&_head, sizeof(uint64_t), 1, _file);
        if (ret != 1) {
            toscreen << "[ERROR]Write head position failed.\n";
            fclose(_file);
            _file = NULL;
            return false;
        }
        // Zero head floor.
        ret = _write_empty_to_file(_file, _floor_size);
        if (ret != _floor_size) {
            toscreen << "[ERROR]Write empty to head node failed.\n";
            fclose(_file);
            _file = NULL;
            return false;
        }
        // Finish init new tree.
        toscreen << "[INFO]Successfully initialize empty bptree. File: " << file_path << " f_num:" << _f_num << ".\n";
        fflush(_file);
        return true;
    }

    // Initialize from existing tree.
    // Read f_num.
    _f_num = *(uint32_t*)buffer;
    // Read size.
    _size = *(uint64_t*)(buffer + 4);
    // Read head.
    _head = *(uint64_t*)(buffer + 12);
    // Init some other data.
    _floor_size = 16 + _f_num * sizeof(K) + (_f_num + 1) * 8;
    _value_size = VAL_CAPACITY * sizeof(V) + 12;
    _leaf_size = LEAF_CAPACITY * (_value_size + sizeof(K)) + 20;
    if (debug) {
        toscreen << "LEAF SIZE: " << _leaf_size << ".\n";
        toscreen << "VALUE SIZE: " << _value_size << ".\n";
    }
    // Read tail.
    ret = fseeko64(_file, 0, SEEK_END);
    if (ret != 0) {
        toscreen << "[ERROR]Find seek_end failed.\n";
        fclose(_file);
        _file = NULL;
        return false;
    }
    // Finish init from existing tree.
    toscreen << "[INFO]Init from existing tree successfully. F_NUM: " << _f_num << ", SIZE: " << _size << ", HEAD_POS:" << _head << ".\n";
    fflush(_file);
    return true;
}

template <typename K, typename V, typename CMP>
bool BPTree<K, V, CMP>::insert(const K& key, const V& val) {
    // Get the leaf.
    uint64_t floor_pos;
    uint32_t offset;
    _get_pos(key, &floor_pos, &offset);
    if (d2) {
        toscreen << "[INFO]Insert find pos: " << floor_pos << " offset: " << offset << ".\n";
    }
    _read_floor(g_buff, floor_pos);

    // Check if error.
    if (_if_leaf(g_buff) == 1) {
        toscreen << "[ERROR]FIND A NON LEAF.\n";
        fclose(_file);
        sleep(1000);
    }
    if (offset > _f_used(g_buff)) {
        toscreen << "[ERROR]OVER CAPACITY.\n";
        fclose(_file);
        sleep(1000);
    }
    if (_f_used(g_buff) > _f_num) {
        toscreen << "[ERROR]OVER CAPACITY. f_used: " << _f_used(g_buff) << " offset: " << offset << ".\n";
        print_structure(0, 0);
        fclose(_file);
        sleep(1000);
    }

    // Got the leaf. If empty, new the leaf.
    uint64_t& leaf_pos = _next_floor(g_buff, offset);
    if (leaf_pos == 0) {
        if (_size != 0) {
            toscreen << "[ERROR]LOGIC ERROR.\n";
            sleep(1000);
        }
        leaf_pos = _create_leaf(floor_pos);
        if (debug) {
            toscreen << "CREATE LEAF AT: " << leaf_pos << ".\n";
        }
        _write_pointer_to_floor(floor_pos, leaf_pos, offset);
    }
    if (debug) {
        toscreen << "READ leaf at: " << leaf_pos << ".\n";
    }
    _read_leaf(g_buff2, leaf_pos);
    // If leaf is not full, push.
    if (debug) {
        toscreen << "Leaf used: " << _leaf_used(g_buff2) << ".\n";
    }
    if (_leaf_used(g_buff2) < LEAF_CAPACITY) {
        // Push element to the leaf.
        _push_element_to_leaf(g_buff2, key, val);
        // Write the leaf to disk.
        _write_leaf(g_buff2, leaf_pos);
        // Write size to disk.
        ++_size;
        if (fseeko64(_file, 4, SEEK_SET) != 0) {
            toscreen << "[ERROR]SEEK ERROR.\n";
            sleep(1000);
        }
        if (debug) {
            std::cout << "WRITE SIZE2: " << _size << std::endl;
        }
        if (wtfwrite(&_size, 8, 1, _file) != 1) {
            toscreen << "[ERROR]WRITE ERROR.\n";
            sleep(1000);
        }
        return true;
    }

    // g_buff:  The floor.
    // g_buff2: The leaf node.
    // g_buff3: The expand tail elements.
    // g_buff4: The new leaf node.

    if (_leaf_used(g_buff2) != LEAF_CAPACITY) {
        toscreen << "[ERROR]LOGIC ERROR.\n";
        sleep(1000);
    }

    // If leaf is full, expand.
    if (_f_used(g_buff) <= _f_num) {
        if (debug) {
            toscreen << "Start to expand leaf, the left leaf add: " << leaf_pos << ".\n";
        }
        // Normally expand this floor.
        uint32_t reserved_num = LEAF_CAPACITY / 2;
        // Copy the tail elements to buff3.
        memcpy(g_buff3, (g_buff2 + 12 + (reserved_num * (sizeof(K) + _value_size))),
            (sizeof(K) + _value_size) * (LEAF_CAPACITY - reserved_num));
        // Set the used_count of buff2.
        _leaf_used(g_buff2) = reserved_num;
        // New a leaf node at disk. Set buff4 as new leaf node.
        uint64_t new_leaf_pos = _create_leaf(floor_pos);
        memset(g_buff4, 0, _leaf_size);
        // Set the next pointer of new leaf and last leaf.
        uint64_t next_leaf_of_leaf_2 = _next_leaf(g_buff2);
        _next_leaf(g_buff2) = new_leaf_pos;
        _next_leaf(g_buff4) = next_leaf_of_leaf_2;
        // Set father of buff4(new leaf).
        _leaf_father(g_buff4) = _leaf_father(g_buff2);
        // Set used_count of buff4(new leaf).
        _leaf_used(g_buff4) = LEAF_CAPACITY - reserved_num;
        // Set the tail elements from buff3 to buff4.
        memcpy((g_buff4 + 12), g_buff3, (sizeof(K) + _value_size) * _leaf_used(g_buff4));
        // Move the last floor for one step.
        wtmemcpy((g_buff + 16 + (sizeof(K) + 8) * (offset + 1) + 8),
            (g_buff + 16 + (sizeof(K) + 8) * offset + 8),
            _floor_size - (16 + (sizeof(K) + 8) * offset) - 8);
        // Set the floor data (offset) to the new leaf.
        memcpy(&_get_key(g_buff, offset) ,(K*)g_buff3, sizeof(K));
        _next_floor(g_buff, offset + 1) = new_leaf_pos;
        ++_f_used(g_buff);
        // Write the new element to leaf.
        if (CMP::compare(*(K*)g_buff3, key) <= 0) {
            _push_element_to_leaf(g_buff4, key, val);
        } else {
            _push_element_to_leaf(g_buff2, key, val);
        }
        if (debug) {
            toscreen << "Expand leaf succ. RIGHT leaf add: " << new_leaf_pos << ".\n";
        }
        // Write leaf to disk and over.
        _write_leaf(g_buff2, leaf_pos);
        _write_leaf(g_buff4, new_leaf_pos);
    } else {
        // ERROR.
        toscreen << "[ERROR]A floor is over size.\n";
        fclose(_file);
        sleep(1000);
    }

    // The leaf is full, and the floor is also full.
    while (_f_used(g_buff) >= _f_num + 1) {
        // floor_pos: The floor's position which is to be spilt.
        // g_buff:  The floor to be split.
        // g_buff2: The father floor of g_buff.
        // g_buff3: The new floor after cur floor.

        if (_f_used(g_buff) > _f_num + 1) {
            toscreen << "[ERROR]LOGIC ERROR.\n";
            sleep(1000);
        }

        if (debug) {
            toscreen << "Expand a floor.\n";
        }

        // Find the father of cur floor, if not, create one.
        if (_f_father(g_buff) == 0) {
            _f_father(g_buff) = _create_floor(0);
            memset(g_buff2, 0, _floor_size);
            _next_floor(g_buff2, 0) = floor_pos;
            _if_leaf(g_buff2) = 1;
            // This father is a root, change the head.
            _head = _f_father(g_buff);
            if (fseeko64(_file, 12, SEEK_SET) != 0) {
                toscreen << "[ERROR]SEEK ERR.\n";
                sleep(1000);
            }
            if (wtfwrite(&_head, 8, 1, _file) != 1) {
                toscreen << "[ERROR]WRITE ERR.\n";
                sleep(1000);
            }
        } else {
            _read_floor(g_buff2, _f_father(g_buff));
        }

        // Reserved number.
        uint32_t reserved_num = _f_num / 2;

        // Set the data of new floor.
        uint64_t new_floor_pos = _create_floor(_f_father(g_buff));
        memset(g_buff3, 0, _floor_size);
        _f_father(g_buff3) = _f_father(g_buff);
        _if_leaf(g_buff3) = _if_leaf(g_buff);
        memcpy((g_buff3 + 16),
            (g_buff + 16 + (sizeof(K) + 8) * (reserved_num + 1)),
            (_f_num - reserved_num) * (sizeof(K) + 8) + 8);

        // Set used_count.
        _f_used(g_buff) = reserved_num;
        _f_used(g_buff3) = _f_num - reserved_num;

        // Handle the child of new floor.
        for (size_t i = 0; i < _f_used(g_buff3) + 1; ++i) {
            uint64_t child_pos = _next_floor(g_buff3, i);
            _set_father(child_pos, new_floor_pos);
        }

        // Handle the father.
        uint32_t offset_at_father = 0;
        K& discard_key = _get_key(g_buff, reserved_num);
        while (offset_at_father < _f_used(g_buff2)) {
            if (CMP::compare(_get_key(g_buff2, offset_at_father), discard_key) <= 0) {
                ++offset_at_father;
                continue;
            }
            break;
        }
        if (offset_at_father < _f_used(g_buff2)) {
            ++_f_used(g_buff2);
            wtmemcpy((g_buff2 + 16 + (sizeof(K) + 8) * offset_at_father + 8 + sizeof(K) + 8),
                (g_buff2 + 16 + (sizeof(K) + 8) * offset_at_father + 8),
                _floor_size - (16 + (sizeof(K) + 8) * offset_at_father + 8));
            memcpy(&_get_key(g_buff2, offset_at_father), &discard_key, sizeof(K));
            _next_floor(g_buff2, offset_at_father + 1) = new_floor_pos;
        } else if (offset_at_father == _f_used(g_buff2)) {
            ++_f_used(g_buff2);
            memcpy(&_get_key(g_buff2, _f_used(g_buff2) - 1), &discard_key, sizeof(K));
            _next_floor(g_buff2, _f_used(g_buff2)) = new_floor_pos;
        } else {
            toscreen << "[ERROR]Search error.\n";
            fclose(_file);
            sleep(1000);
        }

        // Save the current floor.
        _write_floor(g_buff, floor_pos);
        _write_floor(g_buff3, new_floor_pos);
        memcpy(g_buff, g_buff2, _floor_size + sizeof(K) + 8);
        floor_pos = _f_father(g_buff3);
    }
    // Floor is OK, write to disk and over.
    _write_floor(g_buff, floor_pos);
    ++_size;
    if (debug) {
        std::cout << "WRITE SIZE: " << _size << std::endl;
    }
    if (fseeko64(_file, 4, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfwrite(&_size, 8, 1, _file) != 1) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }
    return true;
}

template <typename K, typename V, typename CMP>
int BPTree<K, V, CMP>::search(const K& key, std::vector<V>* val) {
    uint64_t floor_pos;
    uint32_t offset;
    _get_pos(key, &floor_pos, &offset);
    if (d2) {
        toscreen << "Search find pos: " << floor_pos << " offset: " << offset << ".\n";
    }
    // Read this floor.
    if (fseeko64(_file, floor_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        perror("INFO: ");
        fclose(_file);
        sleep(1000);
    }
    if (wtfread(g_buff, _floor_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        toscreen << "Floor_size: " << _floor_size << ".\n";
        fclose(_file);
        sleep(1000);
    }
    // Get the leaf.
    uint64_t& leaf_pos = _next_floor(g_buff, offset);
    if (fseeko64(_file, leaf_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        perror("INFO: ");
        fclose(_file);
        sleep(1000);
    }
    if (wtfread(g_buff2, _leaf_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        toscreen << "Floor_size: " << _floor_size << ".\n";
        fclose(_file);
        sleep(1000);
    }
    // Find key.
    uint32_t leaf_used = _leaf_used(g_buff2);
    if (debug) {
        toscreen << "LEAF USED: " << leaf_used << ".\n";
    }
    uint32_t found_pos = 0;
    for (; found_pos < leaf_used; ++found_pos) {
        if (CMP::compare(_key_at(g_buff2, found_pos), key) == 0) {
            break;
        }
    }
    if (found_pos == _leaf_used(g_buff2)) {
        // NOT FOUND.
        if (debug) {
            toscreen << "NOT FOUND.\n";
        }
        return -1;
    }
    // Read Value.
    uint64_t value_pos = 12 + found_pos * (sizeof(K) + _value_size) + sizeof(K);
    uint32_t value_used = *(uint32_t*)(g_buff2 + value_pos);
    if (debug) {
        toscreen << "FOUND, value used: " << value_used << ".\n";
    }
    for (uint32_t i = 0; (i < value_used && i < VAL_CAPACITY); ++i) {
        V& cur_val = *(V*)(g_buff2 + value_pos + 4 + sizeof(V) * i);
        (*val).push_back(cur_val);
    }
    if (value_used > VAL_CAPACITY) {
        int32_t got_num = VAL_CAPACITY;
        int64_t cur_next_pos = *(int64_t*)(g_buff2 + value_pos + 4 + sizeof(V) * VAL_CAPACITY);
        int32_t total_val_num = (value_used - 1) / VAL_CAPACITY;
        for (int k = 1; k <= total_val_num; ++k) {
            if (fseeko64(_file, cur_next_pos, SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
            if (fread(g_buff2, _value_size, 1, _file) != 1) {
                toscreen << "READ ERR.\n";
                sleep(1000);
            }
            for (int r = 0; r < VAL_CAPACITY; ++r) {
                (*val).push_back(*(V*)(g_buff2 + 4 + r * sizeof(V)));
                ++got_num;
                if (got_num == value_used) {
                    return (*val).size();
                }
            }
            cur_next_pos = *(int64_t*)(g_buff2 + 4 + VAL_CAPACITY * sizeof(V));
        }
    }
    return (*val).size();
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_get_pos(const K& key, uint64_t* floor_pos, uint32_t* offset) {
    uint64_t cur_f = _head;
    uint32_t cur_o = 0;
    do {
        // Read this floor.
        if (fseeko64(_file, cur_f, SEEK_SET) != 0) {
            toscreen << "[ERROR]SEEK ERROR.\n";
            toscreen << "cur_f: " << cur_f << ".\n";
            perror("INFO: ");
            fclose(_file);
            sleep(1000);
        }
        if (wtfread(g_buff, _floor_size, 1, _file) != 1) {
            toscreen << "[ERROR]READ ERROR.\n";
            toscreen << "Floor_size: " << _floor_size << ".\n";
            fclose(_file);
            sleep(1000);
        }
        // Find the next floor (_used_num + 1 means the last pointer).
        while (cur_o < _f_used(g_buff) && CMP::compare(_get_key(g_buff, cur_o), key) <= 0) {
            // Should be the next node.
            ++cur_o;
        }
        // If this floor is leaf floor, it is founded.
        if (_if_leaf(g_buff) == 0) {
            break;
        }
        // If not leaf, enter the next floor.
        cur_f = _next_floor(g_buff, cur_o);
        cur_o = 0;
    } while (true);
    // Found the target place.
    *floor_pos = cur_f;
    *offset = cur_o;
}

template <typename K, typename V, typename CMP>
int BPTree<K, V, CMP>::_write_empty_to_file(FILE* file, size_t size) {
    static char buffer[1000000];
    static bool first = true;
    if (first) {
        memset(buffer, 0, 1000000);
        first = false;
    }

    if (wtfwrite(buffer, size, 1, file) != 1) {
        return -1;
    }
    return size;
}

template <typename K, typename V, typename CMP>
bool BPTree<K, V, CMP>::_is_head(void* pos) {
    return _f_father(pos) == 0;
}

template <typename K, typename V, typename CMP>
uint64_t BPTree<K, V, CMP>::_create_floor(uint64_t father) {
    static char buffer[1000000];
    static bool first = true;
    if (first) {
        memset(buffer, 0, 1000000);
        first = false;
    }

    if (fseeko64(_file, 0, SEEK_END) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    _f_father(buffer) = father;
    if (wtfwrite(buffer, _floor_size, 1, _file) != 1) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }

    return ftello64(_file) - _floor_size;
}

template <typename K, typename V, typename CMP>
uint64_t BPTree<K, V, CMP>::_create_leaf(uint64_t father) {
    static char buffer[1000000];
    static bool first = true;
    if (first) {
        memset(buffer, 0, 1000000);
        first = false;
    }

    if (fseeko64(_file, 0, SEEK_END) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    _leaf_father(buffer) = father;
    if (wtfwrite(buffer, _leaf_size, 1, _file) != 1) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }

    return ftello64(_file) - _leaf_size;
}

template <typename K, typename V, typename CMP>
uint64_t BPTree<K, V, CMP>::_create_value() {
    static char buffer[1000000];
    static bool first = true;
    if (first) {
        memset(buffer, 0, 1000000);
        first = false;
    }

    if (fseeko64(_file, 0, SEEK_END) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfwrite(buffer, _value_size, 1, _file) != 1) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }

    return ftello64(_file) - _value_size;
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_read_floor(void* buffer, uint64_t floor_pos) {
    if (fseeko64(_file, floor_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfread(buffer, _floor_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_write_floor(void* buffer, uint64_t floor_pos) {
    if (fseeko64(_file, floor_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfwrite(buffer, _floor_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_write_pointer_to_floor(uint64_t floor_pos, uint64_t pointer, uint32_t offset) {
    uint64_t real_pos = floor_pos + 16 + (sizeof(K) + 8) * offset;
    if (fseeko64(_file, real_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfwrite(&pointer, 8, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_read_leaf(void* buffer, uint64_t leaf_pos) {
     if (fseeko64(_file, leaf_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfread(buffer, _leaf_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_write_leaf(void* buffer, uint64_t leaf_pos) {
    if (fseeko64(_file, leaf_pos, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }
    if (wtfwrite(buffer, _leaf_size, 1, _file) != 1) {
        toscreen << "[ERROR]READ ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_push_element_to_leaf(void* leaf, const K& key, const V& val) {
    if (_leaf_used(leaf) >= LEAF_CAPACITY) {
        toscreen << "[ERROR]CAPACITY CALC ERROR.\n";
        sleep(1000);
    }
    uint32_t offset = 0;
    while (offset < _leaf_used(leaf) && CMP::compare(_key_at(leaf, offset), key) < 0) {
        ++offset;
    }
    if (debug) {
        toscreen << "Push ele to leaf at offset: " << offset << ".\n";
    }
    if (offset == _leaf_used(leaf) || CMP::compare(_key_at(leaf, offset), key) != 0) {
        if (debug) {
            toscreen << "PUSH TO NEW KEY.\n";
        }
        uint32_t off_val = 12 + (_value_size + sizeof(K)) * offset;
        wtmemcpy((leaf + off_val) + (_value_size + sizeof(K)),
            (leaf + off_val), _leaf_size - 8 - 12 - (_value_size + sizeof(K)) * (offset + 1));
        if (debug) {
            toscreen << "COPY KEY TO OFFVAL: " << off_val << ".\n";
        }
        memcpy(leaf + off_val, &key, sizeof(K));
        memcpy(leaf + off_val + sizeof(K) + 4, &val, sizeof(V));
        uint32_t used = 1;
        memcpy(leaf + off_val + sizeof(K), &used, sizeof(uint32_t));
        ++_leaf_used(leaf);
        return;
    }
    if (debug) {
        toscreen << "PUSH AT EXISTING KEY at offset: " << offset << " leaf used: " << _leaf_used(leaf) << ".\n";
    }
    uint32_t off_val = 12 + (_value_size + sizeof(K)) * offset + sizeof(K);
    uint32_t& val_used_num = *(uint32_t*)(leaf + off_val);
    ++val_used_num;
    if (val_used_num > VAL_CAPACITY) {
        static char buff[1000000];
        // Value node is full, expand the value node.
        if (val_used_num % VAL_CAPACITY != 1) {
            // Existing allocated space is not full.
            int32_t val_th = (val_used_num - 1) / VAL_CAPACITY;
            int64_t cur_next_pos = *(int64_t*)(leaf + off_val + 4 + sizeof(V) * VAL_CAPACITY);
            for (int k = 0; k < val_th - 1; ++k) {
                if (fseeko64(_file, cur_next_pos + 4 + sizeof(V) * VAL_CAPACITY, SEEK_SET) != 0) {
                    toscreen << "SEEK ERR.\n";
                    sleep(1000);
                }
                if (fread(&cur_next_pos, 8, 1, _file) != 1) {
                    toscreen << "READ ERR.\n";
                    sleep(1000);
                }
            }
            if (fseeko64(_file, cur_next_pos, SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
            if (fread(buff, _value_size, 1, _file) != 1) {
                toscreen << "READ ERR.\n";
                sleep(1000);
            }
            int32_t insert_pos = (val_used_num - 1) % VAL_CAPACITY;
            memcpy((buff + 4 + sizeof(V) * insert_pos), &val, sizeof(V));
            if (fseeko64(_file, cur_next_pos, SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
            if (fwrite(buff, _value_size, 1, _file) != 1) {
                toscreen << "WRITE ERR.\n";
                sleep(1000);
            }
            return;
        }
        // Existing allocated space is full.
        int64_t new_val_pos = _create_value();
        int32_t val_th = (val_used_num - 2) / VAL_CAPACITY;
        int64_t cur_next_pos = *(int64_t*)(leaf + off_val + 4 + sizeof(V) * VAL_CAPACITY);
        for (int k = 0; k < val_th - 1; ++k) {
            if (fseeko64(_file, cur_next_pos + 4 + sizeof(V) * VAL_CAPACITY, SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
            if (fread(&cur_next_pos, 8, 1, _file) != 1) {
                toscreen << "READ ERR.\n";
                sleep(1000);
            }
        }
        if (val_used_num == VAL_CAPACITY + 1) {
            // Is directly expand after the leaf.
            *(int64_t*)(leaf + off_val + 4 + sizeof(V) * VAL_CAPACITY) = new_val_pos;
        } else {
            if (fseeko64(_file, (cur_next_pos + 4 + sizeof(V) * VAL_CAPACITY), SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
            if (fwrite(&new_val_pos, 8, 1, _file) != 1) {
                toscreen << "SEEK ERR.\n";
                sleep(1000);
            }
        }
        memset(buff, 0, _value_size);
        memcpy(buff + 4, &val, sizeof(V));
        if (fseeko64(_file, new_val_pos, SEEK_SET) != 0) {
            toscreen << "SEEK ERR.\n";
            sleep(1000);
        }
        if (fwrite(buff, _value_size, 1, _file) != 1) {
            toscreen << "SEEK ERR.\n";
            sleep(1000);
        }
    } else {
        // Value node is not full, insert and return.
        off_val += 4;
        off_val += sizeof(V) * (*(uint32_t*)(leaf + off_val - 4) - 1);
        memcpy((leaf + off_val), &val, sizeof(V));
    } 
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::_set_father(uint64_t l_or_f, uint64_t father_pos) {
    static char buffer[100];
    if (fseeko64(_file, l_or_f, SEEK_SET) != 0) {
        toscreen << "[ERROR]SEEK ERROR.\n";
        sleep(1000);
    }

    *(uint64_t*)buffer = father_pos;
    if (wtfwrite(buffer, 8, 1, _file) != 1) {
        toscreen << "[ERROR]WRITE ERROR.\n";
        sleep(1000);
    }
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::print_structure(uint64_t pos, int type) {
    using namespace std;
    if (pos == 0) {
        pos = _head;
    }
    char buffer[10000];
    if (type == 0) {
        // Floor.
        _read_floor(buffer, pos);
        int is_leaf = _if_leaf(buffer);
        uint64_t father = _f_father(buffer);
        uint32_t used = _f_used(buffer);
        cout << "*********\n";
        cout << "FLOOR.\n";
        cout << "POS: " << pos << ".\n";
        cout << "IS_LEAF: " << is_leaf << ".\n";
        cout << "USED_NUM: " << used << ".\n";
        cout << "FATHER: " << father << ".\n";
        vector<uint64_t> next_to;
        for (uint32_t i = 0; i < used; ++i) {
            cout << i << ": " << _next_floor(buffer, i) << " " << _get_key(buffer, i) << "\n";
            next_to.push_back(_next_floor(buffer, i));
        }
        cout << "last: " << _next_floor(buffer, used) << "\n";
        next_to.push_back(_next_floor(buffer, used));
        for (size_t i = 0; i < next_to.size(); ++i) {
            if (is_leaf == 1) {
                print_structure(next_to[i], 0);
            } else {
                print_structure(next_to[i], 1);
            }
        }
        return;
    }
    if (type == 1) {
        _read_leaf(buffer, pos);
        cout << "*********\n";
        cout << "LEAF.\n";
        cout << "POS: " << pos << ".\n";
        cout << "FATHER: " << _leaf_father(buffer) << "\n";
        cout << "USED_NUM: " << _leaf_used(buffer) << "\n";
        for (size_t i = 0; i < _leaf_used(buffer); ++i) {
            cout << i << ": " << _key_at(buffer, i) << "\n";
        }
        for (size_t i = 0; i < _leaf_used(buffer); ++i) {
            print_structure(_val_at(buffer, i), i);
        }
        return;
    }
    
    return;
}

template <typename K, typename V, typename CMP>
void BPTree<K, V, CMP>::print_structure(void* val, uint64_t pos) {
    char* buffer = (char*)val;
    cout << "*********\n";
    cout << "VAL.\n";
    cout << "POS: " << pos << ".\n";
    cout << "USED_NUM: " << *(uint32_t*)(buffer) << "\n";
    cout << "NEXT VAL: " << *(int64_t*)(buffer + 4 + sizeof(V) * VAL_CAPACITY) << ".\n";
    for (size_t i = 0; i < VAL_CAPACITY; ++i) {
        cout << i << ": " << *(V*)(buffer + 4 + sizeof(V) * i) << "\n";
    }
    cout << "\n";
    if (*(int64_t*)(buffer + 4 + sizeof(V) * VAL_CAPACITY) != 0) {
        char b2[100000];
        if (fseeko64(_file, *(int64_t*)(buffer + 4 + sizeof(V) * VAL_CAPACITY), SEEK_SET) != 0 || 
            fread(b2, _value_size, 1, _file) != 1) {
            toscreen << "ERR.\n";
            sleep(1000);
        }
        print_structure(b2, pos);
    }
    return;
}

} // End namespace wt.
