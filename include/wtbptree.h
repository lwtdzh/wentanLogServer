/**
 * Wentan B+ Tree. Head file. Only for WT log system. Cannot be used for other system.
 * Do not include this file. Include HPP file.
 * Date: 2019/7/29.
 * Author: LiWentan.
 */

#ifndef WTBPTREE_H_
#define WTBPTREE_H_

#include <unordered_map>
#include <pthread.h>
#include <ctime>
#ifdef linux
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#endif
#include <sys/time.h>
#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <fcntl.h>
#include <utility>
#include <map>

#ifndef toscreen
#define toscreen std::cout<<__FILE__<<", "<<__LINE__<<": "
#endif

using std::string;

namespace wt {

class Compare {
public:
    /**
     * @return < 0, lhs < rhs.
     * @return = 0, lhs = rhs.
     * @return > 0, lhs > rhs.
     */
    static int compare(const int& lhs, const int& rhs);
    static int compare(const short& lhs, const short& rhs);
    static int compare(const int64_t& lhs, const int64_t& rhs);
    static int compare(const string& lhs, const string& rhs);
}; // End class Compare.

template <typename K, typename V, typename CMP = Compare>
class BPTree {
public:
    /**
     * Constructive function.
     */
    BPTree();
    ~BPTree() {if (_file != NULL) {fclose(_file);}}

    /**
     * Initialize.
     * @param file_path: The file storaging the BP Tree.
     * @param floor_num: The maximum number of elements of one node.
     * @return False means failed.
     */
    bool init(const string& file_path = "bptree", uint32_t floor_num = 10000);

    /**
     * Insert.
     * @return true: Success.
     */
    bool insert(const K& key, const V& val);

    /**
     * Search.
     * @return The number of elements.
     */
    int search(const K& key, std::vector<V>* val);

    void print_structure(uint64_t pos = 0, int type = 0);
    void print_structure(void* val, uint64_t type);

private:
    FILE* _file; // The file pointer to BPTree.
    uint32_t _f_num; // The maximum number of elements of one node.
    uint64_t _size; // Size of elements.
    uint64_t _head; // Position of head node.

private:
    uint64_t _floor_size; // The reserved size of one floor.
    uint64_t _value_size;
    uint64_t _leaf_size;

private:
    /**
     * Return the written bytes.
     */
    static int _write_empty_to_file(FILE* file, size_t size);

    /**
     *
     */
    bool _is_head(void* pos);

    /**
     * Get position of target key's father position.
     */
    void _get_pos(const K& key, uint64_t* floor_pos, uint32_t* offset);

    /**
     * Create an empty Floor. Return the position.
     */
    uint64_t _create_floor(uint64_t father);

    /**
     * Create an empty Leaf. Return the position.
     */
    uint64_t _create_leaf(uint64_t father = 0);

    /**
     * Create an empty extern Value. Return the position.
     */
    uint64_t _create_value();

    /**
     * Get the elements of a floor in memory.
     * @param node: The address of this floor in memory.
     */
    int32_t& _if_leaf(void* floor) {return *(int32_t*)(floor + 8);}
    K& _get_key(void* floor, uint32_t offset) {return *(K*)(floor + 16 + (8 + sizeof(K)) * offset + 8);}
    uint64_t& _next_floor(void* floor, uint32_t offset) {return *(uint64_t*)(floor + 16 + (8 + sizeof(K)) * offset);}
    uint32_t& _f_used(void* floor) {return *(uint32_t*)(floor + 12);}
    uint64_t& _f_father(void* floor) {return *(uint64_t*)(floor);}

    /**
     *
     */
    uint32_t& _leaf_used(void* leaf) {return *(uint32_t*)(leaf + 8);}
    uint64_t& _leaf_father(void* leaf) {return *(uint64_t*)(leaf);}
    K& _key_at(void* leaf, uint32_t offset) {return *(K*)(leaf + 12 + (_value_size + sizeof(K)) * offset);}
    void* _val_at(void* leaf, uint32_t offset) {return (leaf + 12 + (_value_size + sizeof(K)) * offset + sizeof(K));}
    uint64_t& _next_leaf(void* leaf) {return *(uint64_t*)(leaf + _leaf_size - 8);}
    
    /**
     * Read, write the floor.
     */
    void _read_floor(void* buffer, uint64_t floor_pos);
    void _write_floor(void* buffer, uint64_t floor_pos);
    void _write_pointer_to_floor(uint64_t floor_pos, uint64_t pointer, uint32_t offset);



    /**
     * Read the leaf.
     */
    void _read_leaf(void* buffer, uint64_t leaf_pos);
    void _write_leaf(void* buffer, uint64_t leaf_pos);

    /**
     * 
     */
    void _push_element_to_leaf(void* leaf, const K& key, const V& val);

    /**
     *
     */
    void _set_father(uint64_t l_or_f, uint64_t father_pos);

    /**
     * @return 0 Success.
     */
    int _seek_and_read(FILE* file, void* buff, size_t size);
    
}; // End class BPTree.

} // End namespace wt.

#endif
