/**
 * Some container structure used for time expiring.
 * Author: LiWentan.
 * Date: 2019/8/8.
 */

#ifndef _timeexpirequeuelwt_hpp_
#define _timeexpirequeuelwt_hpp_

#include "wttool.h"

namespace wt {

using namespace std;

/**
 * A container which storaging some elements.
 * All elements are ordered by last visited time.
 */
template <typename T>
class TimeExpireQueue {
private:
    template <typename Y>
    class Node {
        friend class TimeExpireQueue<Y>;
        Node(const Y& in) : _data(in), _next(nullptr), _former(nullptr) {}
        
        Node<Y>* _former;
        Node<Y>* _next;
        Y _data;
    };

public: 
    TimeExpireQueue() : _head(nullptr), _tail(nullptr) {}
    
    /**
     * Update this element, set the last visit time to current time.
     * If this element is not in the container, it will be added to this container.
     */
    int update(const T& in) {
        auto it = _values.find(in);
        if (it != _values.end()) {
            _move_to_newest(it->second);
            return 0;
        }
        
        Node<T>* new_node = new(std::nothrow) Node<T>(in);
        if (new_node == nullptr) {
            toscreen << "Memory full.\n";
            return -1;
        }
        _values[in] = new_node;
        if (_tail != nullptr) {
            _tail->_next = new_node;
            new_node->_former = _tail;
            _tail = _tail->_next;
            return 0;
        }
        _head = _tail = new_node;
        return 0;
    }
    
    /**
     * Expire the eariest visited element from this container.
     * @return The element eariest visited.
     */
    T expire() {
        if (_head == nullptr) {
            static T default_val;
            return default_val;
        }
        T res = _head->_data;
        _values.erase(res);
        Node<T>* cur_node = _head;
        if (_head != _tail) {
            _head = _head->_next;
            _head->_former = nullptr;
            delete cur_node;
            return res;
        }
        _head = _tail = nullptr;
        delete cur_node;
        return res;
    }
    
    /**
     * Return all elements from this container.
     * Ordered by the last visited time.
     */
    vector<T> show() {
        vector<T> res;
        if (_head == nullptr) {
            return res;
        }
        Node<T>* cur_node = _head;
        while (cur_node != nullptr) {
            res.push_back(cur_node->_data);
            cur_node = cur_node->_next;
        }
        return res;
    }
    
private:
    Node<T>* _head;
    Node<T>* _tail;
    std::map<T, Node<T>*> _values;
    
    void _move_to_newest(Node<T>* node) {
        if (node == _tail) {
            return;
        }
        _tail->_next = node;
        node->_next->_former = node->_former;
        if (node != _head) {
            node->_former->_next = node->_next;
        } else {
            _head = node->_next;
        }
        node->_next = nullptr;
        node->_former = _tail;
        _tail = node;
    }
};

/**
 * Help you to open many files.
 * You don't need to close the file, this container will help you to manage.
 */
class FileTimeExpirePool {
public:  
    /**
     * @param path: The root path. All file will be searched here.
     * @param max_num: If more files than this number opened, it will close the eariest visited file.
     */
    FileTimeExpirePool(const string& path = ".", int max_num = 100) : _capacity(max_num), _dir(path) {}
    
    /**
     * Open a file from the root path. Return the pointer.
     * Mode 1: Pos to end, can create file. 
     * Mode 0: Pos to begin, cannot create file. 
     * Mode 2: Pos to target, cannot create file.
     */
    FILE* open(string file_name, int mode = 1, int64_t pos = 0) {
        file_name = _dir + "/" + file_name;
        
        auto it = _data.find(file_name);
        if (it != _data.end()) {
            if (_t_record.update(file_name) != 0) {
                toscreen << "CHECK ERROR.\n";
                sleep(1000);
            }
            if (mode == 1) {
                if (fseeko64(it->second, 0, SEEK_END) != 0) {
                    toscreen << "seek error: " << file_name << ".\n";
                    fclose(it->second);
                    return nullptr;
                }
            } else if (mode == 0) {
                if (fseeko64(it->second, 0, SEEK_SET) != 0) {
                    toscreen << "seek error: " << file_name << ".\n";
                    fclose(it->second);
                    return nullptr;
                }
            } else if (mode == 2) {
                if (fseeko64(it->second, pos, SEEK_SET) != 0) {
                    toscreen << "seek error: " << file_name << ".\n";
                    fclose(it->second);
                    return nullptr;
                }
            }
            return it->second;
        }

        FILE* file = fopen(file_name.c_str(), "r+b");
        if (file == nullptr) {
            if (mode != 1) {
                return nullptr;
            }
            file = fopen(file_name.c_str(), "wb");
            if (file == nullptr) {
                toscreen << "OPEN FILE FAILED:" << file_name << ".\n";
                return nullptr;
            }
            fclose(file);
            file = fopen(file_name.c_str(), "r+b");
            if (file == nullptr) {
                toscreen << "OPEN FILE FAILED:" << file_name << ".\n";
                return nullptr;
            }
        }
        
        if (mode == 1) {
            if (fseeko64(file, 0, SEEK_END) != 0) {
                toscreen << "seek error: " << file_name << ".\n";
                fclose(file);
                return nullptr;
            }
        } else if (mode == 0) {
            if (fseeko64(file, 0, SEEK_SET) != 0) {
                toscreen << "seek error: " << file_name << ".\n";
                fclose(file);
                return nullptr;
            }
        } else if (mode == 2) {
            if (fseeko64(file, pos, SEEK_SET) != 0) {
                toscreen << "seek error: " << file_name << ".\n";
                fclose(file);
                return nullptr;
            }
        }

        if (_capacity == _data.size()) {
            string last = _t_record.expire();
            auto it_exp = _data.find(last);
            if (it_exp == _data.end()) {
                toscreen << "EXPIRE QUEUE AND MAP UNMATCHED.\n";
                sleep(1000);
            }
            
            fclose(it_exp->second);
            _data.erase(it_exp);
        }
        
        _data[file_name] = file;
        _t_record.update(file_name);
        
        return file;
        
    }

private:
    int     _capacity;
    string  _dir;
    std::map<string, FILE*> _data;
    TimeExpireQueue<string> _t_record;
    
}; // End Class FileTimeExpirePool.

/**
 * A set to help you manage a elements(K-V)' pool by time.
 */
template <typename K, typename V>
class TimeExpireSet {
public:
    /**
     * @param capacity: If more elements are pushed, this container will refused to push.
     */
    TimeExpireSet(size_t capacity) : _capacity(capacity) {}
    
    /**
     * Push a K-V or update the K-V's last visited time to newest.
     * If K is existing, value must be corresponding to this key, or return error.
     */
    int update(const K& key, const V& val) {
        auto it = _data.find(key);
        if (it != _data.cend()) {
            if (it->second == val) {
                return _keys.update(key);
            }
            toscreen << "UNMATCHED KEY AND VAL.\n";
            sleep(1000);
            return -2;
        }
        
        if (_data.size() == _capacity) {
            return 1;
        }
        
        if (_keys.update(key) != 0) {
            toscreen << "ERROR.\n";
            sleep(1000);
            return -3;
        }
        _data[key] = val;
        return 0;
    }
    
    /**
     * Expire the eariest visited K-V from the container.
     * Will write the expired K-V to the parameters.
     */
    int expire(K* k_out = nullptr, V* v_out = nullptr) {
        if (_data.size() == 0) {
            // No data.
            return -1;
        }
        K k = _keys.expire();
        if (k_out != nullptr) {
            *k_out = k;
        }
        auto it = _data.find(k);
        if (it == _data.end()) {
            toscreen << "ERROR.\n";
            sleep(1000);
        }
        if (v_out != nullptr) {
            *v_out = it->second;
        }
        _data.erase(it);
        return 0;
    }
    
    /**
     * Find a value by a key.
     * Won't update the time of this key.
     * If want to update time, you need use function: update.
     */
    int find(const K& in, V* v_out) {
        auto it = _data.find(in);
        if (it == _data.cend()) {
            return -1;
        }
        if (v_out != nullptr) {
            *v_out = it->second;
        }
        return 0;
    }
    
private:
    size_t                   _capacity;
    TimeExpireQueue<K>       _keys;
    std::unordered_map<K, V> _data;
    
}; // End Class TimeExpireSet.

} // End namespace wt;

#endif 
