/**
 * A multi-thread safe queue.
 * If using, include this file.
 * Date: 2019/8/8.
 * Author: LiWentan.
 */

#include "wtatomqueue.h"

namespace wt {

template <typename T>
WTAtomQueue<T>::WTAtomQueue(): _head(NULL), _tail(NULL), _length(0) {
    if (pthread_mutex_init(&_locker, NULL) != 0) {
        toscreen << "INIT WTAtomQueue FAILED.\n";
        exit(0);
    }
}
    
template <typename T>
WTAtomQueue<T>::~WTAtomQueue() {
    _lock(&_locker);
    if (_head != NULL) {
        Node<T>* cur_node = _head;
        while (cur_node != NULL) {
            Node<T>* next_node = cur_node->next;
            delete cur_node;
            cur_node = next_node;
        }
    }
    _unlock(&_locker);
}  
    
template <typename T>  
void WTAtomQueue<T>::_lock(pthread_mutex_t* lock) {
    int ret = pthread_mutex_lock(lock);
    if (ret != 0) {
        toscreen << "Lock mutux_lock failed, try again after 1 sec. "
            << "The error code is " << ret << ".\n";
        sleep(1);
        ret = pthread_mutex_lock(lock);
    }
    return;
}

template <typename T>  
void WTAtomQueue<T>::_lockr(pthread_rwlock_t* lock) {
    int ret = pthread_rwlock_rdlock(lock);
    while (ret != 0) {
        toscreen << "Lock read_lock failed, try again after 1 sec. "
            << "The error code is " << ret << ".\n";
        sleep(1);
        ret = pthread_rwlock_rdlock(lock);
    }
    return;
}

template <typename T>  
void WTAtomQueue<T>::_lockw(pthread_rwlock_t* lock) {
    int ret = pthread_rwlock_wrlock(lock);
    while (ret != 0) {
        toscreen << "Lock write_lock failed, try again after 1 sec. "
            << "The error code is " << ret << ".\n";
        sleep(1);
        ret = pthread_rwlock_wrlock(lock);
    }
    return;
}

template <typename T>  
void WTAtomQueue<T>::_unlock(pthread_mutex_t* lock) {
    int ret = pthread_mutex_unlock(lock);
    while (ret != 0) {
        toscreen << "Unlock mutux_lock failed, try again after 1 sec. "
            << "The error code is " << ret << ".\n";
        sleep(1);
        ret = pthread_mutex_unlock(lock);
    }
    return;
}

template <typename T>  
void WTAtomQueue<T>::_unlock(pthread_rwlock_t* lock) {
    int ret = pthread_rwlock_unlock(lock);
    while (ret != 0) {
        toscreen << "Unlock rw_lock failed, try again after 1 sec. "
            << "The error code is " << ret << ".\n";
        sleep(1);
        ret = pthread_rwlock_unlock(lock);
    }
    return;
}

template <typename T>  
int WTAtomQueue<T>::push(const T& in) {
    Node<T>* new_node = new(std::nothrow) Node<T>(in);
    if (new_node == NULL) {
        toscreen << "[WARNING]Memory is not enough.\n";
        return -1;
    }
    
    _lock(&_locker);
    
    if (_tail != NULL) {
        // Queue has elements.
        _tail->next = new_node;
        new_node->former = _tail;
        _tail = new_node;
        ++_length;
        _unlock(&_locker);
        return 0;
    }
    
    // Queue is of no element.
    _head = _tail = new_node;
    ++_length;
    _unlock(&_locker);
    return 0;
}

template <typename T>  
int WTAtomQueue<T>::pop(T* out) {
    Node<T>* top_node = NULL;
    _lock(&_locker);
    if (_head == NULL) {
        // No element.
        _unlock(&_locker);
        return -1;
    }
    
    // Have element.
    top_node = _head;
    if (_length == 1) {
        // Last element.
        _head = _tail = NULL;
    } else {
        _head = _head->next;
    }
    --_length;
    _unlock(&_locker);
    
    *out = top_node->data;
    delete top_node;
    return 0;
}

template <typename T>  
size_t WTAtomQueue<T>::size() {
    size_t res;
    // _lock(&_locker);
    res = _length;
    // _unlock(&_locker);
    return res;
}
    
} // End namespace wt;
