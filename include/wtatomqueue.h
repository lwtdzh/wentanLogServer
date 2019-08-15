/**
 * A multi-thread safe queue.
 * Do not include this file, include HPP file instead.
 * Date: 2019/8/8.
 * Author: LiWentan.
 */

#ifndef _WTATOM_QUEUE_HPP_
#define _WTATOM_QUEUE_HPP_

#include <unistd.h>
#include <pthread.h>
#include <iostream>

#include "wttool.h"

namespace wt {

template <typename T>
class WTAtomQueue {
private:
    template <typename Y>
    class Node {
        friend class WTAtomQueue<Y>;
        Node(const Y& in) : next(NULL), former(NULL), data(in) {}
        Node* next;
        Node* former;
        Y data;
    };
    
public:
    /**
     * Construct and Disturct methods.
     */
    WTAtomQueue();
    virtual ~WTAtomQueue();
    
    /**
     * Push a element to the tail of the queue.
     */
    int push(const T& in);
    
    /**
     * Get the top element, and remove the top element from the queue.
     * @return 0 is OK, -1 is empty queue.
     */
    int pop(T* out);
    
    /**
     * Get the elements' size.
     */
    size_t size();
      
private:
    Node<T>* _head;
    Node<T>* _tail;
    size_t   _length;
    pthread_mutex_t _locker;

    /**
     * Lock or unlock safely.
     */
    static void _lock(pthread_mutex_t* lock);
    static void _lockr(pthread_rwlock_t* lock);
    static void _lockw(pthread_rwlock_t* lock);
    static void _unlock(pthread_mutex_t* lock);
    static void _unlock(pthread_rwlock_t* lock);
};    
  
} // End namespace wt.

#endif
