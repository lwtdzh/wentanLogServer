/**
 * One HandleThread instance is one thread.
 * Include this file to use, you need complie the cpp file first.
 * Date: 2019/8/8.
 * Author: LiWentan.
 */

#ifndef _handlethreadh_
#define _handlethreadh_

#include "wttool.h"
#include "wtatomqueue.hpp"
#include "timeexpirequeue.h"

namespace wt {

using namespace std;

class HandleThread {
public:
    /**
     * Construction function.
     * @param dirpath: Logs will be saved to this directory.
     * @param print_queue: This thread will listen this queue, get request from this queue.
     */
    HandleThread(const string& dirpath, wt::WTAtomQueue<PReq>* print_queue);
    
    /**
     * When you start a search request, you need call this function.
     * This is a asynchronous call function, result and search status will be set to the input parameters.
     * @param req: The print request.
     * @param res: Result of search will be set to this string.
     * @param fin: Help you make sure when search is finished. When search is finished, this flag will be set true.
     */
    int search(SReq* req, string* res, atomic<bool>* fin);

private:
    string           _dp;   // Dir path.
    bool             _runs; // True mean working, if false, the thread will stop after this loop. 
    pthread_t        _h_t;  // Handle thread id.
    bool             _if_search;  // True to make thread handle a search request.
    string*          _search_res; // Push the search result there.
    atomic<bool>*    _search_fin; 
    SReq*            _search_req;
    int              _last_log_p;   // Next recent log position.
    int              _last_log_num; // How many recent logs have been saved in memory.
    char             _last_log[LAST_LOG_MAX][LOG_MAX_LEN + 13]; // Recent Logs content.
    wt::WTAtomQueue<PReq>* _p_queue;   // Print queue.
    FileTimeExpirePool     _file_pool; // File pool.
    wt::TimeExpireSet<string, std::unordered_map<int32_t, vector<int64_t> > * > _index; // Search indexes.

public: 
    /**
     * Get the file name, where logs at this time will be storaged.
     */
    static string _get_file(const string& time);

    /**
     * Return true if this year is a leap year.
     */
    static bool _if_yun(int year);

    /**
     * Return the next day. Format: yyyymmdd. 
     */
    static string _next_day(const string& date);
    
    /**
     * Get the file name after this file name.
     * E.g., if file name is 20191231_1440, the next file may be 20200101_0.
     */
    static string _get_next_file(const string& name);
    
    /**
     * Get the file name which storaging the logs with corresponding time.
     */
    static vector<string> _get_file(const string& start, const string& end);
    
private:
    /**
     * Get the search index for this file.
     * @param file_path: Get index for this file.
     * @param construct: If index is not existing, 1 to construct index and return, 0 to return nullptr.
     */
    std::unordered_map<int32_t, vector<int64_t> >* _get_index(const string& file_path, int construct = 1);
    
    /**
     * Thread function.
     */
    static void* handle_thread(void* args);
};

} // End Class HandleThread.

#endif
