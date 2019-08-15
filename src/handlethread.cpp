/**
 * One HandleThread instance is one thread.
 * Date: 2019/8/8.
 * Author: LiWentan.
 */
 
#include "handlethread.h"

using namespace wt;

int HandleThread::search(SReq* req, string* res, atomic<bool>* fin) {
    while (_if_search) {
        toscreen << "Last is searching, wait...\n";
        usleep(1e5);
    }
    _search_fin = fin;
    _search_res = res;
    _search_req = req;
    _if_search = true;
    return 0;
}

HandleThread::HandleThread(const string& dirpath, wt::WTAtomQueue<PReq>* print_queue) :
    _dp(dirpath), _file_pool(dirpath), _index(INDEX_CACHE_NUM), _last_log_p(-1), _last_log_num(0)  {
    _if_search = false;
    _runs = true;
    _p_queue = print_queue;
    if (pthread_create(&_h_t, nullptr, handle_thread, this) != 0) {
        toscreen << "Thread create failed.\n";
        _h_t = -1;
    }
}

string HandleThread::_get_file(const string& time) {
    int hour = str2num(string(&time[8], 2));
    int min = str2num(string(&time[10], 2));
    int sec = str2num(string(&time[12], 2));
    int second = hour * 3600 + min * 60 + sec;
    int order = second / (86400 / SPLIT_DAY);
    return string(&time[0], 8) + "_" + num2str(order);
}

bool HandleThread::_if_yun(int year) {
    if (year % 100 == 0 && year % 400 == 0) {
        return true;
    }
    if (year % 4 == 0 && year % 100 != 0) {
        return true;
    }
    return false;
}

string HandleThread::_next_day(const string& date) {
    int year = str2num(string(&date[0], 4));
    int mon = str2num(string(&date[4], 2));
    int day = str2num(string(&date[6], 2));
    if (day < 28) {
        ++day;
    } else {
        // Day >= 28.
        if (mon != 2 && day < 30) {
            ++day;
        } else if (mon == 2) {
            if (_if_yun(year) && day == 29) {
                day = 1;
                ++mon;
            } else if (_if_yun(year) && day == 28){
                ++day;
            } else {
                day = 1;
                ++mon;
            }
        } else {
            if (mon == 1 || mon == 3 || mon == 5 || mon == 7 || mon == 8 || mon == 10 || mon == 12) {
                if (day == 30) {
                    ++day;
                } else {
                    day = 1;
                    ++mon;
                }
            } else {
                day = 1;
                ++mon;
            }
        }
    } // FIN.
    if (mon == 13) {
        ++year;
        mon = 1;
    }
    char res[9];
    sprintf(res, "%04d%02d%02d", year, mon, day);
    return res;
}

string HandleThread::_get_next_file(const string& name) {
    int max_day = SPLIT_DAY - 1;
    string date = string(&name[0], 8);
    int order = str2num(splitstr(name, "_")[1]);
    if (order == max_day) {
        // Day + 1;
        return _next_day(date) + "_0";
    }
    return date + "_" + num2str(order + 1);
}

vector<string> HandleThread::_get_file(const string& start, const string& end) {
    vector<string> res;
    if (start > end) {
        return res;
    }
    res.push_back(_get_file(start));
    res.push_back(_get_file(end));
    if (res[0] == res[1]) {
        res.erase(res.begin() + 1);
        return res;
    }
    string last_file = res[0];
    while (true) {
        last_file = _get_next_file(last_file);
        if (last_file == res[1]) {
            break;
        }
        res.push_back(last_file);
    }
    return res;
}

std::unordered_map<int32_t, vector<int64_t> >* HandleThread::_get_index(const string& file_path, int construct) {
    std::unordered_map<int32_t, vector<int64_t> >* val;
    if (_index.find(file_path, &val) == 0) {
        return val;
    }
    if (construct == 0) {
        // No construct for new file.
        return nullptr;
    }
    // Construct index.
    FILE* f = _file_pool.open(file_path, 0);
    if (f == nullptr) {
        // FILE UNEXISTING.
        return nullptr;
    }
    
    val = new(std::nothrow) std::unordered_map<int32_t, vector<int64_t> >();
    if (val == nullptr) {
        toscreen << "ERROR.\n";
        sleep(1000);
    }
    if (_index.update(file_path, val) == 1) { // Expire the last.
        std::unordered_map<int32_t, vector<int64_t> >* v_exp;
        if (_index.expire(nullptr, &v_exp) != 0) {
            toscreen << "ERROR.\n";
            sleep(1000);
        }
        delete v_exp;
        if (_index.update(file_path, val) == 1) {
            toscreen << "ERROR.\n";
            sleep(1000);
        }
    }
    char buffer[LOG_MAX_LEN + 13];
    while (fread(buffer, LOG_MAX_LEN + 13, 1, f) == 1) {
        int32_t& key = *(int32_t*)(buffer + 8);
        vector<int64_t>& i_c = (*val)[key];
        i_c.push_back(ftello64(f) - LOG_MAX_LEN - 13);
    }
    return val;
}

void* HandleThread::handle_thread(void* args) {
    HandleThread* t = (HandleThread*)args;
    PReq preq;
    int retry = 0;

    while (t->_runs) {
        if (t->_if_search) {
            TimeStatistic tt;
            if (t->_search_req->type == 0) { // Last N log.
                if (t->_last_log_p == -1) { // No content.
                    *t->_search_fin = true;
                    t->_if_search = false;
                    continue;
                }
                if (t->_search_req->key > LAST_LOG_MAX) {
                    t->_search_req->key = LAST_LOG_MAX;
                }
                int cur_pos = t->_last_log_p;
                stringstream ss;     
                for (int i = 0; i < t->_search_req->key && i < t->_last_log_num; ++i) {
                    ss << i << ", ";
                    ss << "Time: ";
                    ss << *(int64_t*)t->_last_log[cur_pos] << ", Key: ";
                    ss << *(int32_t*)(t->_last_log[cur_pos] + 8) << ", Content: ";
                    ss << (char*)(t->_last_log[cur_pos] + 12) << ".\n";
                    --cur_pos;
                    if (cur_pos == -1) {
                        cur_pos = LAST_LOG_MAX - 1;
                    }
                    
                }
                ss << "Time cost micosecond: " << tt.cost_us() << endl;
                *t->_search_res = ss.str();
                *t->_search_fin = true;
                t->_if_search = false;
                continue;
            }

            if (t->_search_req->type == 1) { // By time, key and content.
                TimeStatistic tt;
                stringstream ss;
                char buffer[LOG_MAX_LEN + 13];
                vector<string> files = _get_file(t->_search_req->start_time, t->_search_req->end_time);
                toscreen << "Search_key: " << t->_search_req->key << ".\n";
                toscreen << "Search content: " << t->_search_req->content << ".\n";
                toscreen << "Search begin time: " << t->_search_req->start_time << ".\n";
                toscreen << "Search end time: " << t->_search_req->end_time << ".\n";
                for (size_t i = 0; i < files.size(); ++i) {
                    if (t->_search_req->key == 0) { // All log.
                        FILE* f = t->_file_pool.open(files[i], 0);
                        if (f == nullptr) {
                            continue;
                        }
                        while (fread(buffer, LOG_MAX_LEN + 13, 1, f) == 1) {
                            int64_t& time = *(int64_t*)buffer;
                            if (time < str2num(t->_search_req->start_time) || time > str2num(t->_search_req->end_time)) {
                                continue;
                            }
                            if (t->_search_req->content != "") {
                                string con = string(buffer + 12);
                                if (con.find(t->_search_req->content) == string::npos) {
                                    continue;
                                }
                            }
                            ss << i << ", ";
                            ss << "Time: ";
                            ss << *(int64_t*)buffer << ", Key: ";
                            ss << *(int32_t*)(buffer + 8) << ", Content: ";
                            ss << (char*)(buffer + 12) << ".\n";
                        }
                    } else { // Have key.
                        auto index = t->_get_index(files[i]);
                        /*if (files[i] == "20190802_1") {
                            for (auto it = index->begin(); it != index->end(); ++it) {
                                toscreen << "Key: " << it->first << endl;
                            }
                        }*/
                        if (index == nullptr) { // Do not have this file.
                            continue;
                        }
                        auto it = index->find(t->_search_req->key);
                        if (it == index->cend()) { // This file do not have the key.
                            continue;
                        }
                        vector<int64_t>& poses = it->second;
                        for (size_t j = 0; j < poses.size(); ++j) {
                            FILE* f = t->_file_pool.open(files[i], 2, poses[j]);
                            if (f == nullptr) {
                                toscreen << "ERROR.\n";
                                sleep(1000);
                            }
                            if (fread(buffer, LOG_MAX_LEN + 13, 1, f) != 1) {
                                toscreen << "ERROR.\n";
                                toscreen << "File name: " << files[i] << endl;
                                toscreen << "Pos: " << poses[j] << endl;
                                toscreen << "File pos: " << ftello64(f) << endl;
                                sleep(1000);
                            }
                            int64_t& time = *(int64_t*)buffer;
                            if (time < str2num(t->_search_req->start_time) || time > str2num(t->_search_req->end_time)) {
                                continue;
                            }
                            if (t->_search_req->content != "") {
                                string con = string(buffer + 12);
                                if (con.find(t->_search_req->content) == string::npos) {
                                    continue;
                                }
                            }
                            ss << i << ", ";
                            ss << "Time: ";
                            ss << *(int64_t*)buffer << ", Key: ";
                            ss << *(int32_t*)(buffer + 8) << ", Content: ";
                            ss << (char*)(buffer + 12) << ".\n";
                        }
                    }
                    
                }
                ss << "Time cost micosecond: " << tt.cost_us() << endl;
                *t->_search_res = ss.str();
                *t->_search_fin = true;
                t->_if_search = false;
                continue;
            }

            continue;
        } // if_search.

        // If write.
        if (retry >= 30) {
            usleep(2e5);
            retry = 0;
        }
        if (t->_p_queue->pop(&preq) != 0) {
            ++retry;
            continue;
        }

        string times = cur_time();
        int64_t time_num = str2num(times);
        string to_file_str = _get_file(times);
        FILE* to_file = t->_file_pool.open(to_file_str);
        if (to_file == nullptr) {
            toscreen << "OPEN FILE FAILED: " << to_file_str << ".\n";
        }
        t->_last_log_p = (t->_last_log_p + 1) % LAST_LOG_MAX;
        t->_last_log_num = (t->_last_log_num > 1000) ? 1000 : t->_last_log_num + 1;
        char* buffer = t->_last_log[t->_last_log_p];
        memcpy(buffer, &time_num, 8); // time.
        memcpy(buffer + 8, preq.content, 4); // key.
        strncpy(buffer + 12, preq.content + 4, LOG_MAX_LEN + 1); // log content.
        if (fwrite(buffer, LOG_MAX_LEN + 13, 1, to_file) != 1) {
            toscreen << "Write error: " << to_file_str << ".\n";
        }
        auto index_this_file = t->_get_index(to_file_str, 0);
        if (index_this_file != nullptr) {
            // Have index, push this log to the index.
            int32_t& key = *(int32_t*)(buffer + 8);
            (*index_this_file)[key].push_back(ftello64(to_file) - LOG_MAX_LEN - 13);
        }
    } // runs.
}