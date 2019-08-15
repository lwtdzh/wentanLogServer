/**
 * Tools for WT Log system.
 * Only for WT Log system.
 * Other system may not be supported.
 * Author: LiWentan.
 * Date: 2019/8/8.
 */

#ifndef _wttool_h_
#define _wttool_h_

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
#include <string.h>
#include <atomic>
#include <iostream>

#define toscreen std::cout<<__FILE__<<", ht"<<__LINE__<<": "
#define LOG_MAX_LEN      50   // Logs must be shorter than this length (Unit: Byte).
#define SPLIT_DAY        1440 // Split a day's log to this number of files (Default 1440 mean a file, one minite).
#define LAST_LOG_MAX     1000 // How many recent logs will be storaged in memory for search.
#define INDEX_CACHE_NUM  450  // How many search index (one for a file) will be storaged in memory.

namespace wt {

using namespace std;

/**
 * This is the request for printing log.
 * You must construct this structure and push it to print_queue.
 * HandleThread instance will get this structure from print_queue and works.
 */
struct PReq {
    PReq& operator=(const PReq& rhs) {
        memcpy(content, rhs.content, LOG_MAX_LEN + 5);
    }
    PReq(const PReq& rhs) {
        memcpy(content, rhs.content, LOG_MAX_LEN + 5);
    }
    PReq() {
        memset(content, 0, LOG_MAX_LEN + 5);
    }
    char content[LOG_MAX_LEN + 5];
};

/**
 * This is the request for searching log.
 * You must construct this structure and call the search function of HandleThread.
 */
struct SReq {
    // Type 0: Search last N log, key is the number.
    // Type 1: Search log by key, content and time.
    SReq() {
        type = -1;
    }
    int type;
    string start_time;
    string end_time;
    int32_t key;
    string content;
    int socket;
};

static vector<string> splitstr(const string& str, const string& token) {
    vector<string> res;
    size_t next_pos = 0;
    while (true) {
        size_t found = str.find(token, next_pos);
        if (found == string::npos) {
            break;
        }
        if (found == next_pos) {
            next_pos += token.size();
            continue;
        }
        res.push_back(string(str, next_pos, found - next_pos));
        next_pos = found + token.size();
    }
    if (next_pos < str.size()) {
        res.push_back(string(str, next_pos, str.size() - next_pos));
    }
    return res;
}

static int64_t str2num(const string& str) {
    int64_t num;
    sscanf(str.c_str(), "%ld", &num);
    return num;
}

static string num2str(int64_t num) {
    char str[21];
    sprintf(str, "%ld", num);
    return str;
}

static string cur_time() {
    time_t cur_sec;
    std::time(&cur_sec);
    tm local_time;
    localtime_r(&cur_sec, &local_time);
    char res[15];
    sprintf(res, "%04d%02d%02d%02d%02d%02d", local_time.tm_year + 1900,
        local_time.tm_mon + 1, local_time.tm_mday, local_time.tm_hour,
        local_time.tm_min, local_time.tm_sec);
    return res;
}

static void write_to_socket(const char* buf, size_t size, int fd)
{
    int nwrite = 0;
    int data_size = strlen(buf);
    int n = data_size;
    while (n > 0)
    {
        nwrite = write(fd, buf + data_size - n, n);
        if (nwrite < n)
        {
            if (nwrite == -1 && errno != EAGAIN)
            {
                toscreen << "Write error.\n";
                sleep(1000);
            }
            break;
        }
        n -= nwrite;
    }
}

/** 
 * Parse the parameters of a request.
 * Only for GET request. No POST. 
 * @param msg: The binary data of a HTTP GET request.
 */
static std::map<string, string> get_msg_params(const string& msg) {
    map<string, string> p_map;
    size_t start_pos = msg.find("/?");
    if (start_pos == string::npos) {
        return p_map;
    }
    start_pos += 2;
    if (start_pos > msg.size()) {
        return p_map;
    }
    size_t end_pos = msg.find(" HTTP/1.1");
    if (end_pos == string::npos) {
        return p_map;
    }
    string content(msg, start_pos, (end_pos - start_pos));
    vector<string> params = splitstr(content, "&");

    for(size_t i = 0; i < params.size(); ++i) {
        vector<string> c = splitstr(params[i], "=");
        if (c.size() != 2) {
            continue;
        }
        p_map[c[0]] = c[1];
    }
    return p_map;
}

static string print_map(const std::map<string, string>& in) {
    auto it = in.cbegin();
    stringstream ss;
    while (it != in.cend()) {
        ss << "[" << it->first << "]:[" << it->second << "]\n";
        ++it;
    }
    return ss.str();
}

static PReq parse_to_preq(const std::map<string, string>& params) {
    PReq res;
    auto it = params.find("action");
    if (it == params.cend()) {
        toscreen << "Parse error.\n";
        return res;
    }
    if (it->second != "push") {
        toscreen << "Parse error.\n";
        return res;
    }
    try {
        const string& con = params.at("content");
        memcpy(res.content + 4, con.c_str(), con.size());
        memcpy(res.content + 4 + con.size(), "\0", 1);
        int32_t key = str2num(params.at("key"));
        memcpy(res.content, &key, 4);
    } catch (exception& e) {
        toscreen << "Parse error. msg: " << e.what() << ".\n";
        toscreen << print_map(params) << endl;
        res = PReq();
    }
    return res;
}

static SReq parse_to_sreq(const std::map<string, string>& params) {
    SReq req;
    try {
        req.type = str2num(params.at("type"));
        if (req.type == 0) {
            req.key = str2num(params.at("num"));
            return req;
        }
        if (req.type != 1) {
            throw nullptr;
        } 
        req.start_time = params.at("stime");
        req.end_time = params.at("etime");
        req.key = str2num(params.at("key"));
        auto it_con = params.find("content");
        if (it_con != params.cend()) {
            req.content = it_con->second;
        } else {
            req.content = "";
        }
        
    } catch (...) {
        toscreen << "Parse to sreq failed.\n";
        req.type = -1;
    }
    return req;
}

static string build_res(const string& content) {
    char* buffer = new char[content.size() + 45];
    sprintf(buffer, "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n", content.size());
    int head_len = strlen(buffer);
    memcpy(buffer + head_len, content.c_str(), content.size());
    memcpy(buffer + head_len + content.size(), "\0", 1);
    string res(buffer);
    delete buffer;
    return res;
}

static string build_req(const std::map<string, string>& params) {
    stringstream ss;
    ss << "/?";
    for (auto it = params.cbegin(); it != params.cend(); ++it) {
        ss << it->first << "=" << it->second;
        if (it != params.cend()) {
            ss << "&";
        }
    }
    ss << " HTTP/1.1";
    return ss.str();
}

class TimeStatistic {
public:
    TimeStatistic() {
        begin();
    }
    void begin() {
        gettimeofday(&_begin_call, NULL);
    }

    void end() {
        gettimeofday(&_end_call, NULL);
    }

    uint32_t cost_ms() {
        end();
        return (_end_call.tv_sec - _begin_call.tv_sec) * 1000 + 
                (_end_call.tv_usec - _begin_call.tv_usec) / 1000;
    }
    
    uint32_t cost_us() {
        end();
        return (_end_call.tv_sec - _begin_call.tv_sec) * 1e6 + 
                (_end_call.tv_usec - _begin_call.tv_usec);
    }

private:
    struct timeval _begin_call;
    struct timeval _end_call;
};

static string trimstr(const string& str, const string& trim_char = "\n\r\t ") {
    string res = str;
    for (size_t i = 0; i < trim_char.size(); ++i) {
        char char_i[2];
        memcpy(char_i, &trim_char[i], 1);
        memcpy(char_i + 1, "\0", 1);
        size_t it = res.find(char_i);
        while (it != string::npos) {
            res.erase(it, 1);
            it = res.find(char_i);
        }
    }
    return res;
}

/**
 * Parse the argc and argv.
 * Format must be [key]=[value].
 * E.g., abc.exe para1=val1 para2=val2 ...
 */
static std::map<string, string> parse_arg(int argc, char** argv) {
    std::map<string, string> res;
    if (argc == 1) {
        return res;
    }
    for (int i = 1; i < argc; ++i) {
        std::vector<string> res_i = splitstr(argv[i], "=");
        if (res_i.size() != 2) {
            continue;
        }
        res[trimstr(res_i[0], " -")] = trimstr(res_i[1], " ");
    }
    return res;
}

}
#endif
