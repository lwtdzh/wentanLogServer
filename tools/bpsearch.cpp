/**
 * Program for searching by B+ tree.
 * Only used for the logs by WT log system.
 * Date: 2019/8/8.
 * Author: LiWentan.
 * How to use:
 *     You need to have the parameters:
 *     1. path: The path to storaging logs.
 *     2. stime: Start time.
 *     3. etime: End time.
 *     4. key: The key.
 *     5. content: Search logs contanining this string.
 */

#include <iostream>
#include "wttool.h"
#include "wtbptree.hpp"
#include "handlethread.h"

#define FLOOR_NUM 10000

using namespace std;
using namespace wt;

static char buff[(FLOOR_NUM + 13) * 1000];

int check(int argc, char** argv, map<string, string>* args_in) {
    *args_in = parse_arg(argc, argv);
    map<string, string>& args = *args_in;
    
    auto it = args.find("path");
    if (it == args.end()) {
        cout << "NO PATH.\n";
        return 0;
    }
    
    it = args.find("stime");
    if (it == args.end()) {
        cout << "NO STIME.\n";
        return 0;
    }
    
    it = args.find("etime");
    if (it == args.end()) {
        cout << "NO ETIME.\n";
        return 0;
    }
    
    it = args.find("key");
    if (it == args.end()) {
        cout << "NO KEY.\n";
        return 0;
    }
    
    it = args.find("content");
    if (it == args.end()) {
        cout << "NO CONTENT.\n";
        return 0;
    }
    
    return 1;
}

void construct_bptree(FILE* f, const string& bptree_path) {
    if (fseeko64(f, 0, SEEK_SET) != 0) {
        toscreen << "ERROR.\n";
        sleep(1000);
    }
    BPTree<int32_t, int64_t> bptree;
    bptree.init(bptree_path, FLOOR_NUM);
    size_t offset = 0;
    int read_num = 0;
    while ((read_num = fread(buff, LOG_MAX_LEN + 13, 1000, f)) > 0) {
        offset = 0;
        for (int i = 0; i < read_num; ++i) {
            int32_t& key = *(int32_t*)(buff + offset + 8);
            bptree.insert(key, offset);
            offset += (LOG_MAX_LEN + 13);
        }
    }
}

int main(int argc, char **argv) {
    map<string, string> args;
    if (check(argc, argv, &args) == 0) {
        return 0;
    }
    
    TimeStatistic tt;
    vector<string> f_name = HandleThread::_get_file(args["stime"], args["etime"]);
    stringstream res;
    res << "RESULT: \n";
    
    for (size_t i = 0; i < f_name.size(); ++i) {
        string f_path = string(args["path"]) + "/" + f_name[i];
        
        // Try to open the file.
        FILE* f = fopen(f_path.c_str(), "r");
        if (f == NULL) {
            // No file, check next file. 
            continue;
        }
        
        // Try to open the B+ tree.
        FILE* f_bptree = fopen((f_path + ".bptree").c_str(), "r");
        if (f_bptree == NULL) {
            // No bptree, construct a bp tree.
            f_bptree = fopen((f_path + ".bptree").c_str(), "w");
            if (f_bptree == NULL) {
                toscreen << "ERROR.\n";
                return 0;
            }
            construct_bptree(f, f_path + ".bptree");
            fclose(f_bptree);
            f_bptree = fopen((f_path + ".bptree").c_str(), "r");
            if (f_bptree == NULL) {
                toscreen << "ERROR.\n";
                return 0;
            }
        }
        fclose(f_bptree);
        BPTree<int32_t, int64_t> bptree;
        bptree.init(f_path + ".bptree", FLOOR_NUM);
        vector<int64_t> res_i;
        bptree.search(str2num(args["key"]), &res_i);
        for (size_t i = 0; i < res_i.size(); ++i) {
            if (fseeko64(f, res_i[i], SEEK_SET) != 0) {
                toscreen << "SEEK ERR.\n";
                sleep(10000);
            }
            if (fread(buff, LOG_MAX_LEN + 13, 1 ,f) != 1) {
                toscreen << "READ ERR.\n";
                sleep(1000);
            }
            int32_t& key_i = *(int32_t*)(buff + 8);
            if (key_i != str2num(args["key"])) {
                toscreen << "LOGIC ERR.\n";
                sleep(1000);
            }
            if (string((char*)(buff + 12)).find(args["content"]) == string::npos) {
                continue;
            }
            int64_t& time_i = *(int64_t*)buff;
            if (time_i <= str2num(args["etime"]) && time_i >= str2num(args["stime"])) {
                // Is a cooresponded log.
                res << "Time: " << time_i << " key: " << key_i << " content: " << buff + 12 << ".\n";
            }
        }
        fclose(f);
    }
    cout << "SEARCH COST TIME(ms): " << tt.cost_ms() << endl;
    cout << res.str() << endl;
    return 0;
}
