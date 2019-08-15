/**
 * Generate some logs as the format of WT log system for testing.
 * Params:
 * 1. path: Push logs to this directory.
 * 2. stime: Logs generated begin at this time.
 * 3. etime: Logs generated end at this time.
 * 4. sizeafile: How many logs for a file (File number is calculated by stime and etime).
 */

#include <iostream>
#include "wttool.h"
#include "handlethread.h"

using namespace std;
using namespace wt;

bool check_args(const map<string, string>& args) {
    if (args.find("path") == args.cend()) {
        toscreen << "NO PATH.\n";
        return false;
    }
    if (args.find("stime") == args.cend()) {
        toscreen << "NO STIME.\n";
        return false;
    }
    if (args.find("etime") == args.cend()) {
        toscreen << "NO ETIME.\n";
        return false;
    }
    if (args.find("sizeafile") == args.cend()) {
        toscreen << "NO SIZEAFILE.\n";
        return false;
    }
    return true;
}

int64_t fname2time(const string& fname) {
    int f_num = str2num(string(fname, 9));
    static int filesec = 86400 / SPLIT_DAY;
    int start_sec = filesec * f_num + 3;
    int hour = start_sec / 3600;
    start_sec %= 3600;
    int min = start_sec / 60;
    start_sec %= 60;
    int sec = start_sec;
    char time[15];
    sprintf(time, string(fname, 0, 8).c_str());
    sprintf(time + 8, "%02d%02d%02d", hour, min, sec);
    // cout << "F2T TIME: " << time << endl;
    return str2num(time);
}

int main(int argc, char** argv) {
    map<string, string> args = parse_arg(argc, argv);
    if (check_args(args) == false) {
        return 0;
    }

    vector<string> f_name = HandleThread::_get_file(args["stime"], args["etime"]);
    int64_t sizeafile = str2num(args["sizeafile"]);
    int32_t cur_key = 0;
    for (size_t i = 0; i < f_name.size(); ++i) {
        string f_name_i = args["path"] + "/" + f_name[i];
        FILE* f_i = fopen(f_name_i.c_str(), "w");
        if (f_i == NULL) {
            toscreen << "File open failed: " << f_name_i << ".\n";
            continue;
        }
        int64_t f_time = fname2time(f_name[i]);
        for (int64_t j = 0; j < sizeafile; ++j) {
            static char buff[LOG_MAX_LEN + 13];
            memcpy(buff, &f_time, 8);
            memcpy(buff + 8, &(++cur_key), 4);
            sprintf(buff + 12, "this is a log of key: %d", cur_key);
            if (fwrite(buff, LOG_MAX_LEN + 13, 1, f_i) != 1) {
                toscreen << "WRITE ERROR.\n";
                sleep(1000);
            }
        }
        fclose(f_i);
    }
    
    return 0;
}