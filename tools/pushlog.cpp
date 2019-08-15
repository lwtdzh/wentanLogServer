/**
 * Example for pushing logs to the httpservice.
 */

#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <fcntl.h>
#include <errno.h>
#include <iostream>
#include <signal.h>
#include "wttool.h"

#define LOGS_A_DAY 1e9 // The speed of the pushing.
#define THREADS 1

using namespace std;
using namespace wt;

char buff0[102400];
char buff1[102400];
size_t write_number[THREADS];

const int logs_a_sec = LOGS_A_DAY / 86400;
const int usleep_interval = 1e6 / logs_a_sec;

static int send_http(const char* url, const std::map<string, string>& params, char* res = buff1) {
    if (url == nullptr || res == nullptr) {
        return -1;
    }
    
    vector<string> socket_addr = wt::splitstr(url, ":");
    sockaddr_in addr;
    memset(&addr, 0, sizeof(sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)str2num(socket_addr[1]));
    addr.sin_addr.s_addr = inet_addr(socket_addr[0].c_str());
    
    int fd = socket(PF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        toscreen << "Initialize the client socket failed.\n";
        return -1;
    }
    
    int ret = connect(fd, (sockaddr*)&addr, sizeof(sockaddr));
    if (ret < 0) {
        toscreen << "Connect to server failed.\n";
        return -1;
    }
    
    string req_sent = wt::build_req(params);
    wt::write_to_socket(req_sent.c_str(), req_sent.size(), fd);
    int res_len = read(fd, res, 10000);
    
    close(fd);
    return res_len;
}

void* write_thread(void* args) {
    size_t& write_num = *(size_t*)args;
    
    char* url = "127.0.0.1:8089";
    std::map<string, string> params;
    
    for (size_t i = 0; i < LOGS_A_DAY; ++i) {
        wt::TimeStatistic time;
        params["action"] = "push";
        params["content"] = "This log number: " + num2str(i) + ".";
        params["key"] = num2str((i % 2 == 0) ? i : i - 1);
        int ret = send_http(url, params);
        
        int sleep_t = usleep_interval - time.cost_us();
        ++write_num;
        if (sleep_t > 0) {
            usleep(sleep_t);
        }
    }
}

int main() {
   
   pthread_t thread_pool[THREADS];
    
   for (size_t i = 0; i < THREADS; ++i) {
        write_number[i] = 0;
        pthread_create(&thread_pool[i], nullptr, write_thread, &write_number[i]);
   }
   
   int pause_p;
   cin >> pause_p;
   
   return 0;
}
