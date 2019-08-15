/**
 * An Example HTTP server based on WT log system.
 * This QPS is not big since the HTTP short connection have only about 10K QPS.
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
#include "handlethread.h"

using namespace wt;
using namespace std;

#define MAX_EVENTS	1000
#define PORT		8089
#define EPOLL_NUM   1 // The number of EPOLLs.
#define THREAD_NUM  1 // The number of HandleThread instance.

std::vector<HandleThread*> ht_pool; // The HandleThread instances.
wt::WTAtomQueue<PReq> print_queue;  // HandleThread will listen this queue.
HandleThread** hts = (HandleThread**)malloc(sizeof(void*) * THREAD_NUM);

static void setnonblocking(int sockfd)
{
	int opts;
	opts = fcntl(sockfd, F_GETFL);
	if (opts < 0)
	{
		perror("fcntl(F_GETFL)\n");
		exit(1);
	}
	opts = (opts | O_NONBLOCK);
	if (fcntl(sockfd, F_SETFL, opts) < 0)
	{
		perror("fcntl(F_SETFL)\n");
		exit(1);
	}
}

int listenfd;
int epfd[EPOLL_NUM];
int addrlen, nread, n;
struct sockaddr_in local, remote;
char buf[BUFSIZ];

static void* listen_fun(void*) {
    int cur_ep_fd = 0;
    int conn_sock;
    struct epoll_event ev;
    while (true) {
        while ((conn_sock = accept(listenfd, (struct sockaddr *) &remote,
                         (socklen_t *) &addrlen)) > 0)
        {
            setnonblocking(conn_sock);
            ev.events	= EPOLLIN | EPOLLET;
            ev.data.fd	= conn_sock;
            if (epoll_ctl(epfd[cur_ep_fd], EPOLL_CTL_ADD, conn_sock,
                    &ev) == -1)
            {
                perror("epoll_ctl: add");
                exit(EXIT_FAILURE);
            }
            cur_ep_fd = (cur_ep_fd + 1) % EPOLL_NUM;
        }
        if (conn_sock == -1)
        {
            if (errno != EAGAIN && errno != ECONNABORTED
                 && errno != EPROTO && errno != EINTR)
                perror("accept");
        }
    }
}

void my_handler(int s){
   shutdown(listenfd, SHUT_RDWR);
   cout << "CLOSE RET: " << close(listenfd) << endl;
   printf("Caught signal %d\n",s);
   exit(1);
}

void* monitor_print_queue(void*) {
    while (true) {
        sleep(2);
        cout << "PRINT QUEUE SIZE: " << print_queue.size() << endl;
    }
}

void* epoll_thread(void* args) {
    struct epoll_event events[MAX_EVENTS], ev;
    int& epfd_cur = *(int*)args;
    int fd, i, nfds;
    for (;;)
	{
		nfds = epoll_wait(epfd_cur, events, MAX_EVENTS, -1);
		if (nfds == -1)
		{
			perror("epoll_pwait");
			exit(EXIT_FAILURE);
		}

		for (i = 0; i < nfds; ++i)
		{
			fd = events[i].data.fd;
			if (fd == listenfd)
			{
				cout << "IS listen fd.\n";
                sleep(1000);
				continue;
			}
			if (events[i].events & EPOLLIN)
			{
				n = 0;
				while ((nread = read(fd, buf + n, BUFSIZ - 1)) > 0)
				{
					n += nread;
				}
				if (nread == -1 && errno != EAGAIN)
				{
					perror("read error");
				}
				ev.data.fd	= fd;
				ev.events	= events[i].events | EPOLLOUT;
				if (epoll_ctl(epfd_cur, EPOLL_CTL_MOD, fd, &ev) == -1)
				{
					perror("epoll_ctl: mod");
				}
                buf[n] = '\0';

                map<string, string> params = get_msg_params(buf);
                auto it = params.find("action");
                if (it == params.end() || (it->second != "search" && it->second != "push")) {
                    // Wrong request.
                    string res = build_res("NO ACTION.\n");
                    write_to_socket(res.c_str(), res.size(), fd);
                    close(fd);
                    continue;
                }

                if (it->second == "search") {
                    SReq req = parse_to_sreq(params);
                    if (req.type == -1) {
                        string res = build_res("Insufficient parameters.\n");
                        write_to_socket(res.c_str(), res.size(), fd);
                        close(fd);
                        continue;
                    }
                    req.socket = fd;
                    atomic<bool> fin[THREAD_NUM];;
                    string res[THREAD_NUM];
                    stringstream res_combine;
                    wt::TimeStatistic time;
                    for (int i = 0; i < THREAD_NUM; ++i) {
                        fin[i] = false;
                        hts[i]->search(&req, &res[i], &fin[i]);
                    }
                    for (int i = 0; i < THREAD_NUM; ++i) {
                        while (fin[i] == false) {
                            continue;
                        }
                        res_combine << "From thread: " << i << ": \n" << res[i] << "\n";
                    }
                    uint32_t cost_ms = time.cost_ms();
                    string resp = build_res("Time cost: " + num2str(cost_ms) + ".\n" + res_combine.str() + ".\n");
                    write_to_socket(resp.c_str(), resp.size(), fd);
                    close(fd);
                    continue;
                }

                if (it->second == "push") {
                    PReq req = parse_to_preq(params);
                    while (print_queue.push(req) != 0) {

                        toscreen << "There is too much data in the print queue, and I can't handle more at the moment.\n";
                        usleep(4e5);
                    }
                    string res = build_res("OK.\n");
                    write_to_socket(res.c_str(), res.size(), fd);
                    close(fd);
                }
			}
		}
	}
}

using namespace std;
int main()
{
    // SIGNAL...
    struct sigaction sigIntHandler;
    sigIntHandler.sa_handler = my_handler;
    sigemptyset(&sigIntHandler.sa_mask);
    sigIntHandler.sa_flags = 0;
    sigaction(SIGINT, &sigIntHandler, NULL);
    
    pthread_t mon_t;
    pthread_create(&mon_t, nullptr, monitor_print_queue, nullptr);

    for (int i = 0; i < THREAD_NUM; ++i) {
        hts[i] = new HandleThread("logs" + num2str(i), &print_queue);
    }

	if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		perror("sockfd\n");
		exit(1);
	}
	setnonblocking(listenfd);
	bzero(&local, sizeof(local));
	local.sin_family	= AF_INET;
	local.sin_addr.s_addr	= htonl(INADDR_ANY);;
	local.sin_port		= htons(PORT);
	
	int on = 1;  
    if((setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on,sizeof(on)))<0)  
    {  
         perror("setsockopt failed");  
         exit(EXIT_FAILURE);  
    }  
	
	if (bind(listenfd, (struct sockaddr *) &local, sizeof(local)) < 0)
	{
		perror("bind: \n");
		exit(1);
	}
	listen(listenfd, 10000);

    pthread_t epoll_t[EPOLL_NUM];
    
    for (int i = 0; i < EPOLL_NUM; ++i) {
        epfd[i] = epoll_create(MAX_EVENTS);
	    if (epfd[i] == -1)
	    {
		    perror("epoll_create");
		    exit(EXIT_FAILURE);
	    }
	    pthread_create(&epoll_t[i], nullptr, epoll_thread, &epfd[i]);
    }
	
    pthread_t listen_t;
    pthread_create(&listen_t, nullptr, listen_fun, nullptr);

    int pause_p;
    cin >> pause_p;

	return 0;
}
