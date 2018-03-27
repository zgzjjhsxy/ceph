#ifndef MIGRATE_H
#define MIGRATE_H

#include "librbd/ImageCtx.h"
#include "common/ceph_context.h"

#include <vector>
#include <list>
#include <map>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <ctime>

#define CONNECT_MAX 1024
#define IP_MAX 16
#define MAX_POOL_NAME_SIZE 128

#define CLIENT_INCOMING_PORT 6700
#define CLIENT_TO_OSD_PORT 6701
#define OSD_INCOMING_PORT 6702

#define SUCCESS	0
#define SIZE_ERROR 1

#define MIGRATE_INIT	1
#define MIGRATE_START 2
#define MIGRATE_END 3

using librbd::ImageCtx;
using std::vector;
using std::list;
using std::string;
using std::map;

class Migrate;

struct osd_info{
	string osd;
	Migrate *pMigrate;
	osd_info() {}
	osd_info(string o, Migrate *p) : osd(o), pMigrate(p) {}
};

struct object_info{
  uint64_t offset;
  uint64_t length;
  char dest_ip[IP_MAX];
  object_info() : offset(0), length(0) {memset(dest_ip, 0, IP_MAX);}
  object_info(uint64_t off, uint64_t len, string dest) : offset(off), length(len) {memset(dest_ip, 0, IP_MAX);strcpy(dest_ip, dest.c_str());}
};

class Migrate{
  public:
    static vector<string> dest_osd_addr;
    static vector<string> osd_addr;
    static map<string, pthread_t> osd_tid;
    static map<string, int> osd_sock;
    static map<string, list<object_info> > osd_task;
    static int osd_info_type;
    static string pool_name;
    static string image_name;

		static int64_t read_pack(int sockfd, void *buf, uint64_t len);
    static int64_t write_pack(int sockfd, const void *buf, uint64_t len);
    static void send_str(int sock, string str, int max_length);
    static string recv_str(int sock, int max_length);
    static void *info_to_osd(void *arg);
    void connect_to_osd(int osd);
    int osd_locate(ImageCtx *ictx, uint64_t size);
    int migrate_incoming_init(ImageCtx *ictx, uint64_t size);
    int migrate_outcoming_init(ImageCtx *ictx, uint64_t size, const char *ip);
    int migrate_outcoming_start(ImageCtx *ictx, uint64_t offset, uint64_t length);
    int migrate_end();
    void osd_task_clear();
    string choose_dest_addr();
};

#endif
