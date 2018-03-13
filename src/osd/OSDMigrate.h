#ifndef OSDMIGRATE_H
#define OSDMIGRATE_H

#include "osdc/Migrate.h"

using std::string;

class OSDMigrate;

struct connect_info{
	int connfd;
	OSDMigrate *pOSDMigrate;
	connect_info() {}
	connect_info(int c, OSDMigrate *p) : connfd(c), pOSDMigrate(p) {}
};

struct migrate_info{
  uint64_t offset;
  uint64_t length;
  migrate_info() : offset(0), length(0) {}
  migrate_info(uint64_t off, uint64_t len) : offset(off), length(len) {}
};

class OSDMigrate{
  public:
  	CephContext *OSDcct;
  	pthread_t client_tid, incoming_tid;
    int client_sock = -1, incoming_sock = -1;
    int accept_client_sock = -1;
    vector<int> accept_incoming_sock;
    struct sockaddr_in client_addr, incoming_addr;
  	string pool_name, image_name;
  	
  	vector<string> dest_addr;
  	list<object_info> task;
  	map<string, int> connection;
  	bufferlist bl;

    void OSDMigrate_init();
    static void *info_from_client(void *arg);
    static void *OSDMigrate_incoming(void *arg);
    static void *OSDMigrate_incoming_recv(void *arg);

    OSDMigrate(CephContext *cct){
      OSDcct = cct;
    }
    ~OSDMigrate();
    
};

#endif
