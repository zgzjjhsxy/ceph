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

struct task_info{
	string dest_addr;
	OSDMigrate *pOSDMigrate;
	task_info() {}
	task_info(string da, OSDMigrate *p) : dest_addr(da), pOSDMigrate(p) {}
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
  	
  	map<string, list<object_info> > task;
  	map<string, int> connection;

    void OSDMigrate_init();
    static void *info_from_client(void *arg);
    static void *do_task(void *arg);
    static void *OSDMigrate_incoming(void *arg);
    static void *OSDMigrate_incoming_recv(void *arg);
    static void task_clear(OSDMigrate *pOSDMigrate);

    OSDMigrate(CephContext *cct){
      OSDcct = cct;
    }
    ~OSDMigrate();
    
};

#endif
