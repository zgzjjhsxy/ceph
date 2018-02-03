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

class OSDMigrate{
  public:
  	CephContext *OSDcct;
  	pthread_t client_tid, incoming_tid;
    int client_sock = -1, incoming_sock = -1;
    int accept_client_sock = -1;
    vector<int> accept_incoming_sock;
    struct sockaddr_in client_addr, incoming_addr;
    librados::Rados cluster;
  	librados::IoCtx io_ctx;
  	librbd::RBD rbd_inst;
  	librbd::Image image;

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
