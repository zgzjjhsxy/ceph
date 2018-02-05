#include "OSDMigrate.h"
#include "include/buffer.h"

using ceph::bufferlist;

int64_t OSDMigrate::read_pack(int sockfd, void *buf, uint64_t len){
  if(buf == NULL || len <= 0){
  	return 0;
  }
 
	int64_t nleft = len, nread;
	char *ptr = (char *)buf;
 
  while(nleft > 0){
  	if((nread = read(sockfd, ptr, nleft)) < 0){
  		if(errno == EINTR){
  			nread = 0;
  		}else{
  			return -1;
  		}
  	}else if(nread == 0){
  		break;
  	}
  	nleft -= nread;
  	ptr += nread;
	}
	return (len - nleft);
}
 
int64_t OSDMigrate::write_pack(int sockfd, const void * buf, uint64_t len){
  if(buf == NULL || len <= 0){
  	return 0;
  }
  
  int64_t nleft = len, nwritten;
  const char *ptr = (const char *)buf;
  
  while(nleft > 0){
  	if((nwritten = write(sockfd, ptr, nleft)) <= 0){
  		if(nwritten <= 0 && errno == EINTR){
  			nwritten = 0;
  		}else{
  			return -1;
  		}
  	}
  	nleft -= nwritten;
  	ptr += nwritten;
  }
  return (len - nleft);
}

void *OSDMigrate::info_from_client(void *arg){
  OSDMigrate *pOSDMigrate = (OSDMigrate *)arg;
  int connfd = 0;
  while(1){
  			struct sockaddr_in accept_client_addr;
  			socklen_t accept_client_addr_size = sizeof(accept_client_addr);
        connfd = accept(pOSDMigrate->client_sock, (struct sockaddr*)&(accept_client_addr), &accept_client_addr_size);
        if(connfd < 0){
        	continue;
        }
        pOSDMigrate->accept_client_sock = connfd;
        
        int recv_len = 0;
        bool recv = true;
  			unsigned int length = 0, object_info_size = sizeof(struct object_info);
  			vector<string> dest_addr;
  			list<object_info> task;
  			map<string, int> connection;
  			char *type = new char[sizeof(int)], *size, *buffer, *ack;
				memset(type, 0, sizeof(int));
				librados::Rados cluster;
  			librados::IoCtx io_ctx;
  			librbd::RBD rbd_inst;
  			librbd::Image image;

        while(recv && (recv_len = read(connfd, type, sizeof(int))) > 0){
        	int osd_info_type;
        	memcpy(&osd_info_type, type, sizeof(int));
        	
        	switch(osd_info_type){
        		case MIGRATE_INCOMING_INIT:
        			pOSDMigrate->pool_name = Migrate::recv_str(connfd, MAX_POOL_NAME_SIZE);
        			pOSDMigrate->image_name = Migrate::recv_str(connfd, RBD_MAX_IMAGE_NAME_SIZE);
        			break;
        			
        		case MIGRATE_OUTCOMING_INIT:
        			pOSDMigrate->pool_name = Migrate::recv_str(connfd, MAX_POOL_NAME_SIZE);
        			pOSDMigrate->image_name = Migrate::recv_str(connfd, RBD_MAX_IMAGE_NAME_SIZE);
        			size = new char[sizeof(unsigned int)];
							memset(size, 0, sizeof(unsigned int));
							read(connfd, size, sizeof(unsigned int));
							memcpy(&length, size, sizeof(unsigned int));
							delete size;
							dest_addr.resize(length);
							for(unsigned int i = 0; i < length; i++){
								dest_addr[i] = Migrate::recv_str(connfd, IP_MAX);
							}
							break;
							
						case MIGRATE_START:
							size = new char[sizeof(unsigned int)];
							memset(size, 0, sizeof(unsigned int));
							read(connfd, size, sizeof(unsigned int));
							memcpy(&length, size, sizeof(unsigned int));
							delete size;
							
							task.clear();
							buffer = new char[object_info_size];
							for(unsigned int i = 0; i < length; i++){
								object_info temp;
								memset(buffer, 0, object_info_size);
								read(connfd, buffer, object_info_size);
								memcpy(&temp, buffer, object_info_size);
								task.push_back(temp);
							}
							delete buffer;

  						cluster.init_with_context(pOSDMigrate->OSDcct);
        			cluster.conf_read_file("/etc/ceph/ceph.conf");
        			cluster.connect();
        			cluster.ioctx_create(pOSDMigrate->pool_name.c_str(), io_ctx);
        			rbd_inst.open(io_ctx, image, pOSDMigrate->image_name.c_str());
							
							for(list<object_info>::iterator iter = task.begin(); iter != task.end(); ++iter){
								int sock;
								string dest = dest_addr[iter->objectno];
								map<string, int>::iterator is_connect = connection.find(dest);
								if(is_connect == connection.end()){
									sock = socket(AF_INET, SOCK_STREAM, 0);
									struct sockaddr_in serv_addr;
  								memset(&serv_addr, 0, sizeof(serv_addr));
  								serv_addr.sin_family = AF_INET;
  								serv_addr.sin_port=htons(OSD_INCOMING_PORT);
  								serv_addr.sin_addr.s_addr = inet_addr(dest.c_str());
  								int ret = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
  								if(ret < 0){
  									continue;
  								}
  								connection.insert(map<string, int>::value_type(dest, sock));
								}
								sock = connection[dest];
								
								buffer = new char[object_info_size];
								memset(buffer, 0, object_info_size);
								memcpy(buffer, &(*iter), object_info_size);
								write(sock, buffer, object_info_size);
								delete buffer;
								
								bufferlist bl;
								image.read(iter->offset, iter->length, bl);
								buffer = new char[iter->length];
								memset(buffer, 0, iter->length);
								bl.copy(0, iter->length, buffer);
								OSDMigrate::write_pack(sock, buffer, iter->length);
								delete buffer;
								
								ack = new char[sizeof(int)];
								memset(ack, 0, sizeof(int));
								read(sock, ack, sizeof(int));
								delete ack;
							}
							io_ctx.close();
							task.clear();
							break;
							
						case MIGRATE_END:
							recv = false;
							for(map<string, int>::iterator iter = connection.begin(); iter != connection.end(); ++iter){
								close(iter->second);
							}
							dest_addr.clear();
							for(unsigned int i = 0; i < pOSDMigrate->accept_incoming_sock.size(); i++){
    						close(pOSDMigrate->accept_incoming_sock[i]);
    					}
    					pOSDMigrate->accept_incoming_sock.clear();
							break;
        	}
        	
        	int client_ack = SUCCESS;
					write(connfd, &client_ack, sizeof(int));
					if(osd_info_type == MIGRATE_END){
						close(connfd);
						pOSDMigrate->accept_client_sock = -1;
					}
      	}
  }
  return NULL;
}

void *OSDMigrate::OSDMigrate_incoming(void *arg){
  OSDMigrate *pOSDMigrate = (OSDMigrate *)arg;
  int connfd = 0;
  pthread_t tid;
  while(1){
  			struct sockaddr_in accept_incoming_addr;
  			socklen_t accept_incoming_addr_size = sizeof(accept_incoming_addr);
        connfd = accept(pOSDMigrate->incoming_sock, (struct sockaddr*)&(accept_incoming_addr), &accept_incoming_addr_size);
        if(connfd < 0){
        	continue;
        }
        pOSDMigrate->accept_incoming_sock.push_back(connfd);
        struct connect_info *info = new struct connect_info(connfd, pOSDMigrate);
        pthread_create(&tid, NULL, OSDMigrate_incoming_recv, (void *)info);
        pthread_detach(tid);
  }
  return NULL;
}

void *OSDMigrate::OSDMigrate_incoming_recv(void *arg){
	struct connect_info *info = (connect_info *)arg;
	int sock = info->connfd;
	OSDMigrate *incoming = info->pOSDMigrate;
	int recv_len = 0;
	unsigned int object_info_size = sizeof(struct object_info);
	char *buffer = new char[object_info_size];
	memset(buffer, 0, object_info_size);
	
	librados::Rados cluster;
  librados::IoCtx io_ctx;
  librbd::RBD rbd_inst;
 	librbd::Image image;
 	cluster.init_with_context(incoming->OSDcct);
  cluster.conf_read_file("/etc/ceph/ceph.conf");
  cluster.connect();
  cluster.ioctx_create(incoming->pool_name.c_str(), io_ctx);
  rbd_inst.open(io_ctx, image, incoming->image_name.c_str());

	while((recv_len = read(sock, buffer, object_info_size)) > 0){
		struct object_info object;
		memcpy(&object, buffer, object_info_size);
		char *write_buf = new char[object.length];
		OSDMigrate::read_pack(sock, write_buf, object.length);
		bufferlist bl;
		bl.clear();
		bl.append(write_buf, object.length);
		image.write(object.offset, object.length, bl);
		
		int ack = SUCCESS;
		write(sock, &ack, sizeof(int));
		
		memset(buffer, 0, object_info_size);
		delete write_buf;
	}
	delete buffer;
	io_ctx.close();
	return NULL;
}

void OSDMigrate::OSDMigrate_init(){
  client_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  memset(&client_addr, 0, sizeof(client_addr));
  client_addr.sin_family = AF_INET;
  client_addr.sin_port = htons(CLIENT_TO_OSD_PORT);
  if(bind(client_sock, (struct sockaddr *)&client_addr, sizeof(struct sockaddr)) == 0){
    listen(client_sock, CONNECT_MAX);
    pthread_create(&client_tid, NULL, info_from_client, (void *)this);
  }

  incoming_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  memset(&incoming_addr, 0, sizeof(incoming_addr));
  incoming_addr.sin_family = AF_INET;
  incoming_addr.sin_port = htons(OSD_INCOMING_PORT);
  if(bind(incoming_sock, (struct sockaddr *)&incoming_addr, sizeof(struct sockaddr)) == 0){
    listen(incoming_sock, CONNECT_MAX);
    pthread_create(&incoming_tid, NULL, OSDMigrate_incoming, (void *)this);
  }
}

OSDMigrate::~OSDMigrate(){
  if(client_sock >= 0){
  	if(accept_client_sock >= 0){
  		close(accept_client_sock);
  		accept_client_sock = -1;
  	}
    close(client_sock);
    pthread_join(client_tid, NULL);
  }

  if(incoming_sock >= 0){
    for(unsigned int i = 0; i < accept_incoming_sock.size(); i++){
    	close(accept_incoming_sock[i]);
    }
    accept_incoming_sock.clear();
    close(incoming_sock);
    pthread_join(incoming_tid, NULL);
  }
}
