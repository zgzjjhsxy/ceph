#include "OSDMigrate.h"
#include "include/buffer.h"

using ceph::bufferlist;

void OSDMigrate::task_clear(OSDMigrate *pOSDMigrate){
	for(map<string, list<object_info> >::iterator iter = pOSDMigrate->task.begin(); iter != pOSDMigrate->task.end(); ++iter){
    iter->second.clear();
  }
  pOSDMigrate->task.clear();
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
        
        int recv_len = 0, osd_info_type = 0;
        bool recv = true;
  			unsigned int length = 0, object_info_size = sizeof(struct object_info);
  			char *buffer;
  			vector<pthread_t> task_tid;
  			map<string, list<object_info> >::iterator iter;
  			pthread_attr_t attr;
				pthread_attr_init(&attr);
				size_t stack_size = (size_t)1024 * 1024 * 16;
				pthread_attr_setstacksize(&attr, stack_size);

        while(recv && (recv_len = read(connfd, &osd_info_type, sizeof(int))) > 0){
        	switch(osd_info_type){
        		case MIGRATE_INIT:
        			pOSDMigrate->pool_name = Migrate::recv_str(connfd, MAX_POOL_NAME_SIZE);
        			pOSDMigrate->image_name = Migrate::recv_str(connfd, RBD_MAX_IMAGE_NAME_SIZE);
        			break;
							
						case MIGRATE_START:
							Migrate::read_pack(connfd, &length, sizeof(unsigned int));
							
							task_clear(pOSDMigrate);
							buffer = new char[object_info_size];
							for(unsigned int i = 0; i < length; i++){
								object_info temp;
								memset(buffer, 0, object_info_size);
								Migrate::read_pack(connfd, buffer, object_info_size);
								memcpy(&temp, buffer, object_info_size);
								string ip = temp.dest_ip;
      					iter = pOSDMigrate->task.find(ip);
								if(iter == pOSDMigrate->task.end()){
      						pOSDMigrate->task.insert(map<string, list<object_info> >::value_type(ip, list<object_info>()));
      					}
								pOSDMigrate->task[ip].push_back(temp);
							}
							delete buffer;
								
							for(iter = pOSDMigrate->task.begin(); iter != pOSDMigrate->task.end(); ++iter){
								int sock;
								string dest = iter->first;
								map<string, int>::iterator is_connect = pOSDMigrate->connection.find(dest);
								if(is_connect == pOSDMigrate->connection.end()){
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
									pOSDMigrate->connection.insert(map<string, int>::value_type(dest, sock));
								}
									
								pthread_t tid;
								struct task_info *info = new struct task_info(iter->first, pOSDMigrate);
								pthread_create(&tid, &attr, do_task, (void *)info);
								task_tid.push_back(tid);
							}
		
							for(unsigned int i = 0; i < task_tid.size(); i++){
								pthread_join(task_tid[i], NULL);
							}
							task_tid.clear();
							task_clear(pOSDMigrate);
							break;
							
						case MIGRATE_END:
							recv = false;
							for(map<string, int>::iterator iter = pOSDMigrate->connection.begin(); iter != pOSDMigrate->connection.end(); ++iter){
								close(iter->second);
							}
							pOSDMigrate->connection.clear();
							for(unsigned int i = 0; i < pOSDMigrate->accept_incoming_sock.size(); i++){
    						close(pOSDMigrate->accept_incoming_sock[i]);
    					}
    					pOSDMigrate->accept_incoming_sock.clear();
							break;
        	}
        	
        	int client_ack = SUCCESS;
					Migrate::write_pack(connfd, &client_ack, sizeof(int));
					if(osd_info_type == MIGRATE_END){
						close(connfd);
						pOSDMigrate->accept_client_sock = -1;
					}
      	}
  }
  return NULL;
}

void *OSDMigrate::do_task(void *arg){
	struct task_info *info = (task_info *)arg;
	string dest = info->dest_addr;
	OSDMigrate *pOSDMigrate = info->pOSDMigrate;
	int sock = pOSDMigrate->connection[dest], ack = 0;
	bufferlist bl;
  char *buffer;
  unsigned int size, object_info_size = sizeof(struct object_info);
	
	librados::Rados cluster;
  librados::IoCtx io_ctx;
  librbd::RBD rbd_inst;
  librbd::Image image;
  cluster.init_with_context(pOSDMigrate->OSDcct);
  cluster.conf_read_file("/etc/ceph/ceph.conf");
  cluster.connect();
  cluster.ioctx_create(pOSDMigrate->pool_name.c_str(), io_ctx);
  rbd_inst.open(io_ctx, image, pOSDMigrate->image_name.c_str());
  
  buffer = new char[object_info_size];
  size = pOSDMigrate->task[dest].size();
  list<object_info>::iterator iter = pOSDMigrate->task[dest].begin();
  for(unsigned int i = 0; i < size; ++i){
  	memset(buffer, 0, object_info_size);
		memcpy(buffer, &(*iter), object_info_size);
		Migrate::write_pack(sock, buffer, object_info_size);
		
		bl.clear();
		image.read(iter->offset, iter->length, bl);
		Migrate::write_pack(sock, bl.c_str(), iter->length);
		bl.clear();
		++iter;
								
		Migrate::read_pack(sock, &ack, sizeof(int));
  }
  
  return NULL;
}

void *OSDMigrate::OSDMigrate_incoming(void *arg){
  OSDMigrate *pOSDMigrate = (OSDMigrate *)arg;
  int connfd = 0;
  pthread_t tid;
  while(1){
  			pthread_attr_t attr;
  			pthread_attr_init(&attr);
  			size_t stack_size = (size_t)1024 * 1024 * 16;
  			pthread_attr_setstacksize(&attr, stack_size);
  			struct sockaddr_in accept_incoming_addr;
  			socklen_t accept_incoming_addr_size = sizeof(accept_incoming_addr);
        connfd = accept(pOSDMigrate->incoming_sock, (struct sockaddr*)&(accept_incoming_addr), &accept_incoming_addr_size);
        if(connfd < 0){
        	continue;
        }
        pOSDMigrate->accept_incoming_sock.push_back(connfd);
        struct connect_info *info = new struct connect_info(connfd, pOSDMigrate);
        pthread_create(&tid, &attr, OSDMigrate_incoming_recv, (void *)info);
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
		Migrate::read_pack(sock, write_buf, object.length);
		bufferlist bl;
		bl.clear();
		bl.append(write_buf, object.length);
		delete write_buf;
		image.write(object.offset, object.length, bl);
		
		int ack = SUCCESS;
		Migrate::write_pack(sock, &ack, sizeof(int));
		
		memset(buffer, 0, object_info_size);
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
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  size_t stack_size = (size_t)1024 * 1024 * 16;
  pthread_attr_setstacksize(&attr, stack_size);
  if(bind(client_sock, (struct sockaddr *)&client_addr, sizeof(struct sockaddr)) == 0){
    listen(client_sock, CONNECT_MAX);
    pthread_create(&client_tid, &attr, info_from_client, (void *)this);
  }

  incoming_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  memset(&incoming_addr, 0, sizeof(incoming_addr));
  incoming_addr.sin_family = AF_INET;
  incoming_addr.sin_port = htons(OSD_INCOMING_PORT);
  if(bind(incoming_sock, (struct sockaddr *)&incoming_addr, sizeof(struct sockaddr)) == 0){
    listen(incoming_sock, CONNECT_MAX);
    pthread_create(&incoming_tid, &attr, OSDMigrate_incoming, (void *)this);
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
