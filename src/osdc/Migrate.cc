#include "Migrate.h"
#include "osd/osd_types.h"
#include "osdc/Striper.h"
#include "librados/IoCtxImpl.h"
#include "msg/msg_types.h"

vector<string> Migrate::dest_osd_addr;
vector<string> Migrate::osd_addr;
map<string, pthread_t> Migrate::osd_tid;
map<string, int> Migrate::osd_sock;
map<string, list<object_info> > Migrate::osd_task;
int Migrate::osd_info_type = 0;
string Migrate::pool_name;
string Migrate::image_name;

void Migrate::osd_task_clear(){
	for(map<string, list<object_info> >::iterator iter = osd_task.begin(); iter != osd_task.end(); ++iter){
    iter->second.clear();
  }
  osd_task.clear();
}

string Migrate::choose_dest_addr(){
	uint64_t osd_nums = osd_addr.size();
	srand((unsigned)time(NULL));
	string res;
	if(osd_nums > 0){
		res = osd_addr[rand() % osd_nums];
	}
	return res;
}

int64_t Migrate::read_pack(int sockfd, void *buf, uint64_t len){
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
 
int64_t Migrate::write_pack(int sockfd, const void *buf, uint64_t len){
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

void Migrate::send_str(int sock, string str, int max_length){
  char *temp = new char[max_length];
  memset(temp, 0, max_length);
  strcpy(temp, str.c_str());
  write_pack(sock, temp, max_length);
}

string Migrate::recv_str(int sock, int max_length){
  char *temp = new char[max_length];
  memset(temp, 0, max_length);
  read_pack(sock, temp, max_length);
  string temp_str(temp);
  return temp_str;
}

void Migrate::connect_to_osd(int osd){
	map<string, int>::iterator iter = osd_sock.find(osd_addr[osd]);
	if(iter == osd_sock.end()){
		int sock = socket(AF_INET, SOCK_STREAM, 0);
		struct sockaddr_in serv_addr;
  	memset(&serv_addr, 0, sizeof(serv_addr));
  	serv_addr.sin_family = AF_INET;
  	serv_addr.sin_port=htons(CLIENT_TO_OSD_PORT);
  	serv_addr.sin_addr.s_addr = inet_addr(osd_addr[osd].c_str());
  	connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
  	osd_sock.insert(map<string, int>::value_type(osd_addr[osd], sock));
	}
}

void *Migrate::info_to_osd(void *arg){
	struct osd_info *info = (struct osd_info *)arg;
	string osd = info->osd;
	Migrate *pMigrate = info->pMigrate;
	int sock = pMigrate->osd_sock[osd];
	unsigned int length = 0, object_info_size = sizeof(struct object_info);
	char *buffer;
	
	write_pack(sock, &(pMigrate->osd_info_type), sizeof(int));
	
	switch(pMigrate->osd_info_type){
		case MIGRATE_INIT:
			send_str(sock, pool_name, MAX_POOL_NAME_SIZE);
			send_str(sock, image_name, RBD_MAX_IMAGE_NAME_SIZE);
			break;
			
		case MIGRATE_START:
			length = osd_task[osd].size();
			write_pack(sock, &length, sizeof(unsigned int));
			
			buffer = new char[object_info_size];
			list<object_info>::iterator iter = osd_task[osd].begin();
			for(unsigned int i = 0; i < length; i++){
				memset(buffer, 0, object_info_size);
				memcpy(buffer, &(*iter), object_info_size);
				write_pack(sock, buffer, object_info_size);
				++iter;
			}
			delete buffer;
			break;
	}
	
	char *ack = new char[sizeof(int)];
	memset(ack, 0, sizeof(int));
	read_pack(sock, ack, sizeof(int));
	delete ack;
	
	if(pMigrate->osd_info_type == MIGRATE_END){
		close(sock);
	}
	
	return NULL;
}

int Migrate::osd_locate(ImageCtx *ictx, uint64_t length){
  map<object_t,vector<ObjectExtent> > object_extents;
  uint64_t buffer_ofs = 0;
  vector<pair<uint64_t,uint64_t> > image_extents;
  image_extents.push_back(make_pair(0, length));
  CephContext *cct = ictx->cct;
  for(vector<pair<uint64_t,uint64_t> >::const_iterator p = image_extents.begin(); p != image_extents.end(); ++p){
  	uint64_t len = p->second;
    if(len == 0){
	  	continue;
    }
    Striper::file_to_extents(cct, ictx->format_string, &ictx->layout, p->first, len, 0, object_extents, buffer_ofs);
    buffer_ofs += len;
  }
  
  for(map<object_t,vector<ObjectExtent> >::iterator p = object_extents.begin(); p != object_extents.end(); ++p){
    for (vector<ObjectExtent>::iterator q = p->second.begin(); q != p->second.end(); ++q){
      Objecter::op_target_t *target = new struct Objecter::op_target_t(q->oid, q->oloc, CEPH_OSD_FLAG_LOCATE);
      ictx->data_ctx.get_io_ctx_impl()->objecter->calc_target(target);
      entity_inst_t dest = ictx->data_ctx.get_io_ctx_impl()->objecter->get_osdmap()->get_inst(target->osd);
      char *my_addr = inet_ntoa(((sockaddr_in *)&dest.addr.addr)->sin_addr);
      string str_my_addr = my_addr;
      if((uint64_t)(target->osd) >= osd_addr.size()){
        	osd_addr.resize(target->osd + 1);
      }
      osd_addr[target->osd] = str_my_addr;
      delete target;
    }
  }
  
  ifstream file;
  file.open("/etc/ceph/osd_addr.conf");
  if(file.is_open()){
    osd_addr.clear();
    string buf;
    while (getline(file, buf)){
    	if(buf.empty()){
    		continue;
    	}
    	uint64_t osd;
    	string ip;
      istringstream sin(buf);
      sin >> osd >> ip;
      if(osd >= osd_addr.size()){
      	osd_addr.resize(osd + 1);
      }
      osd_addr[osd] = ip;
    }
    file.close();
  }
  return 0;
}


int Migrate::migrate_incoming_init(ImageCtx *ictx, uint64_t size){
  int incoming_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  struct sockaddr_in incoming_addr, outcoming_addr;
  memset(&incoming_addr, 0, sizeof(incoming_addr));
  incoming_addr.sin_family = AF_INET;
  incoming_addr.sin_port = htons(CLIENT_INCOMING_PORT);
  if(bind(incoming_sock, (struct sockaddr *)&incoming_addr, sizeof(struct sockaddr))==-1){
    std::cout << "port " << CLIENT_INCOMING_PORT << " bind error" << std::endl;
    return -1;
  }
  listen(incoming_sock, CONNECT_MAX);
  socklen_t outcoming_addr_size = sizeof(outcoming_addr);
  int outcoming_sock = accept(incoming_sock, (struct sockaddr*)&outcoming_addr, &outcoming_addr_size);
  
  uint64_t source_size;
  read_pack(outcoming_sock, &source_size, sizeof(uint64_t));

  int migrate_flag = SUCCESS;
  if(source_size > size){
    migrate_flag = SIZE_ERROR;
    std::cout << "target image size is too small" << std::endl;
  }
  write_pack(outcoming_sock, &migrate_flag, sizeof(int));
  if(migrate_flag == SUCCESS){
    pool_name = ictx->data_ctx.get_pool_name();
    image_name = ictx->name;
    osd_locate(ictx, size);
    
    uint64_t osd_nums = osd_addr.size();
    write_pack(outcoming_sock, &osd_nums, sizeof(uint64_t));
    
    for(uint64_t i = 0; i < osd_nums; i++){
      send_str(outcoming_sock, osd_addr[i], IP_MAX);
    }
    osd_tid.clear();
    osd_sock.clear();
    osd_info_type = MIGRATE_INIT;
    for(unsigned int i = 0; i < osd_nums; i++){
    	if(!osd_addr[i].empty()){
    		connect_to_osd(i);
    	}
    }
    
    for(map<string, int>::iterator iter = osd_sock.begin(); iter != osd_sock.end(); ++iter){
    	pthread_t tid;
    	struct osd_info *info = new struct osd_info(iter->first, this);
    	pthread_create(&tid, NULL, info_to_osd, (void *)info);
    	osd_tid.insert(map<string, pthread_t>::value_type(iter->first, tid));
    }
    
    for(map<string, pthread_t>::iterator iter = osd_tid.begin(); iter != osd_tid.end(); ++iter){
    	pthread_join(iter->second, NULL);
    }
    osd_tid.clear();
    close(incoming_sock);
  	close(outcoming_sock);
  	return 0;
  }else{
  	close(incoming_sock);
  	close(outcoming_sock);
  	return -1;
  }
}

int Migrate::migrate_outcoming_init(ImageCtx *ictx, uint64_t size, const char *ip){
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port=htons(CLIENT_INCOMING_PORT);
  serv_addr.sin_addr.s_addr = inet_addr(ip);
  int ret = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
  if(ret < 0){
  	return -1;
  }

  write_pack(sock, &size, sizeof(uint64_t));
  int migrate_flag;
  read_pack(sock, &migrate_flag, sizeof(int));

  if(migrate_flag != SUCCESS){
  	std::cout << "target image size is too small" << std::endl;
    close(sock);
    return -1;
  }else{
    uint64_t dest_osd_nums = 0;
    read_pack(sock, &dest_osd_nums, sizeof(uint64_t));
    dest_osd_addr.clear();
    dest_osd_addr.resize(dest_osd_nums);
    for(uint64_t i = 0; i < dest_osd_nums; i++){
      string temp_addr = recv_str(sock, IP_MAX);
      dest_osd_addr[i] = temp_addr;
    }
    pool_name = ictx->data_ctx.get_pool_name();
    image_name = ictx->name;
    osd_locate(ictx, size);
    osd_tid.clear();
    osd_sock.clear();
    osd_info_type = MIGRATE_INIT;
    for(unsigned int i = 0; i < osd_addr.size(); i++){
    	if(!osd_addr[i].empty()){
    		connect_to_osd(i);
    	}
    }
    
    for(map<string, int>::iterator iter = osd_sock.begin(); iter != osd_sock.end(); ++iter){
    	pthread_t tid;
    	struct osd_info *info = new struct osd_info(iter->first, this);
    	pthread_create(&tid, NULL, info_to_osd, (void *)info);
    	osd_tid.insert(map<string, pthread_t>::value_type(iter->first, tid));
    }
    
    for(map<string, pthread_t>::iterator iter = osd_tid.begin(); iter != osd_tid.end(); ++iter){
    	pthread_join(iter->second, NULL);
    }
    osd_tid.clear();
    close(sock);
    return 0;
  }
}

int Migrate::migrate_outcoming_start(ImageCtx *ictx, uint64_t offset, uint64_t length){
	map<object_t,vector<ObjectExtent> > object_extents;
  uint64_t buffer_ofs = 0;
  vector<pair<uint64_t,uint64_t> > image_extents;
  image_extents.push_back(make_pair(offset, length));
  CephContext *cct = ictx->cct;
  for(vector<pair<uint64_t,uint64_t> >::const_iterator p = image_extents.begin(); p != image_extents.end(); ++p){
  	uint64_t len = p->second;
    if(len == 0){
	  	continue;
    }
    Striper::file_to_extents(cct, ictx->format_string, &ictx->layout, p->first, len, 0, object_extents, buffer_ofs);
    buffer_ofs += len;
  }
  
  osd_task_clear();
  for(map<object_t,vector<ObjectExtent> >::iterator p = object_extents.begin(); p != object_extents.end(); ++p){
    for (vector<ObjectExtent>::iterator q = p->second.begin(); q != p->second.end(); ++q){
      Objecter::op_target_t *target = new struct Objecter::op_target_t(q->oid, q->oloc, CEPH_OSD_FLAG_LOCATE);
      ictx->data_ctx.get_io_ctx_impl()->objecter->calc_target(target);
      string ip = osd_addr[target->osd];
      map<string, list<object_info> >::iterator iter = osd_task.find(ip);
      if(iter == osd_task.end()){
      	osd_task.insert(map<string, list<object_info> >::value_type(ip, list<object_info>()));
      }
      for(vector<pair<uint64_t,uint64_t> >::iterator r = q->buffer_extents.begin(); r != q->buffer_extents.end(); ++r){
      	string dest = choose_dest_addr();
      	struct object_info temp(offset + r->first, r->second, dest);
      	osd_task[ip].push_back(temp);
      }
    }
  }
  
  for(unsigned int i = 0; i < osd_addr.size(); i++){
   	if(!osd_addr[i].empty()){
   		connect_to_osd(i);
   	}
  }
  
  osd_tid.clear();
  osd_info_type = MIGRATE_START;
  for(map<string, list<object_info> >::iterator iter = osd_task.begin(); iter != osd_task.end(); ++iter){
    pthread_t tid;
    struct osd_info *info = new struct osd_info(iter->first, this);
    pthread_create(&tid, NULL, info_to_osd, (void *)info);
    osd_tid.insert(map<string, pthread_t>::value_type(iter->first, tid));
  }
  
  for(map<string, pthread_t>::iterator iter = osd_tid.begin(); iter != osd_tid.end(); ++iter){
    pthread_join(iter->second, NULL);
  }
  osd_tid.clear();
  osd_task_clear();
  return 0;
}

int Migrate::migrate_end(){
  for(unsigned int i = 0; i < osd_addr.size(); i++){
    if(!osd_addr[i].empty()){
    	connect_to_osd(i);
    }
  }
  
  osd_tid.clear();
  osd_info_type = MIGRATE_END;
  for(map<string, int>::iterator iter = osd_sock.begin(); iter != osd_sock.end(); ++iter){
    pthread_t tid;
    struct osd_info *info = new struct osd_info(iter->first, this);
    pthread_create(&tid, NULL, info_to_osd, (void *)info);
    osd_tid.insert(map<string, pthread_t>::value_type(iter->first, tid));
  }
  
  for(map<string, pthread_t>::iterator iter = osd_tid.begin(); iter != osd_tid.end(); ++iter){
    pthread_join(iter->second, NULL);
  }
  osd_tid.clear();
  for(map<string, int>::iterator iter = osd_sock.begin(); iter != osd_sock.end(); ++iter){
  	close(iter->second);
  }
  osd_sock.clear();
  
  osd_task_clear();
  dest_osd_addr.clear();
  osd_addr.clear();
  osd_info_type = 0;
  pool_name.clear();
  image_name.clear();
  return 0;
}

