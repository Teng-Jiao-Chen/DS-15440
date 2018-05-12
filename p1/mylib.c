#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <dirtree.h>
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include <errno.h>
#include <assert.h>
#include <netinet/tcp.h>

#define MAXPKGLEN 1024
#define MAXMSGLEN 1005000
#define FD_OFFSET 3000

// The following line declares a function pointer with the same prototype as the open function.  
int (*orig_open)(const char *pathname, int flags, ...);
int (*orig_close)(int fd);
ssize_t (*orig_read) (int fd, void *buf, size_t nbytes);
ssize_t (*orig_write) (int fd, const void *buf, size_t n);
__off_t (*orig_lseek) (int fd, off_t offset, int whence);
int (*orig_stat)(int ver, const char * path, struct stat * stat_buf);
int (*orig_unlink) (const char *__name);
struct dirtreenode* (*orig_getdirtree)( const char *path );
ssize_t (*orig_getdirentries)(int fd, char *buf, size_t nbytes , off_t *basep);
void (*orig_freedirtree)( struct dirtreenode* dt );
void send_message(char * msg, ssize_t size);
void* deserialize(void* ptr, struct dirtreenode** tree);

void print_stat(struct stat *buf); // Print several attributes of stat (for debugging purpose)


int open(const char *pathname, int flags, ...) {

    fprintf(stderr, "Client calling [open]\n");
    fprintf(stderr, "Client call [open] (pathname)%s (flags)%d\n", pathname, flags);

    // Client get modes
	mode_t m = 0;
	if (flags & O_CREAT) {
		va_list a;
		va_start(a, flags);
		m = va_arg(a, mode_t);
		va_end(a);
	}

	// Client send RPC.
	char * msg = NULL;
	asprintf(&msg, "open %s %d %u", pathname, flags, m);
	send_message(msg, strlen(msg));

	// Client get response.
	char *save_ptr;
	int ret = atoi(strtok_r (msg, " ", &save_ptr));
	errno = atoi(strtok_r (NULL, " ", &save_ptr));

    free(msg);

    // This is used for distinguish between the server file and local files
    // Thus all the files operated on the server will go from FD_OFFSET
    if(ret > 0) ret += FD_OFFSET;

    fprintf(stderr, "Client [open] got message: (ret)%d (errno)%d\n", ret, errno);

	return ret;
}


int close(int fd){

    fprintf(stderr, "Client calling [close]\n");
	fprintf(stderr, "Client call [close] (fd)%d\n",fd);

    if(fd < FD_OFFSET) return orig_close(fd);
    else fd -=  FD_OFFSET;

    // Client send RPC.
	char * msg = NULL;
	asprintf(&msg, "close %d", fd);
	send_message(msg, strlen(msg));

	// Client get response.
	char *save_ptr;
	int ret = atoi(strtok_r (msg, " ", &save_ptr));
	errno = atoi(strtok_r (NULL, " ", &save_ptr));
	free(msg);

    fprintf(stderr, "Client [close] got message: (ret)%d (errno)%d\n", ret, errno);

	return ret;
}

ssize_t read (int fd, void *buf, size_t nbytes){

    fprintf(stderr, "Client calling [read]\n");
    fprintf(stderr, "Client call [read] (fd) %d (nbytes) %zu\n", fd, nbytes);

    if(fd < FD_OFFSET) return orig_read(fd, buf, nbytes);
    else fd -=  FD_OFFSET;

    // Client send RPC.
    char msg[MAXMSGLEN] = {};
    sprintf(msg, "read %d %zu", fd, nbytes);
	send_message(msg, strlen(msg));

	// Client get response.
	char *save_ptr;
    int ret = atoi(strtok_r (msg, " ", &save_ptr));
    errno = atoi(strtok_r (NULL, " ", &save_ptr));

	fprintf(stderr, "Client [read] got message (ret)%d (errno)%d\n", ret, errno);

	if(ret >= 0) memcpy(buf, save_ptr, ret);

	return ret;
}

ssize_t write(int fd, const void *buf, size_t nbytes){

    fprintf(stderr, "Client calling [write]\n");
    fprintf(stderr, "Client call [write] (fd)%d (nbytes)%zu\n", fd, nbytes);

    if(fd < FD_OFFSET) return orig_write(fd, buf, nbytes);
    else fd -=  FD_OFFSET;

    // Client send RPC.
    // We do not want one client occupies all the bandwidth
    if(nbytes > MAXPKGLEN) nbytes = MAXPKGLEN;

    char msg[MAXMSGLEN];
	char* msg_header = NULL;
	asprintf(&msg_header, "write %d %zu ", fd, nbytes);
	memcpy(msg , msg_header, strlen(msg_header));
	memcpy(msg + strlen(msg_header), buf, nbytes);
	send_message(msg, nbytes + strlen(msg_header));

	// Client get response.
	char *save_ptr;
	int ret = atoi(strtok_r (msg, " ", &save_ptr));
	errno = atoi(strtok_r (NULL, " ", &save_ptr));

	free(msg_header);

	fprintf(stderr,"Client [write] got message: (ret)%d (errno)%d\n", ret, errno);

	return ret;
}

__off_t lseek(int fd, off_t offset, int whence){

    fprintf(stderr, "Client calling [lseek]\n");
    fprintf(stderr, "Client call [lseek] (fd)%d (offset)%zu (whence)%d\n", fd, offset, whence);

    if(fd < FD_OFFSET) return orig_lseek(fd, offset, whence);
    else fd -=  FD_OFFSET;

    // Client send RPC.
	char * msg = NULL;
	asprintf(&msg, "lseek %d %zu %d", fd, offset, whence);
	send_message(msg, strlen(msg));

    // Client get response.
    char *save_ptr;
    int ret = atoi(strtok_r (msg, " ", &save_ptr));
    errno = atoi(strtok_r (NULL, " ", &save_ptr));

	free(msg);

	fprintf(stderr,"Client [lseek] got message: (ret)%d (errno)%d\n", ret, errno);

    return ret;
}

int __xstat(int ver, const char * pathname, struct stat * stat_buf){

    fprintf(stderr, "Client calling [xstat]\n");
    fprintf(stderr, "Client call [xstat] (ver)%d (pathname)%s\n", ver, pathname);

    // Client send RPC.
    char msg[MAXMSGLEN] = {};
    char* msg_header = NULL;
    asprintf(&msg_header, "stat %d %s", ver, pathname);
    memcpy(msg , msg_header, strlen(msg_header));

    fprintf(stderr, "Client before sending stat \n");
    print_stat(stat_buf);

	send_message(msg, strlen(msg));

	// Client get response.
    char *save_ptr;
    int ret = atoi(strtok_r (msg, " ", &save_ptr));
    errno = atoi(strtok_r (NULL, " ", &save_ptr));

    if(ret >= 0){
        memcpy(stat_buf, save_ptr, sizeof(struct stat));
        fprintf(stderr, "Client gets stat reply \n");
        print_stat(stat_buf);
    }

    free(msg_header);

    fprintf(stderr, "Client [xstat] got message: (ret)%d (errno)%d\n", ret, errno);

    return ret;
}

/* This function is purely for debugging purpose.
 *
 * It is called to print some values of struct stat
 * in order to check if any memory corruption happens.
 *********************************************************************/
void print_stat(struct stat *buf){
    fprintf(stderr, "-- print stat(st_atim) %ld\n",buf->st_size);
    fprintf(stderr, "-- print stat(st_blksize) %ld\n",buf->st_blksize);
    fprintf(stderr, "-- print stat(st_dev) %ld\n",buf->st_dev);
    fprintf(stderr, "-- print stat(st_uid) %ud\n",buf->st_uid);
    fprintf(stderr, "-- print stat(st_size) %ld\n",buf->st_size);
}

int unlink(const char *pathname)
{
    fprintf(stderr, "Client calling [unlink]\n");
    fprintf(stderr, "Client call [unlink] (pathname)%s\n", pathname);

    // Client send RPC.
    char * msg = NULL;
    asprintf(&msg, "unlink %s", pathname);
	send_message(msg, strlen(msg));

    // Client get response.
    char *save_ptr;
    int ret = atoi(strtok_r (msg, " ", &save_ptr));
    errno = atoi(strtok_r (NULL, " ", &save_ptr));

    free(msg);

    fprintf(stderr, "Client [unlink] got message: (ret)%d (errno)%d\n", ret, errno);

    return ret;
}

/* This function is a helper function.
 *
 * It is called to deserialize a struct dirtreenode from the pointer ptr.
 *********************************************************************/
void* deserialize(void* ptr, struct dirtreenode** tree){
    *tree = malloc(sizeof(struct dirtreenode));

    int name_len = *(int32_t*)ptr;
    fprintf(stderr, "[deserialize] name length: %d\n", name_len);
    ptr += 4;

    (*tree)->name = malloc(name_len+1);
    memcpy((*tree)->name, ptr, name_len);
    (*tree)->name[name_len] = '\0';

    fprintf(stderr, "[deserialize] tree name: %s\n", (*tree)->name);
    ptr += name_len;

    (*tree)->num_subdirs = *(int32_t*)ptr;
    ptr += 4;

    if((*tree)->num_subdirs > 0){
        (*tree)->subdirs = malloc((*tree)->num_subdirs * sizeof(struct dirtreenode));
    }else{
        (*tree)->subdirs = NULL;
    }

    int i;
    for(i=0; i< (*tree)->num_subdirs; i++){
        ptr = deserialize(ptr, &((*tree)->subdirs[i]));
    }

    return ptr;
}

struct dirtreenode* getdirtree(const char *pathname) {

    fprintf(stderr, "Client calling [getdirtree]\n");
    fprintf(stderr, "Client call [getdirtree] (pathname)%s\n", pathname);

    // Client send RPC.
    char msg[MAXMSGLEN] = {};
    void * ptr = (void *)&msg[0];
    char* msg_header = NULL;
    asprintf(&msg_header, "getdirtree %s", pathname);
    memcpy(msg , msg_header, strlen(msg_header));
	send_message(msg, strlen(msg));

	// Client get response.
	struct dirtreenode* tree;
	deserialize(ptr, &tree);
	free(msg_header);

	return tree;
}

ssize_t getdirentries(int fd, char *buf, size_t nbytes , off_t *basep){

	fprintf(stderr, "Client calling [getdirentries]\n");
	fprintf(stderr, "Client call [getdirentries] (fd)%d (nbytes)%zu (basep)%ld\n", fd, nbytes, *basep);

	if(fd < FD_OFFSET) return orig_getdirentries(fd, buf, nbytes, basep);
    else fd -=  FD_OFFSET;

	 // Client send RPC.
    char msg[MAXMSGLEN] = {};
    char* msg_header = NULL;
    asprintf(&msg_header, "getdirentries %d %zu %ld", fd, nbytes, *basep);
    memcpy(msg , msg_header, strlen(msg_header));
	send_message(msg, strlen(msg));

	// Client get response.
    void * ptr = (void *)&msg[0];
    ssize_t ret = *(ssize_t *)ptr;
    ptr += 4;

    errno = *(int *)ptr;
    ptr += 4;

    long new_basep = *(off_t *)ptr;
    ptr += 8;

    memcpy(buf, ptr, ret);
    *basep = new_basep;

	free(msg_header);

    fprintf(stderr,"client [getdirentries] got message: (ret)%zu (errno)%d (basep)%ld\n", ret, errno, new_basep);

    return ret;
}

void freedirtree( struct dirtreenode* dt ){
	return orig_freedirtree(dt);
}

void send_message(char * msg, ssize_t size){
	char *serverip;
	char *serverport;
	unsigned short port;
	int sockfd, rv;
	struct sockaddr_in srv;

	// Get environment variable indicating the ip address of the server
	serverip = getenv("server15440");
	if (serverip) {
	    fprintf(stderr, "Got environment variable server15440: %s\n", serverip);
	}
	else {
	    fprintf(stderr, "Environment variable server15440 not found.  Using 127.0.0.1\n");
		serverip = "127.0.0.1";
	}

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) fprintf(stderr, "Got environment variable serverport15440: %s\n", serverport);
	else serverport = "15440";

	port = (unsigned short)atoi(serverport);

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error

	// setup address structure to point to server
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = inet_addr(serverip);	// IP address of server
	srv.sin_port = htons(port);			// server port

	// actually connect to the server
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);

	// send message to server
	send(sockfd, msg, size, 0);	// send message; should check return value

    // get message back
    rv = recv(sockfd, msg, MAXMSGLEN, 0);   // get message
    if (rv<0) err(1,0);         // in case something went wrong

	msg[rv]=0;				// null terminate string to print
	fprintf(stderr, "Client [receive msg] got messge: (msg len)%d\n", rv);

	// close socket
	orig_close(sockfd);

}

// This function is automatically called when program is started
void _init(void) {
	orig_open = dlsym(RTLD_NEXT, "open");
	orig_close = dlsym(RTLD_NEXT, "close");
	orig_read = dlsym(RTLD_NEXT, "read");
	orig_write = dlsym(RTLD_NEXT, "write");
	orig_lseek = dlsym(RTLD_NEXT, "lseek");
	orig_stat = dlsym(RTLD_NEXT, "__xstat");
	orig_unlink = dlsym(RTLD_NEXT, "unlink");
	orig_getdirentries = dlsym(RTLD_NEXT, "getdirentries");
	orig_getdirtree = dlsym(RTLD_NEXT, "getdirtree");
	orig_freedirtree = dlsym(RTLD_NEXT, "freedirtree");
}

