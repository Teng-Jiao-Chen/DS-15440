#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
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
#include <dirent.h>
#include "../include/dirtree.h"

#define MAXMSGLEN 1005000
#define MAXPKGLEN 1024
#define FD_OFFSET 3000

#define OPEN            0
#define WRITE           1
#define CLOSE           2
#define READ            3
#define LSEEK           4
#define STAT            5
#define UNLINK          6
#define GETDIRTREE      7
#define GETDIRENTRIES   8
#define FREEDIRTREE     9

int close_handler(int sessfd, char* message);
int write_handler(int sessfd, char* message);
int open_handler(int sessfd, char* message);
int read_handler(int sessfd, char* message);
int lseek_handler(int sessfd, char* message);
int unlink_handler(int sessfd, char* message);
int stat_handler(int sessfd, char* message);
int getdirtree_handler(int sessfd, char* message);
int getdirentries_handler(int sessfd, char* message);

void* serialize_tree(void* ptr, struct dirtreenode* tree);
void* handle(void* data);
void print_stat(struct stat *buf); // Print several attributes of stat (for debugging purpose)

void helper_printtree(struct dirtreenode* tree);

int main(int argc, char**argv) {
	char *serverport;
	unsigned short port;
	int sockfd, sessfd, rv;
	struct sockaddr_in srv, cli;
	socklen_t sa_size;
	
	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) port = (unsigned short)atoi(serverport);
	else port=15440;
	
	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error
	
	// setup address structure to indicate server port
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = htonl(INADDR_ANY);	// don't care IP address
	srv.sin_port = htons(port);			// server port

	// bind to our port
	rv = bind(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);
	
	// start listening for connections
	rv = listen(sockfd, 5);
	if (rv<0) err(1,0);
	
	// main server loop, handle clients one at a time, quit after 10 clients
	while( 1 ) {
		
		// wait for next client, get session socket
		sa_size = sizeof(struct sockaddr_in);
		sessfd = accept(sockfd, (struct sockaddr *)&cli, &sa_size);
		if (sessfd<0) err(1,0);
		
		// Here we use threads to handle the concurrent accesses
		pthread_t th;
		int ret_create = pthread_create(&th, NULL, handle, (void*)(intptr_t)sessfd);
		int ret_detach = pthread_detach(th);

		if(ret_create || ret_detach) close(sessfd);
	}

	// close socket
	close(sockfd);

	return 0;
}

int open_handler(int sessfd, char* message) {

    fprintf(stderr,"Server calling [open_handler]\n");

    // parse the information
    char* pathname = strtok_r (NULL, " ", &message);
    int flags = atoi(strtok_r (NULL, " ", &message));
    mode_t m = atoi(strtok_r (NULL, " ", &message));

    fprintf(stderr, "Server got call [open] (pathname)%s (flags)%d (mode)%u\n", pathname, flags, m);

    // execute the function
    int ret = open(pathname, flags, m);

    // respond client PRC
    char *msg = NULL;
    asprintf(&msg, "%d %d", ret, errno);
    send(sessfd, msg, strlen(msg), 0);
    free(msg);

    fprintf(stderr, "Server send reply message [open]: (ret)%d (errno)%d\n", ret, errno);

    return EXIT_SUCCESS;
}

int close_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [open_handler]\n");

    // parse the information
    int fd = atoi(message);

    fprintf(stderr, "Server got call [close] (fd)%d\n", fd);

    // execute the function
    int ret = close(fd);

    // respond client PRC
    char *msg = NULL;
    asprintf(&msg, "%d %d", ret, errno);
    send(sessfd, msg, strlen(msg), 0);
    free(msg);

    fprintf(stderr, "Server send reply message [close]: (ret)%d (errno)%d\n", ret, errno);

    return EXIT_SUCCESS;
}

int lseek_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [lseek_handler]\n");

    // parse the information
    char* fd_s = strtok_r (NULL, " ", &message);
    int fd = atoi(fd_s);

    char* offset_s = strtok_r (NULL, " ", &message);
    int offset = atoi(offset_s);

    int whence = atoi(message);

    fprintf(stderr, "Server got call [lseek] (fd)%d (offset)%d (whence)%d\n", fd, offset, whence);

    // execute the function
    int ret = lseek(fd, offset, whence);

    // respond client PRC
    char *msg = NULL;
    asprintf(&msg, "%d %d", ret, errno);
    send(sessfd, msg, strlen(msg), 0);
    free(msg);

    fprintf(stderr, "Server send reply message [lseek]: (ret)%d (errno)%d\n", ret, errno);

    return EXIT_SUCCESS;
}

int unlink_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [unlink_handler]\n");

    // execute the function
    int ret = unlink(message);

    fprintf(stderr, "Server got call [unlink]: (pathname)%s\n", message);

    // respond client PRC
    char *msg = NULL;
    asprintf(&msg, "%d %d", ret, errno);
    send(sessfd, msg, strlen(msg), 0);

    free(msg);

    fprintf(stderr, "Server send reply message [unlink]: (ret)%d (errno)%d\n", ret, errno);

    return EXIT_SUCCESS;
}

int stat_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [stat_handler]\n");

    // parse the information
    char* ver_s = strtok_r (NULL, " ", &message);
    int ver = atoi(ver_s);

    fprintf(stderr, "Server got call [stat]: (ver)%d (pathname)%s\n", ver, message);

    // execute the function
    struct stat buf;
    int ret = __xstat(ver, message, &buf);

    // respond client PRC
    char msg[MAXMSGLEN];
    sprintf(msg, "%d %d ", (int)ret, errno);
    int header_len = strlen(msg);
    memcpy(msg + header_len, (void *)&buf, sizeof(struct stat));
    send(sessfd, msg, header_len + sizeof(struct stat), 0);

    fprintf(stderr, "Server send reply message [stat]: (msg_len)%ld\n", header_len + sizeof(struct stat));
    fprintf(stderr, "Server send reply message [stat]: (ret)%d (errno)%d\n", ret, errno);

    fprintf(stderr, "Server [stat] reply print stat\n");
    print_stat(&buf);

    return EXIT_SUCCESS;
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

int read_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [read_handler]\n");

    // parse the information
    char* fd_s = strtok_r (NULL, " ", &message);
    int fd = atoi(fd_s);

    char* n_s = strtok_r (NULL, " ", &message);
    size_t n = atoi(n_s);

    fprintf(stderr, "Server got call [read] (fd)%d (size)%zu\n", fd, n);

    // We do not want one client occupies all the bandwidth
    if(n > MAXPKGLEN) n = MAXPKGLEN;

    // execute the function
    void *buf = malloc(n+1);
    int ret = read (fd, buf, n);

    // respond client PRC
    char msg[MAXMSGLEN];
    sprintf(msg, "%d %d ", (int)ret, errno);

    int header_len = strlen(msg);

    if(ret >= 0){
        // if (ret < 0) --> memcpy will cause corruption
        memcpy(msg + header_len, buf, ret);
        send(sessfd, msg, header_len + ret, 0);
    }else{
        send(sessfd, msg, header_len, 0);
    }

    free(buf);

    fprintf(stderr, "Server send reply message [read] (ret)%d (errno)%d \n", (int)ret, errno);

    return EXIT_SUCCESS;
}

int write_handler(int sessfd, char* message){

    fprintf(stderr, "Server calling [write_handler]\n");

    // parse the information
    char* fd_s = strtok_r (NULL, " ", &message);
    int fd = atoi(fd_s);

    char* n_s = strtok_r (NULL, " ", &message);
    size_t n = atoi(n_s);

    fprintf(stderr, "Server got call [write] (fd)%d (n)%zu\n", fd, n);

    // execute the function
    ssize_t ret = 0;
    if(fd > 0){
        ret = write(fd, message, n);
    }else{
        ret = -1;
    }

    // respond client PRC
    char *msg = NULL;
    asprintf(&msg, "%zu %d", ret, errno);
    send(sessfd, msg, strlen(msg), 0);

    free(msg);

    fprintf(stderr, "Server send reply message [write] (ret)%d (errno)%d \n", (int)ret, errno);

    return EXIT_SUCCESS;
}

/* This function is a helper function.
 *
 * It is called to serialize a struct dirtreenode to the pointer ptr.
 *********************************************************************/
void* serialize_tree(void* ptr, struct dirtreenode* tree){
    if(tree == NULL) return ptr;

    char* name = tree->name;
    int name_len = strlen(name);
    int num_sub = tree->num_subdirs;

    fprintf(stderr, "[serialize] name length: %d\n", name_len);
    fprintf(stderr, "[serialize] tree name: %s\n", name);

    // serialize name
    memcpy(ptr, &name_len, 4);
    ptr += 4;

    memcpy(ptr, name, name_len);
    ptr += name_len;

    // serialize num_sub
    memcpy(ptr, &num_sub, 4);
    ptr += 4;

    int i;
    for(i=0; i< num_sub; i++){
        ptr = serialize_tree(ptr, tree->subdirs[i]);
    }

    return ptr;
}

/* This is a helper function to print the dir tree node
 *
 *********************************************************/
void helper_printtree(struct dirtreenode* tree){
    fprintf(stderr, "tree name: %s\n",tree->name);
    fprintf(stderr, "subtree num: %d\n",tree->num_subdirs);

    int i;

    for(i=0;i<tree->num_subdirs;i++){
        fprintf(stderr, "--\t");
        helper_printtree( tree->subdirs[i] );
    }
}

int getdirtree_handler(int sessfd, char* message){

    fprintf(stderr,"Server calling [getdirtree_handler]\n");

    fprintf(stderr,"Server got call [getdirtree] (pathname)%s\n", message);

    // execute the function
    struct dirtreenode* tree = getdirtree(message);
    helper_printtree(tree);

    // respond client PRC
    char msg[MAXMSGLEN];
    void * ptr = (void *)&msg[0];
    int len = serialize_tree(ptr , tree) - ptr;
    send(sessfd, msg, len, 0);

    freedirtree(tree);

    fprintf(stderr,"Server [getdirtree] (serialized len)%d\n", len);

    return EXIT_SUCCESS;
}

int getdirentries_handler(int sessfd, char* message){

    fprintf(stderr,"Server calling [getdirentries_handler]\n");

    int fd = atoi(strtok_r (NULL, " ", &message));
    size_t nbytes = atoi(strtok_r (NULL, " ", &message));
    off_t basep = atol(strtok_r (NULL, " ", &message));

    fprintf(stderr,"Server got call [getdirentries]: (fd)%d (nbytes)%zu (basep)%ld\n", fd, nbytes, basep);

    char *buf = malloc(nbytes + 1);
    int ret = getdirentries(fd, buf, nbytes , &basep);

    // serialize everything
    char msg[MAXMSGLEN];
    void * ptr = (void *)&msg[0];
    memcpy(ptr, &ret, 4);
    ptr += 4;

    memcpy(ptr, &errno, 4);
    ptr += 4;

    // serialize num_sub
    memcpy(ptr, &basep, 8);
    ptr += 8;

    // respond client PRC
    if(ret > 0){
        memcpy(ptr, buf, ret);
        send(sessfd, msg, 16 + ret, 0);
    } else{
        send(sessfd, msg, 16, 0);
    }

    free(buf);

    fprintf(stderr,"getdirentries reply: (ret)%d (errno)%d (basep)%ld\n", ret, errno, basep);

    return EXIT_SUCCESS;
}


void* handle(void* data){
    int sessfd = (intptr_t) data;
    char buf[MAXMSGLEN+1];
    int rv;

    while ( (rv=recv(sessfd, buf, MAXMSGLEN, 0)) > 0) {
        buf[rv]=0;      // null terminate string to print
        fprintf(stderr, "(new sessfd)-------------\n");
        fprintf(stderr, "(sessfd)%d\n", sessfd);
        fprintf(stderr, "(msglen)%zu\n", strlen(buf));

        char *token, *save_ptr;

        token = strtok_r ((char *)buf, " ", &save_ptr);

        int key = -1;

        if(strcmp(token, "open") == 0) key = OPEN;
        else if(strcmp(token, "write") == 0) key = WRITE;
        else if(strcmp(token, "close") == 0) key = CLOSE;
        else if(strcmp(token, "read") == 0) key = READ;
        else if(strcmp(token, "lseek") == 0) key = LSEEK;
        else if(strcmp(token, "unlink") == 0) key = UNLINK;
        else if(strcmp(token, "stat") == 0) key = STAT;
        else if(strcmp(token, "getdirtree") == 0) key = GETDIRTREE;
        else if(strcmp(token, "getdirentries") == 0) key = GETDIRENTRIES;

        switch (key) {
            case OPEN:{
                open_handler(sessfd, save_ptr);
                break;
            }
            case WRITE:{
                write_handler(sessfd, save_ptr);
                break;
            }
            case CLOSE:{
                close_handler(sessfd, save_ptr);
                break;
            }
            case READ:{
                read_handler(sessfd, save_ptr);
                break;
            }
            case LSEEK:{
                lseek_handler(sessfd, save_ptr);
                break;
            }
            case UNLINK:{
                unlink_handler(sessfd, save_ptr);
                break;
            }
            case STAT:{
                stat_handler(sessfd, save_ptr);
                break;
            }
            case GETDIRTREE:{
                getdirtree_handler(sessfd, save_ptr);
                break;
            }
            case GETDIRENTRIES:{
                getdirentries_handler(sessfd, save_ptr);
                break;
            }
            default:
                fprintf(stderr, "(unkown operation)\n");
                break;
        }

    }

    close(sessfd);
    return EXIT_SUCCESS;
}

