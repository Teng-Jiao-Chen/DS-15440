all: mylib.so server

server: server.c 
	gcc  -o server server.c -Wall -fPIC -DPIC -L../include -L../lib ../lib/libdirtree.so -lpthread    

mylib.o: mylib.c
	gcc -I../include -Wall -fPIC -DPIC -c mylib.c 

mylib.so: mylib.o
	ld -shared -o mylib.so mylib.o -ldl

clean:
	rm -f *.o *.so

