all: send-to-dest receive-and-store
	rm -f *o

send-to-dest: send_to_dest.o common.o in_memory_storage.o sender.o
	g++ -g -o send-to-dest send_to_dest.o common.o in_memory_storage.o sender.o `pkg-config --cflags --libs arrow arrow-flight arrow-glib gflags plasma` -lboost_system

receive-and-store: receive_and_store.o common.o in_memory_storage.o
	g++ -g -o receive-and-store receive_and_store.o common.o in_memory_storage.o `pkg-config --cflags --libs arrow arrow-flight arrow-glib gflags plasma` -lboost_system

send_to_dest.o: send_to_dest.cc
	g++ -g -c send_to_dest.cc

receive_and_store.o:
	g++ -g -c receive_and_store.cc

common.o: common.cc common.h
	g++ -g -c common.cc

in_memory_storage.o: in_memory_storage.cc in_memory_storage.h
	g++ -g -c in_memory_storage.cc

sender.o: sender.cc sender.h
	g++ -g -c sender.cc

.PHONY: clean
clean:
	rm -f send-to-dest receive-and-store
