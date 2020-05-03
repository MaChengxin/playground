all: flight-sender flight-receiver
	rm -f *o

flight-sender: flight_sender.o common.o plasma_access.o
	g++ -g -o flight-sender flight_sender.o common.o plasma_access.o `pkg-config --cflags --libs arrow arrow-flight gflags plasma` -lboost_system

flight-receiver: flight_receiver.o common.o plasma_access.o
	g++ -g -o flight-receiver flight_receiver.o common.o plasma_access.o `pkg-config --cflags --libs arrow arrow-flight gflags plasma` -lboost_system

flight_sender.o: flight_sender.cc
	g++ -g -c flight_sender.cc

flight_receiver.o: flight_receiver.cc
	g++ -g -c flight_receiver.cc

common.o: common.cc common.h
	g++ -g -c common.cc

plasma_access.o: plasma_access.cc plasma_access.h
	g++ -g -c plasma_access.cc

.PHONY: clean
clean:
	rm -f flight-sender flight-receiver
