CC=g++
EXECS=my_flight_server

all: ${EXECS}

my_flight_server: my_flight_server.cc
	${CC} my_flight_server.cc -o my_flight_server `pkg-config --cflags --libs arrow gflags` -larrow_flight -std=c++11 

clean:
	rm -f ${EXECS}
