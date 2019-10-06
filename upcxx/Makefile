DIR:=${CURDIR}
EXECS=hello_world
CC=~/upcxx_lib/bin/upcxx
UPCXX_RUN=~/upcxx_lib/bin/upcxx-run

all: ${EXECS}

hello_world: hello_world.cpp
	${CC} -o hello_world -O hello_world.cpp

run:
	$(UPCXX_RUN) -n 4 $(DIR)/$(EXECS)

clean:
	rm -f ${EXECS}
