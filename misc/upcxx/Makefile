DIR:=${CURDIR}
CC=~/upcxx_lib/bin/upcxx
UPCXX_RUN=~/upcxx_lib/bin/upcxx-run

all: hello_world red_black_solver dmap

hello_world: hello_world.cpp
	${CC} -o hello_world -O hello_world.cpp

red_black_solver: red_black_solver.cpp
	${CC} -o red_black_solver -O red_black_solver.cpp

dmap: dmap.cpp dmap.hpp
	${CC} -o dmap -O dmap.cpp

run_hello_world:
	$(UPCXX_RUN) -n 4 $(DIR)/hello_world

run_red_black_solver:
	$(UPCXX_RUN) -n 4 $(DIR)/red_black_solver

run_dmap:
	$(UPCXX_RUN) -n 4 $(DIR)/dmap

clean:
	rm -f hello_world red_black_solver dmap
