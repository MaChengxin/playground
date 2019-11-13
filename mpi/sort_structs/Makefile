EXECS=sort_structs
MPICC?=mpicc

all: ${EXECS}

sort_structs: sort_structs.c
	${MPICC} -o sort_structs sort_structs.c

clean:
	rm -f ${EXECS}