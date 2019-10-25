#!/bin/bash

g++ create.cc `pkg-config --cflags --libs arrow plasma` --std=c++11 -o create
g++ get.cc `pkg-config --cflags --libs arrow plasma` --std=c++11 -o get
g++ subscribe.cc `pkg-config --cflags --libs arrow plasma` --std=c++11 -o subscribe
