#!/bin/bash

g++ -g retrieve.cc `pkg-config --cflags --libs arrow plasma` --std=c++11 -o retrieve
