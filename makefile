all:
	g++ -g -o prefix_sum -lpthread prefix_sum.cpp
	gcc  -g -o quicksort -lpthread -lm quicksort.c
