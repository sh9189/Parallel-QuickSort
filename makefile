all:
	g++ -g -o prefix_sum -lpthread prefix_sum.cpp
	gcc  -o quicksort -lpthread -lm quicksort.c
