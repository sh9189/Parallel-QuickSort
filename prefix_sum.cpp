/*
 * prefix_sum.cpp
 *
 *  Created on: Mar 23, 2012
 *      Author: skhajamo
 */
#include <pthread.h>
#include <iostream>
#include <math.h>
#include <time.h>
#include <sys/time.h>
using namespace std;

#define MAX_NUM 1024
#define MAX_THREADS 5

struct thread_info_type
{
	int start; // starting index of array
	int end; // ending index of array
	int threadId;

};

void * parallel_prefix_sum(void *);
void printArray(int arr[],int numElements);

pthread_barrier_t barr;
int inputArr[MAX_NUM]; // = {3,1,7,0,4,1,6,3};
int checkArr[MAX_NUM];
int sumArrSize = pow(2,ceil(log2(MAX_THREADS)));


int * sumArr = new int[sumArrSize];

void parallel_prefix_sum_main()
{
	if(pthread_barrier_init(&barr, NULL, MAX_THREADS))
	{
		cout << "Could not create a barrier\n";
	}

	int numElements = MAX_NUM;
	int numThreads = MAX_THREADS;

	//cout << "Input array is " << endl;
	//printArray(a,numElements);

	pthread_t *p_threads= new pthread_t[numThreads];
	int elementsPerThread = numElements/numThreads;
	int excessElements = numElements%numThreads;

	struct thread_info_type *threadInfoArr = new struct thread_info_type[numThreads];
	int lastIndex = 0;

	// assign ranges and create threads
	for(int i=0;i<numThreads;i++)
	{

		threadInfoArr[i].threadId = i;
		threadInfoArr[i].start = lastIndex;
		lastIndex+= (elementsPerThread -1);
		if(excessElements > 0)
		{
			lastIndex++;
			excessElements--;
		}
		threadInfoArr[i].end = lastIndex;

		pthread_create(&p_threads[i],NULL,parallel_prefix_sum,(void *)&(threadInfoArr[i]));
		lastIndex++; // increment lastIndex so that it is ready for next iteration
	}
	for (int i=0; i< numThreads; i++)
		pthread_join(p_threads[i], NULL);

	//cout << "Prefix Sum array is " << endl;
	//printArray(a,numElements);
}


void printArray(int arr[],int numElements)
{
	for(int i=0;i<numElements;i++)
		cout << arr[i] << " ";
	cout << endl;
}

void * parallel_prefix_sum(void * arg)
{
	struct thread_info_type threadInfo = *((struct thread_info_type *)arg);

	int start = threadInfo.start; // start element Index
	int end = threadInfo.end; // end element Index
	int myId = threadInfo.threadId;
	int numThreads = MAX_THREADS;
	int numElements = end-start+1;

	//cout << "Start is " << start << " End is " << end <<" id is" << myId<<endl;

	// compute local prefix sums

	for(int i=start+1;i<=end;i++)
		inputArr[i]+=inputArr[i-1];

	sumArr[myId] = inputArr[end];

	//barrier to synchronize local sums
	pthread_barrier_wait(&barr);

	/*cout << "Sum array is ";
	for(int i=0;i<sumArrSize;i++)
		cout << sumArr[i] << " ";
	cout << endl;

	cout << "Input array is ";
	for(int i=0;i<MAX_NUM;i++)
		cout << inputArr[i] << " ";
	cout << endl;*/

	// up sweep
	int numSteps = log2(sumArrSize);
	int pow2 = 1,pow1;
	int index,dec_i;

	for(int d = 0; d < numSteps;d++)
	{
		pow1 = 2*pow2;
		if( d == numSteps-1)
			sumArr[sumArrSize-1]=0;
		else
		{
			if(myId% pow1 ==0)
			{

				dec_i = myId-1;
				index = dec_i+pow1;
				sumArr[index] = sumArr[dec_i+pow2]+sumArr[index];
			}
		}
		pthread_barrier_wait(&barr);
		pow2=pow1;
	}

	/*cout << "Id is " << myId << " After Up sweep Sum array is" <<endl;
	for(int i=0;i<sumArrSize;i++)
		cout << sumArr[i] << " ";
	cout << endl;*/
	// down sweep

	int index1,index2,temp;
	pow1 = pow(2, numSteps);
	for(int d = numSteps-1 ; d >= 0;d--)
	{
		pow2 = pow1/2;
		if(myId% pow1 ==0)
		{
			dec_i = myId-1;
			index2 = pow2+dec_i;
			index1 = pow1+dec_i;
			temp = sumArr[index2];
			sumArr[index2] = sumArr[index1];
			sumArr[index1] += temp;
		}
		pthread_barrier_wait(&barr);
		pow1 = pow2;
	}

	/*
	cout <<  "Id is " << myId << "After Down sweep Sum array is" <<endl;
	for(int i=0;i<sumArrSize;i++)
		cout << sumArr[i] << " ";
	cout << endl;*/

	//compute complete prefix sum
	for(int i=start;i<=end;i++)
		inputArr[i]+= sumArr[myId];
}


int main()
{
	//cout << "Sum Arr size is "<< sumArrSize << endl;
	for(int i=0;i<MAX_NUM;i++)
	{
		inputArr[i] = i+1;
		checkArr[i] = inputArr[i];
	}
	struct timeval tz;
	struct timezone tx;
	double start_time, end_time;


	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	parallel_prefix_sum_main();
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;

	double ptime =(double)end_time - (double)start_time;

	cout << "Parallel Time is " << ptime;

	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	for(int i=1;i<MAX_NUM;i++)
		checkArr[i] += checkArr[i-1];
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	double stime = ((double)end_time - (double)start_time);
	cout << "Serial Time is " << stime;

	cout << "Speedup is "<< stime/ptime <<endl;

	for(int i=0;i<MAX_NUM;i++)
	{
		if(inputArr[i]!=checkArr[i])
			cout << "Error " << "i is "<<i<< " parallel result is "<<inputArr[i] << "serial result is " <<checkArr[i]<<endl;
	}

}



