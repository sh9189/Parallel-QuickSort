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
#define MAX_NUM 250000000
#define MAX_THREADS 4 
typedef struct barrier_node {
	pthread_mutex_t count_lock;
	pthread_cond_t ok_to_proceed_up;
	pthread_cond_t ok_to_proceed_down;
	int count;
} mylib_barrier_t_internal;

typedef struct barrier_node mylob_logbarrier_t[MAX_THREADS];


void mylib_init_barrier(mylob_logbarrier_t b)
{
	int i;
	for (i = 0; i < MAX_THREADS; i++) {
		b[i].count = 0;
		pthread_mutex_init(&(b[i].count_lock), NULL);
		pthread_cond_init(&(b[i].ok_to_proceed_up), NULL);
		pthread_cond_init(&(b[i].ok_to_proceed_down), NULL);
	}
}

void mylib_logbarrier (mylob_logbarrier_t b, int num_threads, int thread_id)
{
	int i, base, index;
	i = 2;
	base = 0;

	do {
		index = base + thread_id / i;
		if (thread_id % i == 0) {
			pthread_mutex_lock(&(b[index].count_lock));
			b[index].count ++;
			while (b[index].count < 2)
				pthread_cond_wait(&(b[index].ok_to_proceed_up),
						&(b[index].count_lock));
			pthread_mutex_unlock(&(b[index].count_lock));
		}
		else {
			pthread_mutex_lock(&(b[index].count_lock));
			b[index].count ++;
			if (b[index].count == 2)
				pthread_cond_signal(&(b[index].ok_to_proceed_up));
			/*
			while (b[index].count != 0)
			 */
			while (
					pthread_cond_wait(&(b[index].ok_to_proceed_down),
							&(b[index].count_lock)) != 0);
			pthread_mutex_unlock(&(b[index].count_lock));
			break;
		}
		base = base + MAX_THREADS/i;
		i = i * 2;
	} while (i <= MAX_THREADS);

	i = i / 2;

	for (; i > 1; i = i / 2)
	{
		base = base - MAX_THREADS/i;
		index = base + thread_id / i;
		pthread_mutex_lock(&(b[index].count_lock));
		b[index].count = 0;
		pthread_cond_signal(&(b[index].ok_to_proceed_down));
		pthread_mutex_unlock(&(b[index].count_lock));
	}
}


struct thread_info_type
{
	int start; // starting index of array
	int end; // ending index of array
	int threadId;

};

void * parallel_prefix_sum(void *);
void upSweep(int myId);
void downSweep(int myId);
void printArray(int arr[],int numElements);

mylob_logbarrier_t barr;
int inputArr[MAX_NUM]; // = {3,1,7,0,4,1,6,3};
int checkArr[MAX_NUM];
int sumArr[MAX_THREADS*2];

void parallel_prefix_sum_main()
{
	mylib_init_barrier (barr);

	int numElements = MAX_NUM;
	int numThreads = MAX_THREADS;

	//cout << "Input array is " << endl;
	//printArray(a,numElements);

	pthread_t *p_threads= new pthread_t[numThreads];
	int elementsPerThread = numElements/numThreads;
	int excessElements = numElements%numThreads;
	//for(int i=0;i<2*numThreads;i++)
	//sumArr[i]=0;
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
	int middle = start + numElements/2; // middle element Index

	//cout << "Start is " << start << " End is " << end << " middle is "<<middle<<" id is" << myId<<endl;

	/*cout << "First Half" << endl;
	for(int i=start;i< middle;i++)
		cout<< myId << " " << inputArr[i] << endl;

	cout << "Second Half" << endl;
	for(int i=middle;i<= end;i++)
		cout<< myId << " " << inputArr[i] << endl;*/


	// compute local sum and put it in sum array
	/*int firstHalfSum = 0 , secondHalfSum = 0;
	for(int i=start;i<middle;i++) // compute first half sum
		firstHalfSum += inputArr[i];

	for(int i=middle; i<= end;i++) // compute second half sum
		secondHalfSum += inputArr[i];*/

	// compute local prefix sums

	for(int i=start+1;i<middle;i++)
		inputArr[i]+=inputArr[i-1];

	for(int i=middle+1;i<=end;i++)
		inputArr[i]+=inputArr[i-1];

	if(start == middle && middle == end) // one element per thread
	{
		sumArr[2*myId+1] = inputArr[start];
		//cout << "Thread id is " << myId << "Start is " << start <<endl;
	}
	else
	{
		sumArr[2*myId] = inputArr[middle-1];
		sumArr[2*myId + 1] = inputArr[end];
	}


	//barrier to synchronize local sums
	mylib_logbarrier(barr, numThreads, myId);

	/*cout << "Sum array is ";
	for(int i=0;i<2*numThreads;i++)
		cout << sumArr[i] << " ";
	cout << endl;

	cout << "Input array is ";
	for(int i=0;i<MAX_NUM;i++)
		cout << i << " ";
	cout << endl;*/

	// up sweep

	upSweep(myId);

	/*cout << "After Up sweep Sum array is" <<endl;
	for(int i=0;i<2*numThreads;i++)
		cout << sumArr[i] << " ";
	cout << endl;*/
	// down sweep


	downSweep(myId);

	/*cout << "After Down sweep Sum array is" <<endl;
	for(int i=0;i<2*numThreads;i++)
		cout << sumArr[i] << " ";
	cout << endl;*/

	//compute complete prefix sum
	for(int i=start;i<middle;i++)
		inputArr[i]+= sumArr[2*myId];
	for(int i=middle;i<=end;i++)
		inputArr[i]+= sumArr[2*myId+1];

	/*cout << "Input array is ";
	for(int i=0;i<MAX_NUM;i++)
		cout << inputArr[i] << " ";
	cout << endl;*/

}


void downSweep(int myId)
{
	int numThreads = MAX_THREADS;
	int numElements = 2*numThreads;
	//sumArr[numElements-1] = 0;
	//mylib_logbarrier(barr, numThreads, myId);
	int numSteps = log2(numElements) - 1;
	int pow1 = pow(2, numSteps+1),pow2;
	int index1,index2,dec_i,temp;
	for(int d = numSteps ; d >= 0;d--)
	{
		//int pow1 = pow(2,d+1);
		//int pow2 = pow(2,d);
		pow2 = pow1/2;

		for(int i=0 ; i < 2*numThreads ; i+= pow1)
		{
			if( i== 2*myId)
			{
				dec_i = i-1;
				index2 = pow2+dec_i;
				index1 = pow1+dec_i;
				temp = sumArr[index2];
				sumArr[index2] = sumArr[index1];
				sumArr[index1] += temp;
			}
		}
		mylib_logbarrier(barr, numThreads, myId);
		/*cout << "After Down sweep Sum array is" <<endl;
		for(int i=0;i<2*numThreads;i++)
			cout << sumArr[i] << " ";
		cout << endl;*/
		pow1 = pow2;
	}

}

void upSweep(int myId)
{
	int numThreads = MAX_THREADS;
	int numElements = 2*numThreads;
	int numSteps = log2(numElements);
	//int  i = 2*myId;
	int pow2 = 1,pow1;
	int index,dec_i;
	/*for(int d = 0; d < numSteps;d++)
	{
		//int pow1 = pow(2,d+1);
		//int pow2 = pow(2,d);
		pow1 = 2*pow2;
		if( d == numSteps-1)
			sumArr[numElements-1]=0;
		else
		{
			dec_i = i-1;
			index = dec_i+pow1;
			sumArr[index] = sumArr[dec_i+pow2]+sumArr[index];
		}
		mylib_logbarrier(barr, numThreads, myId);
		if(myId % pow1==0)
		{
			d++;
			for(;d<numSteps;d++)
				mylib_logbarrier(barr, numThreads, myId);
		}

		i-=pow1;
		pow2=pow1;
	}*/
	for(int d = 0; d < numSteps;d++)
	{
		//int pow1 = pow(2,d+1);
		//int pow2 = pow(2,d);
		pow1 = 2*pow2;
		if( d == numSteps-1)
			sumArr[numElements-1]=0;
		else
		{
			for(int i=0 ; i < 2*numThreads ; i+= pow1)
			{
				if( i== 2*myId)
				{
					dec_i = i-1;
					index = dec_i+pow1;
					sumArr[index] = sumArr[dec_i+pow2]+sumArr[index];
				}
			}
		}
		mylib_logbarrier(barr, numThreads, myId);
		pow2=pow1;
	}



}




int main()
{
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
	for(int i=0;i<MAX_NUM;i++)
		checkArr[i+1] += checkArr[i];
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



