/*
 * quicksort.cpp
 *
 *  Created on: Mar 29, 2012
 *      Author: skhajamo
 */


/*
 * prefix_sum.cpp
 *
 *  Created on: Mar 23, 2012
 *      Author: skhajamo
 */
#include <pthread.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#define MAX_NUM 16
#define MAX_THREADS 4

struct thread_info_type
{
	int *inputArr;
	int *lsumArr;
	int *rsumArr;
	int *mirrorArr;
	int start; // starting index of array
	int end; // ending index of array
	int threadId;
	int leaderId;
	int pivotIndex;
	int threadsInPartition;
	int totalElementsInPartition;
	pthread_barrier_t *commonBarr;
	pthread_barrier_t *ownBarr;
};

struct thread_local_type
{
	int myId;
	struct thread_info_type * threadInfoArr;
};


int compare (const void * a, const void * b)
{
	return ( *(int*)a - *(int*)b );
}



void * parallel_quick_sort(void *);
void printArray(int arr[],int start,int end);
void swap(int *x,int *y);
void validate(int* output, int num_elements);


int *pqsort(int* inputArr, int numElements, int numThreads)
{
	//int sumArrSize = pow(2,ceil(log2(numThreads)));
	int *lsumArr = (int *)malloc(sizeof(int)*numThreads);
	int *rsumArr = (int *)malloc(sizeof(int)*numThreads);
	pthread_barrier_t commonBarr[MAX_THREADS];
	pthread_barrier_t ownBarr[MAX_THREADS];
	int *mirrorArr = (int *)malloc(sizeof(int)*numElements);
	for(int i=0;i<MAX_THREADS;i++)
	{
		if(pthread_barrier_init(&ownBarr[i], NULL, 2))
		{
			printf("Could not create barrier %d\n",i);
		}
	}
	if(pthread_barrier_init(&commonBarr[0], NULL, MAX_THREADS))
	{
		printf("Could not create barrier %d\n",0);
	}
	//cout << "Input array is " << endl;
	//printArray(a,numElements);

	pthread_t *p_threads= (pthread_t *)malloc(sizeof(pthread_t)*numThreads);
	int elementsPerThread = numElements/numThreads;
	int excessElements = numElements%numThreads;

	struct thread_info_type *threadInfoArr = (struct thread_info_type *)malloc(sizeof(struct thread_info_type)*numThreads);
	int lastIndex = 0;

	// assign ranges and create threads
	for(int i=0;i<numThreads;i++)
	{
		threadInfoArr[i].totalElementsInPartition = numElements;
		threadInfoArr[i].threadId = i;
		threadInfoArr[i].start = lastIndex;
		threadInfoArr[i].leaderId = 0; // initially thread 0 is leader
		threadInfoArr[i].inputArr = inputArr;
		threadInfoArr[i].lsumArr = lsumArr;
		threadInfoArr[i].rsumArr = rsumArr;
		threadInfoArr[i].pivotIndex = 0; // pivot is at position 0
		threadInfoArr[i].mirrorArr = mirrorArr;
		threadInfoArr[i].commonBarr = commonBarr;
		threadInfoArr[i].ownBarr = ownBarr;
		threadInfoArr[i].threadsInPartition = numThreads;
		lastIndex+= (elementsPerThread -1);
		if(excessElements > 0)
		{
			lastIndex++;
			excessElements--;
		}
		threadInfoArr[i].end = lastIndex;
		lastIndex++; // increment lastIndex so that it is ready for next iteration
	}
	struct thread_local_type * threadLocalArr = (struct thread_local_type *)malloc(sizeof(struct thread_local_type)*numThreads);

	for(int i=0;i<numThreads;i++)
	{
		threadLocalArr[i].myId = i;
		threadLocalArr[i].threadInfoArr = threadInfoArr;
		pthread_create(&p_threads[i],NULL,parallel_quick_sort,&threadLocalArr[i]);
	}

	for (int i=0; i< numThreads; i++)
		pthread_join(p_threads[i], NULL);

	free(lsumArr);
	free(rsumArr);
	free(threadInfoArr);
	free(threadLocalArr);
	free(mirrorArr);
	//cout << "Prefix Sum array is " << endl;
	//printArray(a,numElements);
}


void printArray(int arr[],int start,int end)
{
	for(int i=start;i<=end;i++)
		printf("%d ",arr[i]);
	printf("\n");
}

void * parallel_quick_sort(void * arg)
{

	struct thread_local_type threadLocal = *((struct thread_local_type *)arg);
	struct thread_info_type * threadInfoArr = threadLocal.threadInfoArr;

	int *inputArr = threadInfoArr[threadLocal.myId].inputArr;
	int *lsumArr = threadInfoArr[threadLocal.myId].lsumArr;
	int *rsumArr = threadInfoArr[threadLocal.myId].rsumArr;
	int *mirrorArr = threadInfoArr[threadLocal.myId].mirrorArr;
	int myId = threadLocal.myId;

	pthread_barrier_t *commonBarr = threadInfoArr[threadLocal.myId].commonBarr;
	pthread_barrier_t *ownBarr = threadInfoArr[threadLocal.myId].ownBarr;
	int *currentArr;
	int *currentMirrorArr;

	int ping=1;

	while(1)
	{
		struct thread_info_type threadInfo = threadInfoArr[threadLocal.myId];

		int start = threadInfo.start; // start element Index
		int end = threadInfo.end; // end element Index
		int leaderId = threadInfo.leaderId;
		int numElements = end-start+1;
		int pivotIndex = threadInfo.pivotIndex;
		int totalElementsInPartition = threadInfo.totalElementsInPartition;
		int threadsInPartition = threadInfo.threadsInPartition;


		printf("Start is %d End is %d MyId is %d\n",start,end,myId);
		printf("leaderid is %d pivotIndex is %d MyId is %d\n",leaderId,pivotIndex,myId);

		if(ping)
		{
			currentArr = inputArr;
			currentMirrorArr = mirrorArr;
		}
		else
		{
			currentArr = mirrorArr;
			currentMirrorArr = inputArr;

		}
		int pivotElement = currentArr[pivotIndex];

		if(threadsInPartition == 1) // terminating condition
		{
			if(currentArr != inputArr)
			{
				for(int i=start;i<=end;i++)
					inputArr[i] = mirrorArr[i];
			}
			qsort(inputArr+start,end-start+1,sizeof(int),compare);
			break;

		}



		/*int leftIndex = start;
		int rightIndex = end;
		while(leftIndex < rightIndex)
		{
			while(leftIndex <= end && currentArr[leftIndex]< pivotElement)
				leftIndex++;
			while(rightIndex >= start && currentArr[rightIndex]>= pivotElement)
				rightIndex--;
			if(leftIndex < rightIndex)
				swap(&currentArr[leftIndex],&currentArr[rightIndex]);
		}*/
		int firstIndex,secondIndex;
		if(myId == leaderId)
		{
			firstIndex = start+1;
			secondIndex = start+1;

		}
		else
		{
			firstIndex = start;
			secondIndex = start;
		}
		while(secondIndex <= end)
		{
			if(currentArr[secondIndex] < pivotElement)
			{
				swap(&currentArr[firstIndex],&currentArr[secondIndex]);
				firstIndex++;
			}
			secondIndex++;
		}

		printf("Id is %d Out of Loop\n",myId);

		int numLessElements,numGreaterElements,lessStartIndex,greaterStartIndex;
		if(myId == leaderId)
		{
			numLessElements = firstIndex - start -1;
			numGreaterElements = numElements - numLessElements;
			lessStartIndex = start+1;
			greaterStartIndex = firstIndex;
		}
		else
		{
			numLessElements = firstIndex - start;
			numGreaterElements = numElements - numLessElements;
			lessStartIndex = start;
			greaterStartIndex = firstIndex;
		}
		lsumArr[myId] = numLessElements;
		rsumArr[myId] = numGreaterElements;
		pthread_barrier_wait(&commonBarr[leaderId]);

		if(myId == leaderId)
		{
			printf("Id is %d CurrentArr is\n",myId);
			printArray(currentArr,start,start+totalElementsInPartition-1);
			printf("Id is %d lsumArr is\n",myId);
			printArray(lsumArr,leaderId,leaderId+threadsInPartition-1);
			printf("Id is %d rsumArr is\n",myId);
			printArray(rsumArr,leaderId,leaderId+threadsInPartition-1);

		}
		//prefix sum for less than and greater than
		if(myId == leaderId)
		{
			for(int i=leaderId+1;i < (leaderId+threadsInPartition);i++)
			{
				lsumArr[i]+=lsumArr[i-1];
				rsumArr[i]+=rsumArr[i-1];
			}

		}
		pthread_barrier_wait(&commonBarr[leaderId]);

		int targetLessStartIndex = lsumArr[myId]-numLessElements;
		int targetGreaterStartIndex = lsumArr[leaderId+threadsInPartition-1]+1+rsumArr[myId]-numGreaterElements;
		memcpy(currentMirrorArr+targetLessStartIndex,currentArr+lessStartIndex,numLessElements*sizeof(int));
		if(myId==leaderId)
			memcpy(currentMirrorArr+targetGreaterStartIndex-1,currentArr+greaterStartIndex-1,sizeof(int));
		memcpy(currentMirrorArr+targetGreaterStartIndex,currentArr+greaterStartIndex,numGreaterElements*sizeof(int));

		// wait for everyone to copy
		pthread_barrier_wait(&commonBarr[leaderId]);

		if(myId == leaderId)
		{
			printf("Id is %d CurrentMirrorArr is\n",myId);
			printArray(currentMirrorArr,start,start+totalElementsInPartition-1);
		}

		int totalLessElements = lsumArr[leaderId+threadsInPartition-1];
		int totalGreaterElements = rsumArr[leaderId+threadsInPartition-1];
		int lessThreads = (double)totalLessElements / totalElementsInPartition * threadsInPartition;
		int greaterThreads = threadsInPartition - lessThreads;

		//partition for next iteration
		if(myId == leaderId)
		{
			int elementsPerThread = totalLessElements/lessThreads;
			int excessElements = totalLessElements%lessThreads;
			int lastIndex = start;
			for(int i=leaderId;i<(leaderId+lessThreads);i++)
			{
				threadInfoArr[i].start = lastIndex;
				lastIndex+= (elementsPerThread -1);
				if(excessElements > 0)
				{
					lastIndex++;
					excessElements--;
				}
				threadInfoArr[i].end = lastIndex;
				lastIndex++; // increment lastIndex so that it is ready for next iteration
				threadInfoArr[i].threadsInPartition = lessThreads;
				threadInfoArr[i].totalElementsInPartition = totalLessElements;
			}
			//skip pivots
			while(currentMirrorArr[lastIndex] == pivotElement)
				lastIndex++;


			elementsPerThread = totalGreaterElements/greaterThreads;
			excessElements = totalGreaterElements%greaterThreads;
			int greaterPivotPosition = lastIndex;
			for(int i=(leaderId+lessThreads);i<(leaderId+lessThreads+greaterThreads);i++)
			{
				threadInfoArr[i].start = lastIndex;
				lastIndex+= (elementsPerThread -1);
				if(excessElements > 0)
				{
					lastIndex++;
					excessElements--;
				}
				threadInfoArr[i].end = lastIndex;
				lastIndex++; // increment lastIndex so that it is ready for next iteration
				threadInfoArr[i].threadsInPartition = greaterThreads;
				threadInfoArr[i].totalElementsInPartition = totalGreaterElements;
				threadInfoArr[i].leaderId = leaderId+lessThreads;
				threadInfoArr[i].pivotIndex = greaterPivotPosition;

			}
			//change barrier for right partition leader
			pthread_barrier_destroy(&commonBarr[leaderId]);
			pthread_barrier_init(&commonBarr[leaderId], NULL, lessThreads);
			pthread_barrier_init(&commonBarr[leaderId+lessThreads], NULL, greaterThreads);
			//wake up other threads
			for(int i=leaderId+1;i < (leaderId+threadsInPartition);i++)
				pthread_barrier_wait(&ownBarr[i]);
		}
		pthread_barrier_wait(&ownBarr[myId]);

		ping =!ping;
	}
}

void swap(int *x,int *y)
{
	int temp = *x;
	*x = *y;
	*y = temp;
}
void validate(int* output, int num_elements) {
	int i = 0;
	assert(output != NULL);
	for(i = 0; i < num_elements - 1; i++) {
		if (output[i] > output[i+1]) {
			printf("************* NOT sorted *************\n");
			return;
		}
	}
	printf("============= SORTED ===========\n");
}

int main()
{
	int inputArr[MAX_NUM];
	int checkArr[MAX_NUM];

	srand(time(NULL));
	for(int i=0;i<MAX_NUM;i++)
	{
		inputArr[i] = rand()%10+1;
		//inputArr[i] = i+1;
		checkArr[i] = inputArr[i];
	}
	struct timeval tz;
	struct timezone tx;
	double start_time, end_time;

	printf("Input Arr is\n");
	printArray(inputArr,0,MAX_NUM-1);


	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(inputArr,MAX_NUM,MAX_THREADS);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;

	validate(inputArr, MAX_NUM);

	double ptime =(double)end_time - (double)start_time;

	printf("Parallel Time is %lf\n",ptime);

	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(checkArr,MAX_NUM,1);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	double stime = ((double)end_time - (double)start_time);
	printf("Serial Time is %lf\n",stime);
	printf("Speedup is %lf\n",stime/ptime);

	for(int i=0;i<MAX_NUM;i++)
	{
		if(inputArr[i]!=checkArr[i])
			printf("Error i is %d parallel result is %d serial result is %d\n",i,inputArr[i],checkArr[i]);
	}

}





