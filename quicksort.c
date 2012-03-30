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

#define MAX_NUM 1000000
#define MAX_THREADS 8

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



int kth_smallest(int a[], int n, int k)
{
	int i,j,l,m ;
	int x ;
	l=0 ; m=n-1 ;
	while (l<m) {
		x=a[k] ;
		i=l ;
		j=m ;
		do {
			while (a[i]<x) i++ ;
			while (x<a[j]) j-- ;
			if (i<=j) {
				swap(&a[i],&a[j]) ;
				i++ ; j-- ;
			}
		} while (i<=j) ;
		if (j<k) l=i ;
		if (k<i) m=j ;
	}
	return k;
}

int median(int a[], int n)
{
	return kth_smallest(a,n,(((n)&1)?((n)/2):(((n)/2)-1)));
}



int *pqsort(int* inputArr, int numElements, int numThreads)
{
	//int sumArrSize = pow(2,ceil(log2(numThreads)));
	int *lsumArr = (int *)malloc(sizeof(int)*numThreads);
	assert (lsumArr!=NULL);
	int *rsumArr = (int *)malloc(sizeof(int)*numThreads);
	assert (rsumArr!=NULL);
	pthread_barrier_t commonBarr[MAX_THREADS];
	pthread_barrier_t ownBarr[MAX_THREADS];
	int *mirrorArr = (int *)malloc(sizeof(int)*numElements);
	assert (mirrorArr!=NULL);
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
	assert (p_threads!=NULL);
	int elementsPerThread = numElements/numThreads;
	int excessElements = numElements%numThreads;

	struct thread_info_type *threadInfoArr = (struct thread_info_type *)malloc(sizeof(struct thread_info_type)*numThreads);
	assert (threadInfoArr!=NULL);
	int lastIndex = 0;

	//find median and swap with zeroth element
	//int * medianArr = (int *)malloc(sizeof(int)*numElements);
	//assert(medianArr!=NULL);
	//memcpy(medianArr,inputArr,sizeof(int)*numElements);
	//bring median to position zero
	int pivotIndex = median(inputArr,numElements);
	swap(&inputArr[0],&inputArr[pivotIndex]);


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
	assert (threadLocalArr!=NULL);
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


		//printf("Start is %d End is %d MyId is %d\n",start,end,myId);
		//printf("leaderid is %d pivotIndex is %d MyId is %d\n",leaderId,pivotIndex,myId);

		if(threadsInPartition == 1) // terminating condition
		{

			/*printf("Id is %d ping is %d Input array is",myId,ping);
			printArray(inputArr,start,end);
			printf("Id is %d Mirror array is",myId);
			printArray(mirrorArr,start,end);*/

			//printf("Id is %d Quick sorting input arr from position %d to %d\n",myId,start,end);
			qsort(inputArr+start,end-start+1,sizeof(int),compare);
			//printf("Id is %d After sorting\n",myId);
			//printArray(inputArr,start,end);

			break;

		}


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

		//printf("Id is %d Out of Loop\n",myId);

		int numLessElements,numGreaterElements,lessStartIndex,greaterStartIndex;
		if(myId == leaderId)
		{
			numLessElements = firstIndex - start -1;
			numGreaterElements = numElements-numLessElements-1;
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
			//printf("Id is %d CurrentArr is\n",myId);
			//printArray(currentArr,start,start+totalElementsInPartition-1);
			//printf("Id is %d lsumArr is\n",myId);
			//printArray(lsumArr,leaderId,leaderId+threadsInPartition-1);
			//printf("Id is %d rsumArr is\n",myId);
			//printArray(rsumArr,leaderId,leaderId+threadsInPartition-1);

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

		int offset = threadInfoArr[leaderId].start;
		//int offset = 0;
		int targetLessStartIndex = lsumArr[myId]-numLessElements;
		int targetGreaterStartIndex = lsumArr[leaderId+threadsInPartition-1]+1+rsumArr[myId]-numGreaterElements;
		memcpy(currentMirrorArr+offset+targetLessStartIndex,currentArr+lessStartIndex,numLessElements*sizeof(int));
		if(myId==leaderId)
			memcpy(currentMirrorArr+offset+targetGreaterStartIndex-1,currentArr+pivotIndex,sizeof(int));
		memcpy(currentMirrorArr+offset+targetGreaterStartIndex,currentArr+greaterStartIndex,numGreaterElements*sizeof(int));

		/*printf("Id is %d Copying from %d to %d num is %d\n",myId,lessStartIndex,offset+targetLessStartIndex,numLessElements);
		if(myId==leaderId)
			printf("Id is %d Copying from %d to %d num is %d\n",myId,pivotIndex,offset+targetGreaterStartIndex-1,1);
		printf("Id is %d Copying from %d to %d num is %d\n",myId,greaterStartIndex,offset+targetGreaterStartIndex,numGreaterElements);*/

		// wait for everyone to copy
		pthread_barrier_wait(&commonBarr[leaderId]);

		/*if(myId == leaderId)
		{
			printf("Id is %d CurrentMirrorArr is\n",myId);
			printArray(currentMirrorArr,start,start+totalElementsInPartition-1);
		}*/

		int totalLessElements = lsumArr[leaderId+threadsInPartition-1];
		int totalGreaterElements = rsumArr[leaderId+threadsInPartition-1];
		int lessThreads = (double)totalLessElements / totalElementsInPartition * threadsInPartition;
		if(lessThreads < 1)
			lessThreads =1;

		int greaterThreads = threadsInPartition - lessThreads;

		//partition for next iteration
		if(myId == leaderId)
		{
			int pivotIndex = median(currentArr+start,totalLessElements);
			assert(pivotIndex < (start+totalLessElements-1));
			swap(&currentArr[start],&currentArr[pivotIndex]);

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
			pthread_barrier_destroy(&commonBarr[leaderId]);
			pthread_barrier_init(&commonBarr[leaderId], NULL, lessThreads);
			//copy output to input arr
			if(lessThreads == 1)
			{
				//copy left side
				if(currentMirrorArr != inputArr)
					memcpy(inputArr+start,mirrorArr+start,sizeof(int)*(totalLessElements));
			}
			if(greaterThreads == 1)
			{
				//copy right side
				if(currentMirrorArr != inputArr)
					memcpy(inputArr+start+totalLessElements+1,mirrorArr+start+totalLessElements+1,sizeof(int)*(totalGreaterElements));
			}
			//copy pivot to output
			if(currentMirrorArr != inputArr)
				memcpy(inputArr+lastIndex,mirrorArr+lastIndex,sizeof(int));
			//skip pivot
			lastIndex++;



			elementsPerThread = totalGreaterElements/greaterThreads;
			excessElements = totalGreaterElements%greaterThreads;
			int greaterPivotPosition = lastIndex;

			pivotIndex = median(currentArr+greaterPivotPosition,totalGreaterElements);
			assert(pivotIndex < (greaterPivotPosition+totalGreaterElements-1));
			swap(&currentArr[greaterPivotPosition],&currentArr[pivotIndex]);

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
			pthread_barrier_init(&commonBarr[leaderId+lessThreads], NULL, greaterThreads);
			//wake up other threads
			for(int i=leaderId+1;i < (leaderId+threadsInPartition);i++)
				pthread_barrier_wait(&ownBarr[i]);
		}
		else
			pthread_barrier_wait(&ownBarr[myId]);

		if(ping==1)
			ping=0;
		else
			ping=1;
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
	//int inputArr[MAX_NUM];
	//int checkArr[MAX_NUM];
	int *inputArr = (int *)malloc(sizeof(int)*MAX_NUM);
	assert(inputArr!=NULL);
	int *checkArr = (int *)malloc(sizeof(int)*MAX_NUM);
	assert(checkArr!=NULL);


	srand(time(NULL));
	//srand(9999);
	for(int i=0;i<MAX_NUM;i++)
	{
		inputArr[i] = rand()%10+1;
		//inputArr[i] = i+1;
		checkArr[i] = inputArr[i];
	}
	struct timeval tz;
	struct timezone tx;
	double start_time, end_time;

	//printf("Input Arr is\n");
	//printArray(inputArr,0,MAX_NUM-1);


	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(inputArr,MAX_NUM,MAX_THREADS);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;

	validate(inputArr, MAX_NUM);
	//printf("Output Arr is\n");
	//printArray(inputArr,0,MAX_NUM-1);



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





