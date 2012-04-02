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

//#define DEBUG
#define MAX_NUM 10000000
#define MAX_THREADS 8

struct thread_info_type
{
	int *inputArr;
	int *lsumArr;
	int *rsumArr;
	int *mirrorArr;
	int start; // starting index of array
	int end; // ending index of array
	int leaderId;
	int threadsInPartition;
	int totalElementsInPartition;
	pthread_barrier_t *commonBarr;
	pthread_barrier_t *ownBarr;
	int *pivotElementArr;

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
void validate(int* output, int num_elements);

int kth_smallest(int a[], int n, int k)
{
	int i,j,l,m ;
	int x,temp ;
	l=0 ; m=n-1 ;
	while (l<m) {
		x=a[k] ;
		i=l ;
		j=m ;
		do {
			while (a[i]<x) i++ ;
			while (x<a[j]) j-- ;
			if (i<=j) {
				temp = a[i];
				a[i] = a[j];
				a[j] = temp;
				i++ ; j-- ;
			}
		} while (i<=j) ;
		if (j<k) l=i ;
		if (k<i) m=j ;
	}
	return a[k];
}

int new_median(int a[], int n)
{
	return kth_smallest(a,n,(((n)&1)?((n)/2):(((n)/2)-1)));
}

void printArray(int arr[],int start,int end)
{
	int i;
	for(i=start;i<=end;i++)
		printf("%d ",arr[i]);
	printf("\n");
}

int *pqsort(int* inputArr, int numElements, int numThreads)
{
	int *lsumArr = (int *)malloc(sizeof(int)*numThreads);
	int *rsumArr = (int *)malloc(sizeof(int)*numThreads);
	pthread_barrier_t * commonBarr =(pthread_barrier_t *)malloc(sizeof(pthread_barrier_t)*numThreads) ;
	pthread_barrier_t * ownBarr =(pthread_barrier_t *)malloc(sizeof(pthread_barrier_t)*numThreads) ;
	int *mirrorArr = (int *)malloc(sizeof(int)*numElements);
	struct thread_info_type *threadInfoArr = (struct thread_info_type *)malloc(sizeof(struct thread_info_type)*numThreads);
	int *pivotElementArr = (int *)malloc(sizeof(int)*numThreads);
	int *pivotIndexArr = (int *)malloc(sizeof(int)*numThreads);
	pthread_t *p_threads= (pthread_t *)malloc(sizeof(pthread_t)*numThreads);
	struct thread_local_type * threadLocalArr = (struct thread_local_type *)malloc(sizeof(struct thread_local_type)*numThreads);
#ifdef DEBUG
	assert (lsumArr!=NULL);
	assert (rsumArr!=NULL);
	assert(commonBarr!=NULL);
	assert (mirrorArr!=NULL);
	assert(ownBarr!=NULL);
	assert (threadInfoArr!=NULL);
	assert(pivotElementArr!=NULL);
	assert(pivotIndexArr!=NULL);
	assert (p_threads!=NULL);
	assert (threadLocalArr!=NULL);
#endif
	int i;
	for(i=0;i<numThreads;i++)
	{
		if(pthread_barrier_init(&ownBarr[i], NULL, 2))
		{
			printf("Could not create barrier %d\n",i);
		}
	}
	if(pthread_barrier_init(&commonBarr[0], NULL, numThreads))
	{
		printf("Could not create barrier %d\n",0);
	}
	//cout << "Input array is " << endl;
	//printArray(a,numElements);



	int elementsPerThread = numElements/numThreads;
	int excessElements = numElements%numThreads;
	int lastIndex = 0;

	// assign ranges and create threads
	for(i=0;i<numThreads;i++)
	{
		threadInfoArr[i].totalElementsInPartition = numElements;
		threadInfoArr[i].start = lastIndex;
		threadInfoArr[i].leaderId = 0; // initially thread 0 is leader
		threadInfoArr[i].inputArr = inputArr;
		threadInfoArr[i].lsumArr = lsumArr;
		threadInfoArr[i].rsumArr = rsumArr;
		threadInfoArr[i].mirrorArr = mirrorArr;
		threadInfoArr[i].commonBarr = commonBarr;
		threadInfoArr[i].ownBarr = ownBarr;
		threadInfoArr[i].threadsInPartition = numThreads;
		threadInfoArr[i].pivotElementArr = pivotElementArr;
		lastIndex+= (elementsPerThread -1);
		if(excessElements > 0)
		{
			lastIndex++;
			excessElements--;
		}
		threadInfoArr[i].end = lastIndex;
		lastIndex++; // increment lastIndex so that it is ready for next iteration
	}

	for(i=0;i<numThreads;i++)
	{
		threadLocalArr[i].myId = i;
		threadLocalArr[i].threadInfoArr = threadInfoArr;
		pthread_create(&p_threads[i],NULL,parallel_quick_sort,&threadLocalArr[i]);
	}

	for (i=0; i< numThreads; i++)
		pthread_join(p_threads[i], NULL);

	free(lsumArr);
	free(rsumArr);
	free(threadInfoArr);
	free(threadLocalArr);
	free(mirrorArr);
	free(pivotElementArr);
	return inputArr;
}

void * parallel_quick_sort(void * arg)
{
	int i;
	struct thread_local_type threadLocal = *((struct thread_local_type *)arg);
	struct thread_info_type * threadInfoArr = threadLocal.threadInfoArr;
	int *inputArr = threadInfoArr[threadLocal.myId].inputArr;
	int *lsumArr = threadInfoArr[threadLocal.myId].lsumArr;
	int *rsumArr = threadInfoArr[threadLocal.myId].rsumArr;
	int *mirrorArr = threadInfoArr[threadLocal.myId].mirrorArr;
	int *pivotElementArr = threadInfoArr[threadLocal.myId].pivotElementArr;
	int myId = threadLocal.myId;
	pthread_barrier_t *commonBarr = threadInfoArr[threadLocal.myId].commonBarr;
	pthread_barrier_t *ownBarr = threadInfoArr[threadLocal.myId].ownBarr;
	int *currentArr;
	int *currentMirrorArr;
	int ping=1;
	struct thread_info_type threadInfo;
	int start,end,leaderId,numElements,totalElementsInPartition,threadsInPartition;
	int localMedian;
	int *a,n,k;
	int pivotElement;
	int firstIndex,secondIndex;
	int numLessElements,numGreaterElements,lessStartIndex,greaterStartIndex;
	int offset,targetLessStartIndex,targetGreaterStartIndex;
	int totalLessElements;
	int totalGreaterElements;
	int lessThreads;
	int greaterThreads;
	int lastIndex,elementsPerThread,excessElements;
	int temp;
	while(1)
	{
		threadInfo = threadInfoArr[threadLocal.myId];
		start = threadInfo.start; // start element Index
		end = threadInfo.end; // end element Index
		leaderId = threadInfo.leaderId;
		numElements = end-start+1;
		totalElementsInPartition = threadInfo.totalElementsInPartition;
		threadsInPartition = threadInfo.threadsInPartition;

		//printf("Start is %d End is %d MyId is %d\n",start,end,myId);
		//printf("leaderid is %d MyId is %d\n",leaderId,myId);

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

		if(threadsInPartition == 1) // terminating condition
		{
			if(currentArr != inputArr)
			{
				//printf("Id is %d Copying to output\n",myId);
				memcpy(inputArr+start,mirrorArr+start,sizeof(int)*(numElements));
			}
			qsort(inputArr+start,numElements,sizeof(int),compare);
			break;
		}

		//find pivot across threads
		//printf("Id is %d Sending array ",myId);
		//printArray(currentArr,start,end);

		a=currentArr+start;
		n = numElements;
		if(numElements & 1)
		{
			k = numElements/2;
		}
		else
		{
			k = numElements/2-1;
		}
		{
			register int i,j,l,m ;
			register int x,temp ;
			l=0 ; m=n-1 ;
			while (l<m) {
				x=a[k] ;
				i=l ;
				j=m ;
				do {
					while (a[i]<x) i++ ;
					while (x<a[j]) j-- ;
					if (i<=j) {
						temp = a[i];
						a[i] = a[j];
						a[j] = temp;
						i++ ; j-- ;
					}
				} while (i<=j) ;
				if (j<k) l=i ;
				if (k<i) m=j ;
			}
		}
		localMedian = a[k];


#ifdef DEBUG
		printf("Id is %d localMedian is %d\n",myId,localMedian);
#endif

		pivotElementArr[myId] = localMedian;
		//wait for everyone
		pthread_barrier_wait(&commonBarr[leaderId]);
		/*if(myId==leaderId)
		{
			printf("Id is %d pivotElementArr is",myId);
			printArray(pivotElementArr,leaderId,leaderId+threadsInPartition-1);
		}*/

		int *localPivotArr = (int *)malloc(sizeof(int)*threadsInPartition);
		memcpy(localPivotArr,pivotElementArr+leaderId,threadsInPartition*sizeof(int));
		//printf("Id is %d localPivotArr is",myId);
		//printArray(localPivotArr,0,threadsInPartition-1);

		a = localPivotArr;
		n = threadsInPartition;
		if(threadsInPartition&1)
			k=threadsInPartition/2;
		else
			k=threadsInPartition/2-1;
		{
			register int i,j,l,m ;
			register int x,temp ;
			l=0 ; m=n-1 ;
			while (l<m) {
				x=a[k] ;
				i=l ;
				j=m ;
				do {
					while (a[i]<x) i++ ;
					while (x<a[j]) j-- ;
					if (i<=j) {
						temp = a[i];
						a[i] = a[j];
						a[j] = temp;
						i++ ; j-- ;
					}
				} while (i<=j) ;
				if (j<k) l=i ;
				if (k<i) m=j ;
			}
		}
		pivotElement = a[k];
		free(localPivotArr);
#ifdef DEBUG
		printf("Id is %d pivotElement is %d \n",myId,pivotElement);
#endif

		firstIndex = start;
		secondIndex = start;
		while(secondIndex <= end)
		{
			//printf("Id is %d FirstIndex is %d SecondIndex is %d pivotElement is %d currentArr[secondIndex] is %d\n",myId,firstIndex,secondIndex,pivotElement,currentArr[secondIndex]);

			if(currentArr[secondIndex] <= pivotElement)
			{
				temp = currentArr[secondIndex];
				currentArr[secondIndex] = currentArr[firstIndex];
				currentArr[firstIndex] = temp;
				firstIndex++;
			}
			secondIndex++;
		}

		//printf("Id is %d FirstIndex is %d SecondIndex is %d\n",myId,firstIndex,secondIndex);


		numLessElements = firstIndex - start;
		numGreaterElements = numElements - numLessElements;
		lessStartIndex = start;
		greaterStartIndex = firstIndex;

		lsumArr[myId] = numLessElements;
		rsumArr[myId] = numGreaterElements;
		pthread_barrier_wait(&commonBarr[leaderId]);


#ifdef DEBUG
		if(myId == leaderId)
		{
			printf("Id is %d CurrentArr is\n",myId);
			printArray(currentArr,start,start+totalElementsInPartition-1);
			printf("Id is %d lsumArr is\n",myId);
			printArray(lsumArr,leaderId,leaderId+threadsInPartition-1);
			printf("Id is %d rsumArr is\n",myId);
			printArray(rsumArr,leaderId,leaderId+threadsInPartition-1);

		}
#endif
		//prefix sum for less than and greater than
		if(myId == leaderId)
		{
			for(i=leaderId+1;i < (leaderId+threadsInPartition);i++)
			{
				lsumArr[i]+=lsumArr[i-1];
				rsumArr[i]+=rsumArr[i-1];
			}

		}
		pthread_barrier_wait(&commonBarr[leaderId]);

		offset = threadInfoArr[leaderId].start;
		//int offset = 0;
		targetLessStartIndex = lsumArr[myId]-numLessElements;
		targetGreaterStartIndex = lsumArr[leaderId+threadsInPartition-1]+rsumArr[myId]-numGreaterElements;
		memcpy(currentMirrorArr+offset+targetLessStartIndex,currentArr+lessStartIndex,numLessElements*sizeof(int));
		memcpy(currentMirrorArr+offset+targetGreaterStartIndex,currentArr+greaterStartIndex,numGreaterElements*sizeof(int));
		// wait for everyone to copy
		pthread_barrier_wait(&commonBarr[leaderId]);
#ifdef DEBUG
		if(myId == leaderId)
		{
			printf("After memcpy CurrentMirrorArr is",myId);
			printArray(currentMirrorArr,start,start+totalElementsInPartition-1);
		}
#endif



		totalLessElements = lsumArr[leaderId+threadsInPartition-1];
		totalGreaterElements = rsumArr[leaderId+threadsInPartition-1];
		lessThreads = round((double)totalLessElements / (totalElementsInPartition-1) * threadsInPartition);
		if(totalLessElements>0 && lessThreads < 1)
			lessThreads=1;
		if(lessThreads == threadsInPartition)
			lessThreads--;

		greaterThreads = threadsInPartition - lessThreads;
		if(greaterThreads == threadsInPartition)
		{
			lessThreads++;
			greaterThreads--;
		}
		//partition for next iteration
		if(myId == leaderId)
		{
			lastIndex = start;
			if(lessThreads!=0)
			{

				elementsPerThread = totalLessElements/lessThreads;
				excessElements = totalLessElements%lessThreads;

				for(i=leaderId;i<(leaderId+lessThreads);i++)
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
			}

			if(greaterThreads!=0)
			{

				elementsPerThread = totalGreaterElements/greaterThreads;
				excessElements = totalGreaterElements%greaterThreads;
				int greaterPivotPosition = lastIndex;

				for(i=(leaderId+lessThreads);i<(leaderId+lessThreads+greaterThreads);i++)
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
				}
				//change barrier for right partition leader
				pthread_barrier_init(&commonBarr[leaderId+lessThreads], NULL, greaterThreads);
			}
			//wake up other threads
			for(i=leaderId+1;i < (leaderId+threadsInPartition);i++)
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
	//	printf("============= SORTED ===========\n");
}

int main()
{
	//int inputArr[MAX_NUM];
	//int checkArr[MAX_NUM];
	int *inputArr = (int *)malloc(sizeof(int)*MAX_NUM);
	assert(inputArr!=NULL);
	int *checkArr = (int *)malloc(sizeof(int)*MAX_NUM);
	assert(checkArr!=NULL);
	int *checkArr2 = (int *)malloc(sizeof(int)*MAX_NUM);
	assert(checkArr2!=NULL);


	srand(time(NULL));
	//srand(9999);
	int i;
	for(i=0;i<MAX_NUM;i++)
	{
		inputArr[i] = rand() % (MAX_NUM/10);
		//		inputArr[i] = 1;
		checkArr[i] = inputArr[i];
		checkArr2[i] = inputArr[i];
	}
	struct timeval tz;
	struct timezone tx;
	double start_time, end_time;
#ifdef DEBUG
	printf("Input Arr is\n");
	printArray(inputArr,0,MAX_NUM-1);
#endif

	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(inputArr,MAX_NUM,MAX_THREADS);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;

	validate(inputArr, MAX_NUM);
	//printf("Output Arr is\n");
	//printArray(inputArr,0,MAX_NUM-1);



	double ptime =(double)end_time - (double)start_time;
	//printf("Parallel Time is %lf\n",ptime);

	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(checkArr,MAX_NUM,1);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	validate(checkArr, MAX_NUM);

	double stime = ((double)end_time - (double)start_time);
	//printf("Serial Time is %lf\n",stime);
	printf("Speedup is %lf\n",stime/ptime);

	for(i=0;i<MAX_NUM;i++)
	{
		if(inputArr[i]!=checkArr[i])
			printf("Error i is %d parallel result is %d serial result is %d\n",i,inputArr[i],checkArr[i]);
	}

	/*
	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	qsort(checkArr2,MAX_NUM,sizeof(int),compare);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	validate(checkArr2, MAX_NUM);
	double stime2 = ((double)end_time - (double)start_time);
	printf("Serial Time is %lf\n",stime2);
	printf("Speedup is %lf\n",stime2/ptime);
	 */

}


