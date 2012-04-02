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
	return a[k];
}

int kth_smallest_with_pos(int a[], int n, int k,int pos[])
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
				swap(&pos[i],&pos[j]);
				i++ ; j-- ;
			}
		} while (i<=j) ;
		if (j<k) l=i ;
		if (k<i) m=j ;
	}
	return k;
}


int new_median(int a[], int n)
{
	return kth_smallest(a,n,(((n)&1)?((n)/2):(((n)/2)-1)));
}



pthread_barrier_t globalBarr;


int *pqsort(int* inputArr, int numElements, int numThreads)
{
	//int sumArrSize = pow(2,ceil(log2(numThreads)));
	int *lsumArr = (int *)malloc(sizeof(int)*numThreads);
	assert (lsumArr!=NULL);
	int *rsumArr = (int *)malloc(sizeof(int)*numThreads);
	assert (rsumArr!=NULL);
	pthread_barrier_t commonBarr[numThreads];
	//pthread_barrier_t ownBarr[MAX_THREADS];


	int *mirrorArr = (int *)malloc(sizeof(int)*numElements);
	assert (mirrorArr!=NULL);

	int i;
	/*for(i=0;i<MAX_THREADS;i++)
	{
		if(pthread_barrier_init(&ownBarr[i], NULL, 2))
		{
			printf("Could not create barrier %d\n",i);
		}
	}*/
	if(pthread_barrier_init(&commonBarr[0], NULL, numThreads))
	{
		printf("Could not create barrier %d\n",0);
	}
	if(pthread_barrier_init(&globalBarr, NULL, numThreads))
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

	//bring median to position zero
	//int pivotIndex = median(inputArr,numElements);
	//swap(&inputArr[0],&inputArr[pivotIndex]);
	//printf("Pivot position is %d\n",pivotIndex);

	int *pivotElementArr = (int *)malloc(sizeof(int)*numThreads);
	assert(pivotElementArr!=NULL);
	int *pivotIndexArr = (int *)malloc(sizeof(int)*numThreads);
	assert(pivotIndexArr!=NULL);




	// assign ranges and create threads
	for(i=0;i<numThreads;i++)
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
	struct thread_local_type * threadLocalArr = (struct thread_local_type *)malloc(sizeof(struct thread_local_type)*numThreads);
	assert (threadLocalArr!=NULL);
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

	//cout << "Prefix Sum array is " << endl;
	//printArray(a,numElements);
}


void printArray(int arr[],int start,int end)
{
	int i;
	for(i=start;i<=end;i++)
		printf("%d ",arr[i]);
	printf("\n");
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
	int *currentArr;
	int *currentMirrorArr;

	int ping=1;
	int totalIterations = 0;
	int numIterations = 0;
	int done = 0;
	while(1)
	{
		struct thread_info_type threadInfo = threadInfoArr[threadLocal.myId];

		int start = threadInfo.start; // start element Index
		int end = threadInfo.end; // end element Index
		int leaderId = threadInfo.leaderId;
		int numElements = end-start+1;
		int totalElementsInPartition = threadInfo.totalElementsInPartition;
		int threadsInPartition = threadInfo.threadsInPartition;
		if(totalIterations == 0)
		{
			totalIterations = log2(threadsInPartition)+1;
		}


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
		//		printf("Id is %d Copying to output\n",myId);
				memcpy(inputArr+start,mirrorArr+start,sizeof(int)*(numElements));
			}
			qsort(inputArr+start,numElements,sizeof(int),compare);
			done = 1;
		}
		if(!done)
		{
			//find pivot across threads
			//printf("Id is %d Sending array ",myId);
			//printArray(currentArr,start,end);

<<<<<<< HEAD
			int localMedian = median(currentArr+start,numElements);
=======
		//find pivot across threads
		//printf("Id is %d Sending array ",myId);
		//printArray(currentArr,start,end);

		int localMedian = new_median(currentArr+start,numElements);
>>>>>>> eda00f4cd7f6846b6d13d4c3abab7d4e89baa5a9

#ifdef DEBUG
			printf("Id is %d localMedian is %d\n",myId,localMedian);
#endif
			//printArray(currentArr,start,end);
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

		int pivotElement = new_median(localPivotArr,threadsInPartition);
		free(localPivotArr);

#ifdef DEBUG
			printf("Id is %d pivotElement is %d \n",myId,pivotElement);
#endif
			int firstIndex,secondIndex;
			firstIndex = start;
			secondIndex = start;
			while(secondIndex <= end)
			{
				//printf("Id is %d FirstIndex is %d SecondIndex is %d pivotElement is %d currentArr[secondIndex] is %d\n",myId,firstIndex,secondIndex,pivotElement,currentArr[secondIndex]);

				if(currentArr[secondIndex] <= pivotElement)
				{
					swap(&currentArr[firstIndex],&currentArr[secondIndex]);
					firstIndex++;
				}
				secondIndex++;
			}

			//printf("Id is %d FirstIndex is %d SecondIndex is %d\n",myId,firstIndex,secondIndex);

			int numLessElements,numGreaterElements,lessStartIndex,greaterStartIndex;
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
<<<<<<< HEAD
			//prefix sum for less than and greater than
			if(myId == leaderId)
=======
		//prefix sum for less than and greater than
		if(myId == leaderId)
		{
			for(i=leaderId+1;i < (leaderId+threadsInPartition);i++)
>>>>>>> a1bd1052cd527d7f983fc464a14395453c80bdee
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
			int targetGreaterStartIndex = lsumArr[leaderId+threadsInPartition-1]+rsumArr[myId]-numGreaterElements;
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



			int totalLessElements = lsumArr[leaderId+threadsInPartition-1];
			int totalGreaterElements = rsumArr[leaderId+threadsInPartition-1];
			int lessThreads = round((double)totalLessElements / (totalElementsInPartition-1) * threadsInPartition);
			if(totalLessElements>0 && lessThreads < 1)
				lessThreads=1;
			if(lessThreads == threadsInPartition)
				lessThreads--;


		int greaterThreads = threadsInPartition - lessThreads;
		if(greaterThreads == threadsInPartition)
		{
			lessThreads++;
			greaterThreads--;
		}
		//partition for next iteration
		if(myId == leaderId)
		{
			//printf("Less Threads %d totalLessElements %d Greater Threads %d totalGreaterElements %d\n",lessThreads,totalLessElements,greaterThreads,totalGreaterElements);
			//int pivotIndex = median(currentArr+start,totalLessElements);
			//pivotIndex+=start;
			//printf("Id is %d Pivot for left partition is %d start is %d\n",myId,pivotIndex,start);
			//assert(pivotIndex < (start+totalLessElements));
			//swap(&currentArr[start],&currentArr[pivotIndex]);
			int lastIndex = start,elementsPerThread,excessElements;
			if(lessThreads!=0)
			{
				lessThreads++;
				greaterThreads--;
			}
			//partition for next iteration
			if(myId == leaderId)
			{
				printf("Less Threads %d totalLessElements %d Greater Threads %d totalGreaterElements %d\n",lessThreads,totalLessElements,greaterThreads,totalGreaterElements);
				//int pivotIndex = median(currentArr+start,totalLessElements);
				//pivotIndex+=start;
				//printf("Id is %d Pivot for left partition is %d start is %d\n",myId,pivotIndex,start);
				//assert(pivotIndex < (start+totalLessElements));
				//swap(&currentArr[start],&currentArr[pivotIndex]);
				int lastIndex = start,elementsPerThread,excessElements;
				if(lessThreads!=0)
				{

					elementsPerThread = totalLessElements/lessThreads;
					excessElements = totalLessElements%lessThreads;

<<<<<<< HEAD
					for(int i=leaderId;i<(leaderId+lessThreads);i++)
=======
				for(i=leaderId;i<(leaderId+lessThreads);i++)
				{
					threadInfoArr[i].start = lastIndex;
					lastIndex+= (elementsPerThread -1);
					if(excessElements > 0)
>>>>>>> a1bd1052cd527d7f983fc464a14395453c80bdee
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

					//pivotIndex = median(currentArr+greaterPivotPosition,totalGreaterElements);
					//pivotIndex+=greaterPivotPosition;
					//assert(pivotIndex < (greaterPivotPosition+totalGreaterElements));
					//swap(&currentArr[greaterPivotPosition],&currentArr[pivotIndex]);
					//printf("Id is %d Pivot for right partition is %d\n",myId,pivotIndex);

<<<<<<< HEAD
					for(int i=(leaderId+lessThreads);i<(leaderId+lessThreads+greaterThreads);i++)
=======
				for(i=(leaderId+lessThreads);i<(leaderId+lessThreads+greaterThreads);i++)
				{
					threadInfoArr[i].start = lastIndex;
					lastIndex+= (elementsPerThread -1);
					if(excessElements > 0)
>>>>>>> a1bd1052cd527d7f983fc464a14395453c80bdee
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
				}
			}
<<<<<<< HEAD
=======
			//wake up other threads
			for(i=leaderId+1;i < (leaderId+threadsInPartition);i++)
				pthread_barrier_wait(&ownBarr[i]);
>>>>>>> a1bd1052cd527d7f983fc464a14395453c80bdee
		}
		//printf("Id is %d Waiting for global barrier\n",myId);
		pthread_barrier_wait(&globalBarr);
		numIterations++;
		if(numIterations == totalIterations)
			break;
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

//	printf("Parallel Time is %lf\n",ptime);

	gettimeofday(&tz, &tx);
	start_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	pqsort(checkArr,MAX_NUM,1);
	gettimeofday(&tz, &tx);
	end_time = (double)tz.tv_sec + (double) tz.tv_usec / 1000000.0;
	validate(checkArr, MAX_NUM);

	double stime = ((double)end_time - (double)start_time);
//	printf("Serial Time is %lf\n",stime);
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





