#include<iostream>
#define MAX_NUM 100000000
int a[MAX_NUM];
using namespace std;
int main()
{
	for(int i=0;i<MAX_NUM;i++)
	{
		a[i] = i+1;
	}
	clock_t start = clock();
	for(int i=0;i<MAX_NUM;i++)
		a[i+1] += a[i];
	clock_t end = clock();

	double diff = ((double)end - (double)start);
	cout << "Time is " << diff;
	cout << "Start is " << start;
	cout << "End is " << end;

	//for(int i=0;i<MAX_NUM;i++)
	//cout << a[i] << " ";
}
