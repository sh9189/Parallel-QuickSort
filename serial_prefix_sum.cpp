#include<iostream>
#define MAX_NUM 500000000
int a[MAX_NUM];
using namespace std;
int main()
{
	for(int i=0;i<MAX_NUM;i++)
	{
		a[i] = i+1;
	}
	for(int i=0;i<MAX_NUM;i++)
		a[i+1] += a[i];

	//for(int i=0;i<MAX_NUM;i++)
		//cout << a[i] << " ";
}
