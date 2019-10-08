#include <iostream>
#include <pthread.h>
#include <vector>
#include <queue>
#include <utility>
#include <algorithm>
#define NN	0x500000
#define MINSIZE 5
#define MAXQUEUESIZE (NN / MINSIZE)

using namespace std;

int P;
int N;
vector<int> th_args;
queue<pair<int, int>> Q;
vector<int> A;

//pthread_barrier_t barr;
pthread_mutex_t mut;
pthread_cond_t Qempty;
pthread_cond_t Qfull;

int sleepcount, sort_complete;

void insert_sublist(int start, int end)
{
	pthread_mutex_lock(&mut);
	
	//while(Q.size() == MAXQUEUESIZE)
		//pthread_cond_wait(&Qfull, &mut);
	
	Q.push(make_pair(start, end));
	/*cout << "inserted " << start << " " << end << "\n";*/
	pthread_cond_signal(&Qempty);
	pthread_mutex_unlock(&mut);
}
pair<int, int> remove_sublist()
{
	pair<int, int> list;
	
	pthread_mutex_lock(&mut);
	/*cout << "size of sublist queue: " << Q.size() << "\n";*/
	if(Q.empty() && !sort_complete)
	{
		sleepcount++;
		if(sleepcount < P)
		{
			/*cout << "threads sleeping: " << sleepcount << "\n";*/
			pthread_cond_wait(&Qempty, &mut);
			sleepcount--;
		}
		else
		{
			sort_complete = 1;
			pthread_cond_broadcast(&Qempty);
		}
	}
	if(sort_complete)
	{
		list.first = 0;
		list.second = 0;
	}
	else
	{
		list.first = Q.front().first;
		list.second = Q.front().second;
		Q.pop();
		pthread_cond_signal(&Qfull);
	}
		
	pthread_mutex_unlock(&mut);
	
	return list;
}

void insertion_sort(int start, int end)
{
	for(int i = start + 1; i <= end; ++i)
	{
		int val = A[i];
		int j = i - 1;
		while(j >= start && A[j] > val)
		{
			A[j + 1] = A[j];
			j--;
		}
		A[j + 1] = val;
	}	
}

int partition(int start, int end)
{
	int pivot = A[end];
	int i = (start - 1);
  
    for (int j = start; j <= end - 1; j++)  
    {  
        if (A[j] <= pivot)  
        {  
            i++;
            swap(A[i], A[j]);  
        }  
    }
    swap(A[i + 1], A[end]);  
    return (i + 1);  
}

void quick_sort(int start, int end)
{

	while(1)
	{
		if(end <= start)
			return;
		
		if ((end - start + 1) <= MINSIZE)
		{
			/*pthread_mutex_lock(&mut);
			cout << "running insertion sort from " << start << "to " << end << "\n";
			pthread_mutex_unlock(&mut);*/
			insertion_sort(start, end);
			return;
		}
		else
		{
			/*pthread_mutex_lock(&mut);
			cout << "running quick sort from " << start << "to " << end << "\n";
			pthread_mutex_unlock(&mut);*/
			int pivot = partition(start, end);
			if(pivot > start + 1 && pivot < end - 1)
			{
				if(pivot - start < end - pivot)
				{
					insert_sublist(start, pivot - 1);
					start = pivot + 1;
				}
				else
				{
					insert_sublist(pivot + 1, end);
					end = pivot - 1;
				}
			}
			else
				if(pivot > start + 1)
					end = pivot - 1;
				else
					start = pivot + 1;
		}
				
	}
}

void* th_func(void* arg)
{
	int th_num = *(int *)arg;
	pair<int, int> list;
	if(th_num == 0)
	{
		list.first = 0;
		list.second = N - 1;
	}
	else
	{
		list = remove_sublist();
		/*pthread_mutex_lock(&mut);
		cout << "thread " << th_num << " removed: " << list.first << " " << list.second << "\n";
		pthread_mutex_unlock(&mut);*/
	}
	
	int done = 0;
	while(!done)
	{
		if(list.first == 0 && list.second == 0)
			done = 1;
		else
		{
			quick_sort(list.first, list.second);
			list = remove_sublist();
			/*pthread_mutex_lock(&mut);
			cout << "thread " << th_num << " removed: " << list.first << " " << list.second << "\n";
			pthread_mutex_unlock(&mut);*/
		}
	}
	
	return NULL;	
}

int main(int argc, char** argv)
{
	if(argc != 3)
	{
		printf("Usage : quiksort <NPROC> <NELEMENTS>\n");
		exit(0);
	}
	
	P = stoi(argv[1]);
	N = stoi(argv[2]);
	
//	pthread_barrier_init(&barr, NULL, P);
	
	pthread_mutex_init(&mut, NULL);
	pthread_cond_init(&Qempty, NULL);
	pthread_cond_init(&Qfull, NULL);
	
	sleepcount = 0;
	sort_complete = 0;
	
	for(int i = 0; i < P; ++i) th_args.push_back(i);
	
	// Initialize and Shuffle
	for (int i=0; i<N; i++)
		A.push_back(i);

	int j = 1137;

	for (int i=0; i<N; i++)
	{
		int t = A[j];
		A[j] = A[i];
		A[i] = t;
		j = (j + 337) % N;
	}
	
	/*for(auto x: A)
		cout << x << " ";
	cout << "\n";*/

	//create threads
	vector<pthread_t> th(P);
	for(int i = 0; i < P; ++i)
		pthread_create(&th[i], NULL, th_func, &th_args[i]);
	
	for(int i = 0; i < P; ++i)
		pthread_join(th[i], NULL);
		
	/*for(auto x: A)
		cout << x << " ";
	cout << "\n";	*/

	
}
