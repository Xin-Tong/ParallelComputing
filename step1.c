#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h> 
#include "step1.h"
#include <omp.h>

#define readThreads 8
#define mapreduceThreads 16
#define writeThreads 1

int main() {
	int numOfThreads = readThreads + mapreduceThreads + writeThreads;
	char word[40];
	int i, readflag = 0, readend = 0, pid, hash;
	Q * head = NULL;
	Q * tail = NULL;
	Table * table = initiateTable();
	omp_set_num_threads(numOfThreads);
	double time = - omp_get_wtime();
	#pragma omp parallel private(pid, word){
		pid = omp_get_thread_num();
		// read Thread
		if (pid < readThreads) {
			#pragma omp single
			readend = read(head, tail, readflag);
		}
			
		//mapreduce Thread
		if (pid >= readThreads && pid < (readThreads + mapreduceThreads)){
			while (1) {
				if(tail != NULL) {
					dequeue(head, tail, word);
					hash = hashFunction(word, mapreduceThreads);
					mapreduce(word, table, hash);
				}
				if (readend == 1 && tail == NULL){
					break;
				}
			}
		}
		
		// wright Thread
		#pragma omp barrier
		if (pid >= readThreads + mapreduceThreads){
			wright(table);
		}
		#pragma omp barrier
		free(table); 
   		time += omp_get_wtime();
   		printf("Time of Counting Word Frequency is %f\n",time);
   		return 1;					
}
	

int hashFunction(char * data, int size) {
	int hash = 0;
	int i;
	for (i = 0; i < strlen(data); i++){
		hash = data[i] + 31 * hash;
	}
	hash = (hash & 0x7fffffff) % size;
}

Table * initiateTable(){
	int i;
	Table * table = (Table*) malloc(sizeof(table)*mapreduceThreads);
	for(i = 0; i < mapreduceThreads;i++){
		table[i].tuple = NULL;
	}
	return table;
}

void enqueue(char * word, Q * head, Q * tail) {
	#pragma omp critical
	{
		Q * tmp;
		tmp = (Q *)malloc(sizeof(Q));
		strcpy(tmp->word, word);
		tmp->next = NULL;
		tmp->prev = NULL;
		if (head == NULL) {
			head = tmp;
			tail = tmp;
		}
		else {
			tmp->next = head->next;
			head->next->prev = tmp;
			tmp->prev = head;
			head->next = tmp;
		}
	}
}


void dequeue(Q * head, Q * tail, char * word) {
	#pragma omp critical
	{
		if(tail != NULL){
			Q * tmp = tail;
			tail = tail->prev;
			tail->next = NULL;
			strcpy(word, tmp->word);
			free(tmp);
		}
	}
}

int read(Q * head, Q * tail, int readflag) {
	char word[30];
	FILE *file;
	file = fopen("74.txt", "r");
	readflag = fscanf(file, "%s", word);
	while(readflag){
		#pragma omp task private(readflag)
		enqueue(word, head, tail);
		readflag = fscanf(file, "%s", word);
	}
	fclose(file);
	return 1;
}

void mapreduce(char * word, Table * table, int code){
	Tuple * item = table[code].tuple;
	if (item == NULL){
		Tuple * newTuple = (Tuple *)malloc(sizeof(Tuple));
		table[code].tuple = newTuple;
		newTuple->next = NULL;
		newTuple->freq = 1;
		strcpy(newTuple->word, word);
	}
	else {
		while (item != NULL) {
			if (strcmp(item->word, word) == 0){
				item->freq += 1;
				break;
			}
			if (item->next == NULL || strcmp(item->next->word, word) < 0){
				Tuple *newTuple = (Tuple *) malloc(sizeof(Tuple));
				newTuple->next = item->next;
				newTuple->freq = 1;
				item->next = newTuple;
				strcmp(newTuple->word, word);
				break;
			}
			item = item->next;
		}
	}
}

void wright(Table * table) {
//	#pragma omp single	
	int i;
	FILE * file;
	file = fopen("output.txt", "w");
	#pragma omp parallel for 
	for (i = 0; i < mapreduceThreads; i++) {
		if(table[i].tuple != NULL) {
			PrintandFree(table[i].tuple, file);
		}
	}
	fclose(file);
}
		
void PrintandFree(Tuple * tuple, FILE * file) {
	if (tuple->next==NULL){
          fprintf(file, "%-15s    %10d \n", tuple->word, tuple->freq);
          free (tuple);
    }
    else {
          PrintandFree(tuple->next,file);
          fprintf(file, "%-15s    %10d \n", tuple->word, tuple->freq);
          free (tuple);
    }
}

	
		
	
