#ifndef STEP1_H_
#define STEP1_H_
#include <stdio.h>
#include <stdint.h>
typedef struct Tuple{
	char word[40];
	int freq;
	struct Tuple *next;
}Tuple;

typedef struct Q {
	char word[40];
	struct Q * next;
	struct Q * prev;
}Q;

typedef struct Table{
	Tuple * tuple;
}Table;

int hashFunction(char * data, int size);
Table * initiateTable();
void enqueue(char * word, Q * head, Q * tail);
void dequeue(Q * head, Q * tail, char * word);
int read(Q * head, Q * tail, int readflag);
void mapreduce(char * word, Table * table, int code);
void wright(Table * table);
void PrintandFree(Tuple * tuple, FILE * file);

#endif

