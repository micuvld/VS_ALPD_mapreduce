#pragma once
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <set>
#include <vector>
#include <sstream>
#include <map>
#include <string.h>
using namespace std;

struct FrequencyLine {
	string docName;
	string word;
	int count;
};

enum Operations {
	MAP,
	SORT,
	REDUCE,
	SHUFFLESORT,
	FINALREDUCE
};

vector<string> split(string toSplit, char delim);

bool secondTokenCompare(const string a, const string b);

bool compareWordThenDoc(const FrequencyLine a, const FrequencyLine b);

bool compareDocThenWord(const FrequencyLine a, const FrequencyLine b);

string generateOutputFileName(string path, Operations operation);

string generateOutputFileName(char letter, Operations operation);

bool isCharOfWord(char c);

void writeDocWordCount(ofstream &outFile, string inputFileName, string word, int count);

char *stringToChar(string s);