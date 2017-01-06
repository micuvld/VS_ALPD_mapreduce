#pragma once
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <set>
#include <vector>
#include <sstream>
#include <map>
#include "Utils.h"
using namespace std;

string doMapping(string inputFilePath);

string doSort(string inputFilePath);

void doFirstReduce(string inputFilePath);

void doShuffleSort(string inputFileString, string outputFileString);

void doFinalReduce(string inputFileString, string outputFilePath);