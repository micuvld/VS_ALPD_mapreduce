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

void doMapping(string inputFilePath, string outputFilePath);

void doSort(string inputFilePath, string outputFilePath);

void doFirstReduce(string inputFilePath, string outputFilePath);

void doShuffleSort(string inputFileString, string outputFileString);

void doFinalReduce(string inputFileString, string outputFilePath);