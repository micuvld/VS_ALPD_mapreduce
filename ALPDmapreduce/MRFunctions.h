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

string doShuffleSort(string inputFileString);

void doFinalReduce(string inputFileString);