/*
============================================================================
Name        : mapreduce.c
Author      : 
Version     :
Copyright   : Your copyright notice
Description : Compute Pi in MPI C++
============================================================================
*/
#include <math.h> 

#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <set>
#include <vector>
#include <sstream>
#include <map>
#include "mpi.h"
using namespace std;

struct FrequencyLine {
	string docName;
	string word;
	int count;
};

vector<string> split(string toSplit, char delim) {
	stringstream toSplitStream;
	string token;
	vector<string> tokens;

	toSplitStream.str(toSplit);
	while(getline(toSplitStream, token, delim)) {
		tokens.push_back(token);
	}

	return tokens;
}

bool secondTokenCompare(const string a, const string b) {
	string fileNameA, wordA, countA;
	string fileNameB, wordB, countB;

	vector<string> tokensA = split(a, ',');
	vector<string> tokensB = split(b, ',');

	return tokensA.at(1) < tokensB.at(1);
}

bool compareWordThenDoc(const FrequencyLine a, const FrequencyLine b) 
{
	if (a.word < b.word) return true;
	if (b.word < a.word) return false;

	if (a.docName < b.docName) return true;
	if (b.docName < a.docName) return false;

	return false;
}


bool isCharOfWord(char c) {
	if (isalpha(c) || c == '\'') {
		return true;
	} else {
		return false;
	}
}

void writeDocWordCount(ofstream &outFile, string inputFileName, string word, int count) {
	outFile << "<" + inputFileName + ","
		<< word + ","
		<< to_string(count) + ">\n";
}

int main(int argc, char *argv[]) {
	//	int n, rank, size, i;
	//	double PI25DT = 3.141592653589793238462643;
	//	double mypi, pi, h, sum, x;
	string text = "";
	string line;
	char c;
	string word = "";

	ifstream inputFile;
	ofstream outFile;

	/*
	* MAP phase
	*/
	inputFile.open("test-files/4.txt");
	inputFile >> noskipws;
	outFile.open("mapped1.txt");

	int i = 0;

	while(inputFile >> c) {
		i++;
		if (isCharOfWord(c)) {
			word += tolower(c);
		} else {
			if (word != "") {
				if (i%2 == 0) {
					writeDocWordCount(outFile, "1.txt", word, 1);
				} else {
					writeDocWordCount(outFile, "2.txt", word, 1);
				}
			}
			word = "";
		}
	}

	inputFile.close();
	outFile.close();

	/*
	* SORT phase
	*/
	string fileName;
	//string word;
	string count;
	vector<FrequencyLine> mappedLines;
	FrequencyLine newMappedLine;

	inputFile.open("mapped1.txt");
	inputFile >> noskipws;
	outFile.open("sorted.txt");

	while(getline(inputFile, line)) {
		//remove angular brackets
		line.erase(0,1);
		line.erase(line.length() - 1, 1);

		vector<string> tokens = split(line, ',');
		newMappedLine.docName = tokens.at(0);
		newMappedLine.word = tokens.at(1);
		newMappedLine.count = stoi(tokens.at(2));

		mappedLines.push_back(newMappedLine);
	}

	sort(mappedLines.begin(), mappedLines.end(), compareWordThenDoc);

	for(FrequencyLine ml : mappedLines) {
		outFile << "<" + ml.docName + "," + ml.word + "," + to_string(ml.count) + ">" << endl;
	}

	inputFile.close();
	outFile.close();

	/*
	* REDUCE phase
	*/
	inputFile.open("sorted.txt");
	inputFile >> noskipws;

	map<string, FrequencyLine> mapOfSortedLines;
	map<string, FrequencyLine>::iterator sortedIterator;
	FrequencyLine sortedLine;

	int frequencyByFile;
	vector<string> tokens;
	

	while(getline(inputFile, line)) {
		//remove angular brackets
		line.erase(0,1);
		line.erase(line.length() - 1, 1);

		tokens = split(line, ',');

		//docName + word
		sortedIterator = mapOfSortedLines.find(tokens.at(0) + tokens.at(1));

		//if !exists in map: add it; else: count++
		if (sortedIterator == mapOfSortedLines.end()) {
			sortedLine.docName = tokens.at(0);
			sortedLine.word = tokens.at(1);
			sortedLine.count = stoi(tokens.at(2));
			mapOfSortedLines.insert(pair<string,FrequencyLine>(tokens.at(0) + tokens.at(1), sortedLine));
		} else {
			(*sortedIterator).second.count++;
		}
	}

	char firstLetterFile = '\0';
	string fileEnding = "Reduced.txt";
	for(auto docWord : mapOfSortedLines) {
		if (docWord.second.word.at(0) != firstLetterFile) {
			firstLetterFile = docWord.second.word.at(0);
			cout << firstLetterFile;
			if (outFile.is_open()) {
				outFile.close();
			}
			cout << firstLetterFile + fileEnding;
			outFile.open(firstLetterFile + fileEnding);
		}
		outFile << "<" + docWord.second.docName + "," + docWord.second.word \
			+ "," + to_string(docWord.second.count) + ">"
			<< endl;
	}

	inputFile.close();
	outFile.close();

	/*
	 *SHUFFLE SORT phase
	 */


	//inputFile.open("reduced.txt");
	//inputFile >> noskipws;
	//outFile.open("shuffleSorted.txt");

	//  MPI::Init(argc, argv);
	//	size = MPI::COMM_WORLD.Get_size();
	//	rank = MPI::COMM_WORLD.Get_rank();
	//
	//	n=1000; // number of intervals
	//
	//	MPI::COMM_WORLD.Bcast(&n, 1, MPI::INT, 0);
	//	h = 1.0 / (double) n;
	//	sum = 0.0;
	//	for (i = rank + 1; i <= n; i += size) {
	//		x = h * ((double) i - 0.5);
	//		sum += (4.0 / (1.0 + x * x));
	//	}
	//	mypi = h * sum;
	//
	//	MPI::COMM_WORLD.Reduce(&mypi, &pi, 1, MPI::DOUBLE, MPI::SUM, 0);
	//	if (rank == 0)
	//		cout << "pi is approximately " << pi << ", Error is "
	//				<< fabs(pi - PI25DT) << endl;
	//
	//	MPI::Finalize();
	return 0;
}

