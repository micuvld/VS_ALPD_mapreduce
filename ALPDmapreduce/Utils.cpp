#include "Utils.h"

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

bool compareDocThenWord(const FrequencyLine a, const FrequencyLine b) 
{
	if (a.docName < b.docName) return true;
	if (b.docName < a.docName) return false;

	if (a.word < b.word) return true;
	if (b.word < a.word) return false;

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