#include "Utils.h"
#include "MRFunctions.h"

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

string getFileNameFromPath(string path) {
	return split(path, '/').back();
}

string generateFileName(string path, Operations operation) {
	string pathWithoutFileName = "output/";
	string fileName;
	string fileNameEnding;
	vector<string> tokens = split(path, '/');

	switch(operation) {
	case Operations::MAP:
		fileName = "mapped#" + tokens.back();
		break;
	case Operations::SORT:
		fileName = "sorted#" + tokens.back();
		break;
	case Operations::SHUFFLESORT:
		fileNameEnding = "ShuffleSorted.txt";
		fileName = tokens.back().at(0) + fileNameEnding;
		break;
	case Operations::FINALREDUCE:
		pathWithoutFileName = "final-results/";
		fileNameEnding = "FinalReduce.txt";
		fileName = tokens.back().at(0) + fileNameEnding;
		break;
	}

	return pathWithoutFileName + fileName;
}

string generateFileName(char letter, Operations operation) {
	string fileName;
	string fileNameEnding;

	switch(operation) {
	case Operations::REDUCE:
		fileNameEnding = "Reduced.txt";
		fileName = letter + fileNameEnding;
		break;
	case Operations::SHUFFLESORT:
		fileNameEnding = "ShuffleSorted.txt";
		fileName = letter + fileNameEnding;
		break;
	}

	return "output/" + fileName;
}

bool isCharOfWord(char c) {
	if ((c >= -1 && c <= 255 && isalpha(c)) || c == '\'') {
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

char *stringToChar(string s) {
	char *c = new char[s.length() + 1];
	strcpy(c, s.c_str());
	return c;
}

void writeConcurrent(string filePath, string toWrite) {
	  HANDLE hFile;
	  DWORD  bytesToWrite, dwBytesWritten, dwPos;
      BYTE   buff[4096];
	  wstring wFilePath;
	  wstring wToWrite;
	  OVERLAPPED overlapped;

	  boolean isLocked = false;

	  wFilePath.assign(filePath.begin(), filePath.end());
	  hFile = CreateFile(wFilePath.c_str(), // open Two.txt
              FILE_APPEND_DATA,         // open for writing
              FILE_SHARE_WRITE,          // allow multiple readers
              NULL,                     // no security
              OPEN_ALWAYS,              // open or create
              FILE_ATTRIBUTE_NORMAL,    // normal file
              NULL);                    // no attr. template

	  if (hFile == INVALID_HANDLE_VALUE)
	  {
		 cout << "Could not open file: " << filePath << endl;
		 return;
	  }

	  toWrite += "\r\n";
	  wToWrite.assign(toWrite.begin(), toWrite.end());

	  dwPos = SetFilePointer(hFile, 0, NULL, FILE_END);
	  overlapped.Offset = wToWrite.length();
	  overlapped.OffsetHigh = 0;
	  overlapped.hEvent = 0;
	  LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK, 0, toWrite.length(), 0, &overlapped);
      WriteFile(hFile, toWrite.c_str(), toWrite.length(), &dwBytesWritten, NULL);
      UnlockFile(hFile, dwPos, 0, toWrite.length(), 0);

	  CloseHandle(hFile);
}