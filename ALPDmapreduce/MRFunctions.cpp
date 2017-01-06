#include "Utils.h"
#include "MRFunctions.h"

void doMapping(string inputFilePath, string outputFilePath) {
	ifstream inputFile;
	ofstream outFile;
	string word;
	char c;

	inputFile.open(inputFilePath);
	inputFile >> noskipws;
	outFile.open(outputFilePath);

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
}

void doSort(string inputFilePath, string outputFilePath) {
	ifstream inputFile;
	ofstream outFile;

	string line;
	vector<FrequencyLine> mappedLines;
	FrequencyLine newMappedLine;

	inputFile.open(inputFilePath);
	inputFile >> noskipws;
	outFile.open(outputFilePath);

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
}

void doFirstReduce(string inputFilePath, string outFilePath) {
	ifstream inputFile;
	ofstream outFile;
	string line;

	inputFile.open(inputFilePath);
	inputFile >> noskipws;

	map<string, FrequencyLine> mapOfSortedLines;
	map<string, FrequencyLine>::iterator sortedIterator;
	FrequencyLine sortedLine;

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

	char firstLetterFile = (*mapOfSortedLines.begin()).second.word.at(0);
	string fileEnding = "Reduced.txt";

	outFile.open(outFilePath + firstLetterFile + fileEnding, ios_base::app);
	for(auto docWord : mapOfSortedLines) {
		if (docWord.second.word.at(0) != firstLetterFile) {
			firstLetterFile = docWord.second.word.at(0);
			outFile.close();			
			outFile.open(outFilePath + firstLetterFile + fileEnding , ios_base::app);
			cout <<firstLetterFile + fileEnding;
		}

		outFile << "<" + docWord.second.docName + "," + docWord.second.word \
			+ "," + to_string(docWord.second.count) + ">"
			<< endl;
	}

	inputFile.close();
	outFile.close();
}

void doShuffleSort(string inputFileString, string outputFileString) {
	ifstream inputFile;
	ofstream outFile;
	vector<string> tokens;

	string line;

	vector<FrequencyLine> reducedLines;
	FrequencyLine newReducedLine;

	inputFile.open(inputFileString);
	inputFile >> noskipws;
	outFile.open(outputFileString, ios_base::app);

	while(getline(inputFile, line)) {
		//remove angular brackets
		line.erase(0,1);
		line.erase(line.length() - 1, 1);

		tokens = split(line, ',');
		newReducedLine.docName = tokens.at(0);
		newReducedLine.word = tokens.at(1);
		newReducedLine.count = stoi(tokens.at(2));

		reducedLines.push_back(newReducedLine);
	}

	sort(reducedLines.begin(), reducedLines.end(), compareWordThenDoc);

	for(auto rl : reducedLines) {
		outFile << "<" + rl.word + "," + rl.docName + "," + to_string(rl.count) + ">" << endl;
	}

	inputFile.close();
	outFile.close();
}

void doFinalReduce(string inputFileString, string outFilePath) {
	ifstream inputFile;
	ofstream outFile;
	string line;
	vector<string> tokens;

	inputFile.open(inputFileString);
	inputFile >> noskipws;

	map<string, string> mapOfShuffleSortedLines;
	map<string, string>::iterator shuffleSortedIterator;
	FrequencyLine shuffleSortedLine;

	while(getline(inputFile, line)) {
		//remove angular brackets
		line.erase(0,1);
		line.erase(line.length() - 1, 1);

		tokens = split(line, ',');

		//find by word
		shuffleSortedIterator = mapOfShuffleSortedLines.find(tokens.at(0));

		//if !exists in map: add it; else: count++
		if (shuffleSortedIterator == mapOfShuffleSortedLines.end()) {
			mapOfShuffleSortedLines.insert(pair<string,string>(tokens.at(0), "{" + tokens.at(1) + "," + tokens.at(2) + "},"));
		} else {
			(*shuffleSortedIterator).second.append("{" + tokens.at(1) + "," + tokens.at(2)  + "},");
		}
	}

	char firstLetterFile = (*mapOfShuffleSortedLines.begin()).first.at(0);
	string fileEnding = "FinalReduced.txt";

	outFile.open(outFilePath + firstLetterFile + fileEnding, ios_base::app);
	for(auto word : mapOfShuffleSortedLines) {
		if (word.first.at(0) != firstLetterFile) {
			firstLetterFile = word.first.at(0);
			outFile.close();			
			outFile.open(outFilePath + firstLetterFile + fileEnding , ios_base::app);
		}

		word.second.erase(word.second.length() - 1, 1);
		outFile << "<" + word.first + "," + word.second + ">" << endl;
	}

	inputFile.close();
	outFile.close();
}