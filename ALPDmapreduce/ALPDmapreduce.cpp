/*
============================================================================
Name        : mapreduce.c
Author      : 
Version     :
Copyright   : Your copyright notice
Description : Compute Pi in MPI C++
============================================================================
*/
#include <ctime>
#include "Utils.h"
#include "MRFunctions.h"
#include "mpi.h"
using namespace std;

int main(int argc, char *argv[]) {
	string path = "output/";
	string inputFilesPath = "test-files/";
	string inputFilesExtension = ".txt";
	string outputFileName;
	ifstream inputFile;
	ofstream outFile;
	char *fileNameChar;
	char c;
	char i,j;

	int commSize;
	int my_rank;
	int numberOfWorkers;

	int numberOfInputFiles;
	int numberOfMappedFiles = 0;
	int nextInputFileIndex = 0;
	int numberOfReducedFiles = 0;
	int numberOfSecondPhaseFiles;
	int numberOfFinalReducedFiles = 0;
	int nextShuffleSortFileIndex = 0;
	char receivedFileName[50];
	int messageLength;
	bool firstReducePhaseIsFinished = 0;
	bool finalReducePhaseIsFinished = 0;
	bool jobDone = 0;

	MPI_Status status;

	vector<string> fileNames;
	vector<string> filesToSort;
	vector<string> filesToReduce;
	vector<string> filesToShuffleSort;
	vector<string> filesToFinalReduce;

	MPI_Init(&argc, &argv);

	MPI_Comm_size(MPI_COMM_WORLD, &commSize);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	numberOfWorkers = commSize - 1;

	if (my_rank == 0) {
		cout << "Number of input files: ";
		cin >> numberOfInputFiles;

		for (i = 1; i <= numberOfInputFiles; ++i) {
			fileNames.push_back(inputFilesPath + to_string((i + '\0')) + inputFilesExtension);
		}

		//send files to all proceses
		while (!fileNames.empty() && nextInputFileIndex < numberOfWorkers) {
			fileNameChar = stringToChar(fileNames.at(0));
			cout << "Sent map " << getFileNameFromPath(fileNameChar) << " to " << nextInputFileIndex + 1 << endl;
			flush(cout);

			MPI_Send(fileNameChar, fileNames.at(0).length() + 1, MPI_CHAR, nextInputFileIndex + 1, Operations::MAP, MPI_COMM_WORLD);
			fileNames.erase(fileNames.begin());

			nextInputFileIndex++;
		}

		while (!firstReducePhaseIsFinished) {
			//get message length
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &messageLength);

			//actual receive
			MPI_Recv(receivedFileName, messageLength, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			switch(status.MPI_TAG) {
			case Operations::MAP:
				filesToSort.push_back(string(receivedFileName));

				if (!fileNames.empty()) {
					fileNameChar = stringToChar(fileNames.at(0));
					cout << "Sent map " << fileNameChar << " to " << status.MPI_SOURCE << endl;
					flush(cout);

					MPI_Send(fileNameChar, fileNames.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::MAP, MPI_COMM_WORLD);
					fileNames.erase(fileNames.begin());
				} else {
					if (!filesToSort.empty()) {
						fileNameChar = stringToChar(filesToSort.at(0));
						cout << "Sent sort " << fileNameChar << " to " << status.MPI_SOURCE << endl;
						flush(cout);

						MPI_Send(fileNameChar, filesToSort.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::SORT, MPI_COMM_WORLD);
						filesToSort.erase(filesToSort.begin());
					}
				}
				break;
			case Operations::SORT:
				filesToReduce.push_back(string(receivedFileName));

				if (!filesToSort.empty()) {
					fileNameChar = stringToChar(filesToSort.at(0));
					cout << "Sent sort " << fileNameChar << " to " << status.MPI_SOURCE << endl;
					flush(cout);

					MPI_Send(fileNameChar, filesToSort.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::SORT, MPI_COMM_WORLD);
					filesToSort.erase(filesToSort.begin());
				} else {
					if(!filesToReduce.empty()) {
						fileNameChar = stringToChar(filesToReduce.at(0));
						cout << "Sent reduce " << fileNameChar << " to " << status.MPI_SOURCE << endl;
						flush(cout);

						MPI_Send(fileNameChar, filesToReduce.at(0).length() + 1,  MPI_CHAR, status.MPI_SOURCE, Operations::REDUCE, MPI_COMM_WORLD);
						filesToReduce.erase(filesToReduce.begin());
					}
				}
				break;
			case Operations::REDUCE:
				numberOfReducedFiles++;

				if(!filesToReduce.empty() && numberOfReducedFiles < numberOfInputFiles) {
					fileNameChar = stringToChar(filesToReduce.at(0));
					cout << "Sent reduce " << fileNameChar << " to " << status.MPI_SOURCE << endl;
					flush(cout);

					MPI_Send(fileNameChar, filesToReduce.at(0).length() + 1,  MPI_CHAR, status.MPI_SOURCE, Operations::REDUCE, MPI_COMM_WORLD);
					filesToReduce.erase(filesToReduce.begin());
				} 

				if (numberOfReducedFiles == numberOfInputFiles) {
					cout << endl <<"Finished FIRST REDUCE phase" << endl << endl;
					firstReducePhaseIsFinished = true;
				}
				break;
			}
		}

		inputFile.open(inputFilesPath + "lettersForReducedFiles.txt");
		while(inputFile >> c) {
			if (isCharOfWord(c)) {
				filesToShuffleSort.push_back(generateFileName(c, Operations::REDUCE));
				filesToFinalReduce.push_back(generateFileName(c, Operations::SHUFFLESORT));
			}
		}

		numberOfSecondPhaseFiles = filesToFinalReduce.size();

		while (!filesToShuffleSort.empty() && nextShuffleSortFileIndex < numberOfWorkers ) {
			fileNameChar = stringToChar(filesToShuffleSort.at(0));
			cout << "Sent SS " << getFileNameFromPath(fileNameChar) << " to " << nextShuffleSortFileIndex + 1 << endl;
			flush(cout);

			MPI_Send(fileNameChar, filesToShuffleSort.at(0).length() + 1, MPI_CHAR, nextShuffleSortFileIndex + 1, Operations::SHUFFLESORT, MPI_COMM_WORLD);
			filesToShuffleSort.erase(filesToShuffleSort.begin());

			nextShuffleSortFileIndex++;
		}

		while(!finalReducePhaseIsFinished) {
			//get message length
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &messageLength);

			//actual receive
			MPI_Recv(receivedFileName, messageLength, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			switch(status.MPI_TAG) {
			case Operations::SHUFFLESORT:
				filesToFinalReduce.push_back(string(receivedFileName));

				if (!filesToShuffleSort.empty()) {
					fileNameChar = stringToChar(filesToShuffleSort.at(0));
					cout << "Sent SS " << getFileNameFromPath(fileNameChar) << " to " << status.MPI_SOURCE << endl;
					flush(cout);

					MPI_Send(fileNameChar, filesToShuffleSort.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::SHUFFLESORT, MPI_COMM_WORLD);
					filesToShuffleSort.erase(filesToShuffleSort.begin());
				} else {
					if (!filesToFinalReduce.empty()) {
						fileNameChar = stringToChar(filesToFinalReduce.at(0));
						cout << "Sent FR " << fileNameChar << " to " << status.MPI_SOURCE << endl;
						flush(cout);

						MPI_Send(fileNameChar, filesToFinalReduce.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::FINALREDUCE, MPI_COMM_WORLD);
						filesToFinalReduce.erase(filesToFinalReduce.begin());
					}
				}
				break;
			case Operations::FINALREDUCE:
				numberOfFinalReducedFiles++;
				cout << endl<<"Left: " + filesToFinalReduce.size() << endl;
				flush(cout);
				if (!filesToFinalReduce.empty() && numberOfFinalReducedFiles < numberOfSecondPhaseFiles) {
					fileNameChar = stringToChar(filesToFinalReduce.at(0));
					cout << "Sent FR " << fileNameChar << " to " << status.MPI_SOURCE << endl;
					flush(cout);

					MPI_Send(fileNameChar, filesToFinalReduce.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::FINALREDUCE, MPI_COMM_WORLD);
					filesToFinalReduce.erase(filesToFinalReduce.begin());
				}

				if (numberOfFinalReducedFiles == numberOfSecondPhaseFiles) {
					cout << endl <<"Finished SECOND REDUCE phase" << endl << endl;
					finalReducePhaseIsFinished = true;
				}
				break;
			}
		}

		for (i = 0; i < numberOfWorkers; ++i) {
			MPI_Send(fileNameChar, 1, MPI_CHAR, i + 1, Operations::END, MPI_COMM_WORLD);
		}

		cout << "Master procces finished his job";

	} else {
		while(!jobDone) {
			//get message length
			cout << endl << my_rank << " WAITING FOR MESSAGE" << endl;
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &messageLength);

			//actual receive
			MPI_Recv(receivedFileName, messageLength, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			switch(status.MPI_TAG) {
			case Operations::MAP:
				outputFileName = doMapping(receivedFileName);
				cout << my_rank << " Finished mapping " << getFileNameFromPath(receivedFileName) << endl;


				fileNameChar = stringToChar(outputFileName);
				MPI_Send(fileNameChar, outputFileName.length() + 1, MPI_CHAR, 0, Operations::MAP, MPI_COMM_WORLD);
				break;

			case Operations::SORT:
				outputFileName = doSort(receivedFileName);
				cout << my_rank << " Finished sorting " << getFileNameFromPath(receivedFileName) << endl;

				fileNameChar = stringToChar(outputFileName);
				MPI_Send(fileNameChar, outputFileName.length() + 1, MPI_CHAR, 0, Operations::SORT, MPI_COMM_WORLD);
				break;

			case Operations::REDUCE:
				doFirstReduce(receivedFileName);
				cout << my_rank << " Finished reducing " << getFileNameFromPath(receivedFileName) << endl;

				//fileNameChar = stringToChar(outputFileName);
				MPI_Send(receivedFileName, string(receivedFileName).length() + 1, MPI_CHAR, 0, Operations::REDUCE, MPI_COMM_WORLD);
				break;

			case Operations::SHUFFLESORT:
				outputFileName = doShuffleSort(receivedFileName);
				cout << my_rank << " Finished shuffleSorting " << getFileNameFromPath(receivedFileName) << endl;

				fileNameChar = stringToChar(outputFileName);
				MPI_Send(fileNameChar, outputFileName.length() + 1, MPI_CHAR, 0, Operations::SHUFFLESORT, MPI_COMM_WORLD);
				break;

			case Operations::FINALREDUCE:
				doFinalReduce(receivedFileName);
				cout << my_rank << " Finished finalReduce " << getFileNameFromPath(receivedFileName) << endl;

				MPI_Send(receivedFileName, string(receivedFileName).length() + 1, MPI_CHAR, 0, Operations::FINALREDUCE, MPI_COMM_WORLD);
				break;

			case Operations::END:
				jobDone = true;
			}
		}

		cout << "Procces #" << my_rank << " finished his job";
	}

	MPI_Finalize();

	return 0;
}