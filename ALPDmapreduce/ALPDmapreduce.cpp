/*
============================================================================
Name        : mapreduce.c
Author      : 
Version     :
Copyright   : Your copyright notice
Description : Compute Pi in MPI C++
============================================================================
*/
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
	string fileNames[25];
	char *fileNameChar;
	char i,j;

	int commSize;
	int my_rank;
	int numberOfWorkers;

	int numberOfInputFiles = 25;
	int numberOfMappedFiles = 0;
	int nextInputFileIndex = 0;
	char receivedFileName[50];
	int messageLength;
	MPI_Status status;

	vector<string> filesToSort;

	for (i = 1; i <= numberOfInputFiles; ++i) {
		fileNames[i-1] = (inputFilesPath + to_string((i + '\0')) + inputFilesExtension);
	}

	MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &commSize);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	numberOfWorkers = commSize - 1;

	if (my_rank == 0) {
		//send files to all proceses
		while (nextInputFileIndex < numberOfWorkers) {
			fileNameChar = stringToChar(fileNames[nextInputFileIndex]);
			MPI_Send(fileNameChar, fileNames[nextInputFileIndex].length() + 1, MPI_CHAR, nextInputFileIndex + 1, Operations::MAP, MPI_COMM_WORLD);

			nextInputFileIndex++;
		}

		while (true) {
			//get message length
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &messageLength);

			//actual receive
			MPI_Recv(receivedFileName, messageLength, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			switch(status.MPI_TAG) {
			case Operations::MAP:
				filesToSort.push_back(string(receivedFileName));

				if (nextInputFileIndex < numberOfInputFiles) {
					fileNameChar = stringToChar(fileNames[nextInputFileIndex]);
					MPI_Send(fileNameChar, fileNames[nextInputFileIndex].length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::MAP, MPI_COMM_WORLD);
					nextInputFileIndex++;
				} else {
					if (!filesToSort.empty()) {
						fileNameChar = stringToChar(filesToSort.at(0));
						MPI_Send(fileNameChar, filesToSort.at(0).length() + 1, MPI_CHAR, status.MPI_SOURCE, Operations::SORT, MPI_COMM_WORLD);
						filesToSort.erase(filesToSort.begin());
					}
				}
				break;
			case Operations::SORT:
				break;
			}
		}

	} else {
		while(true) {
			//get message length
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &messageLength);

			//actual receive
			MPI_Recv(receivedFileName, messageLength, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			switch(status.MPI_TAG) {
			case Operations::MAP:
				outputFileName = doMapping(receivedFileName);

				fileNameChar = stringToChar(outputFileName);
				MPI_Send(fileNameChar, outputFileName.length() + 1, MPI_CHAR, 0, Operations::MAP, MPI_COMM_WORLD);
				break;
			case Operations::SORT:
				outputFileName = doSort(receivedFileName);

				fileNameChar = stringToChar(outputFileName);
				MPI_Send(fileNameChar, outputFileName.length() + 1, MPI_CHAR, 0, Operations::SORT, MPI_COMM_WORLD);
				break;
			}
		}
	}

	MPI_Finalize();

	///*
	//* MAP phase
	//*/

	//doMapping("test-files/4.txt", path + "mapped1.txt");

	///*
	//* SORT phase
	//*/
	//doSort(path + "mapped1.txt", path + "sorted.txt");
	//
	///*
	//* REDUCE phase
	//*/

	//doFirstReduce(path + "sorted.txt", path);
	//
	///*
	//*SHUFFLE SORT phase
	//*/

	//doShuffleSort(path + "aReduced.txt",path + "aShuffleSorted.txt");

	///*
	// *FINAL REDUCE phase
	// */

	//doFinalReduce(path + "aShuffleSorted.txt", path);

	return 0;
}