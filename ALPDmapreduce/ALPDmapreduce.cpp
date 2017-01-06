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
	ifstream inputFile;
	ofstream outFile;

	/*
	* MAP phase
	*/

	doMapping("test-files/4.txt", path + "mapped1.txt");

	/*
	* SORT phase
	*/
	doSort(path + "mapped1.txt", path + "sorted.txt");
	
	/*
	* REDUCE phase
	*/

	doFirstReduce(path + "sorted.txt", path);
	
	/*
	*SHUFFLE SORT phase
	*/

	doShuffleSort(path + "aReduced.txt",path + "aShuffleSorted.txt");

	/*
	 *FINAL REDUCE phase
	 */

	doFinalReduce(path + "aShuffleSorted.txt", path);
	
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