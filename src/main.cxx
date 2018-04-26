
#include <hdf5-ng.hxx>
#include <iostream>

using namespace std;

int main(int argc, char const ** argv) {
	if (argc < 2)
		return 1;

	hdf5ng::file hf(argv[1]);

	cout << "debug" << endl;

	return 0;
}

