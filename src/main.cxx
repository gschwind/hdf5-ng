
#include <hdf5-ng.hxx>
#include <iostream>
#include <stack>

using namespace std;

int main(int argc, char const ** argv) {
	if (argc < 2)
		return 1;

	h5ng::h5obj hf(argv[1]);

	stack<h5ng::h5obj> s;
	s.push(hf);
	while(not s.empty()) {
		auto cur = s.top();
		s.pop();
		for(auto x: cur.keys()) {
			s.push(cur[x]);
		}
		cur.print_info();
	}

	cout << "READ DATA" << endl;

	float * out = new float[100];
	hf["set"].read(reinterpret_cast<void*>(out), h5ng::slc(0,1), h5ng::slc(5,6), h5ng::slc(0,100,2));
	for(int i = 0; i < 50; ++i) {
		cout << out[i] << endl;
	}

	return 0;
}

