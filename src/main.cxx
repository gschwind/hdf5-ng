
#include <hdf5-ng.hxx>
#include <iostream>
#include <stack>
#include <algorithm>

using namespace std;

int main(int argc, char const ** argv) {
	if (argc < 2)
		return 1;

	h5ng::h5obj hf(argv[1]);

	stack<h5ng::h5obj> s;
	vector<h5ng::h5obj> visited;
	s.push(hf);
	while(not s.empty()) {
		auto cur = s.top();
		s.pop();
		auto k = cur.keys();
		if (k.size() == 0) {
			cout << "========== INFO =========" << endl;
			cur.print_info();
			cout << "========== /INFO =========" << endl;
		} else {
			if(std::find(visited.begin(), visited.end(), cur) == visited.end()) {
				for(auto x: cur.keys()) {
					s.push(cur[x]);
				}
			} else {
				cout << "Already visited node" << endl;
			}
		}
		visited.push_back(cur);
	}

//	cout << "READ DATA 0" << endl;
//
//	float * out = new float[100];
//	hf["set_continuous"].read(reinterpret_cast<void*>(out), h5ng::slc(0,1), h5ng::slc(5,9,3), h5ng::slc(0,100,2));
//	for(int i = 0; i < 100; ++i) {
//		cout << out[i] << endl;
//		out[i] = -1;
//	}
//
//	cout << "READ DATA 1" << endl;
//
//	hf["set_chunked"].read(reinterpret_cast<void*>(out), h5ng::slc(0,1), h5ng::slc(5,9,3), h5ng::slc(0,100,2));
//	for(int i = 0; i < 100; ++i) {
//		cout << out[i] << endl;
//	}

	return 0;
}

