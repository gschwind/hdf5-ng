
#include <hdf5-ng.hxx>
#include <iostream>
#include <stack>
#include <algorithm>

using namespace std;

int main(int argc, char const ** argv) {
	if (argc < 2)
		return 1;

	h5ng::h5obj hf(argv[1]);

	stack<pair<string, h5ng::h5obj>> s;
	vector<h5ng::h5obj> visited;
	s.push(make_pair("", hf));
	while(not s.empty()) {
		auto cur = s.top();
		s.pop();
		auto k = cur.second.keys();
		if (k.size() == 0) {
			cout << "========== INFO =========" << endl;
			cout << cur.first << endl;
			cur.second.print_info();
			cout << "========== /INFO =========" << endl;
		} else {
			if(std::find(visited.begin(), visited.end(), cur.second) == visited.end()) {
				for(auto x: cur.second.keys()) {
					string name = cur.first + "/" + x;
					s.push(make_pair(name, cur.second[x]));
				}
			} else {
				cout << "Already visited node" << endl;
			}
		}

		for (auto x: cur.second.list_attributes()) {
			cout << "ATTRIBUTE " << x << endl;
		}

		visited.push_back(cur.second);
	}


	try {
		auto chunks = hf["set"].list_chunk();

		uint64_t size = 0;
		for (auto const &chunk: chunks) {
			cout << "chunk = " << chunk.address << " , size = " << chunk.size_of_chunk << endl;
			size += chunk.size_of_chunk;
		}

		cout << "Total size = " << size << endl;
		cout << "Total number of chunk = " << chunks.size() << endl;

	} catch(h5ng::exception & e) {
		cout << "FAIL TO LIST CHUNK" << endl;
		cout << e.what() << endl;
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

