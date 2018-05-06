
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

	return 0;
}

