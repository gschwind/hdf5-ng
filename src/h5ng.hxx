/*
 * h5ng.hxx
 *
 *  Created on: 6 mai 2018
 *      Author: benoit.gschwind
 */

#ifndef SRC_H5NG_HXX_
#define SRC_H5NG_HXX_

#include <string>
#include <vector>
#include <memory>

namespace h5ng {

using namespace std;

class h5obj;

struct _h5obj {
	_h5obj() = default;
	virtual ~_h5obj() = default;

	_h5obj(_h5obj const &) = delete;
	_h5obj & operator=(_h5obj const &) = delete;

	virtual auto operator[](string const & name) const -> h5obj = 0;
	virtual auto shape() const -> vector<size_t> = 0;
	virtual auto shape(int i) const -> size_t = 0;
	virtual auto keys() const -> vector<char const *> = 0;
	virtual void print_info() = 0;

};

// this is a wrapper object to hide the underlying shared_ptr
class h5obj {
	shared_ptr<_h5obj> _ptr;

public:

	h5obj(h5obj const & x) = default;
	h5obj & operator=(h5obj const &) = default;
	h5obj(shared_ptr<_h5obj> const & x) : _ptr{x} { }

	h5obj(string const & filename);

	virtual ~h5obj() = default;

	vector<char const *> keys() {
		return _ptr->keys();
	}

	h5obj operator[](string const & name) const {
		return _ptr->operator [](name);
	}

	vector<size_t> shape() const {
		return _ptr->shape();
	}

	size_t shape(int i) const {
		return _ptr->shape(i);
	}

	void print_info() {
		_ptr->print_info();
	}

};

} // h5ng

#endif /* SRC_H5NG_HXX_ */
