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

#include <cstring>
#include <cstdio>

namespace h5ng {

using namespace std;

class exception : public std::exception {
	char * msg;

public:

	template<typename ... ARGS>
	exception(char const * fmt, ARGS ... args) {
		int length = std::snprintf(nullptr, 0, fmt, args...);
		msg = new char[length+1];
		std::snprintf(msg, length+1, fmt, args...);
	}

	exception(exception const & e)
	{
		auto length = std::strlen(e.msg);
		msg = new char[length+1];
		std::copy(&e.msg[0], &e.msg[length+1], msg);
	}

	exception(exception const && x) : msg{std::move(x.msg)} { };

	exception & operator=(exception const & e)
	{
		if(this == &e)
			return *this;

		delete[] msg;
		auto length = std::strlen(e.msg);
		msg = new char[length+1];
		std::copy(&e.msg[0], &e.msg[length+1], msg);
		return (*this);
	}

	virtual ~exception() {
		delete [] msg;
	}

	char const * what() const noexcept override
	{
		return msg;
	}
};

#define EXCEPTION(fmt, ...) exception(fmt " at %s:%d", ##__VA_ARGS__, __FILE__, __LINE__)

class h5obj;

struct chunk_desc_t {
	uint32_t size_of_chunk;
	uint32_t filters;
	uint64_t address;
	chunk_desc_t (uint32_t size_of_chunk, uint32_t filters, uint64_t address) : size_of_chunk{size_of_chunk}, filters{filters}, address{address} { }
	chunk_desc_t(chunk_desc_t const &) = default;
	chunk_desc_t & operator=(chunk_desc_t const &) = default;
};

struct _h5obj {
	_h5obj() = default;
	virtual ~_h5obj() = default;

	_h5obj(_h5obj const &) = delete;
	_h5obj & operator=(_h5obj const &) = delete;

	virtual auto get_id() const -> uint64_t = 0;
	virtual auto operator[](string const & name) const -> h5obj = 0;
	virtual auto shape() const -> vector<size_t> = 0;
	virtual auto shape(int i) const -> size_t = 0;
	virtual auto keys() const -> vector<string> = 0;
	virtual auto list_attributes() const -> vector<string> = 0;

	virtual auto modification_time() const -> uint32_t = 0;
	virtual auto comment() const -> char const * = 0;
	virtual auto element_size() const -> uint64_t = 0;

	virtual auto list_chunk() const -> vector<chunk_desc_t> {
		throw EXCEPTION("Not implemented");
	}

	virtual void print_info() const = 0;

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

	bool operator==(h5obj const & obj) const {
		return obj.get_id() == get_id();
	}

	vector<string> keys() {
		return _ptr->keys();
	}

	vector<string> list_attributes() {
		return _ptr->list_attributes();
	}

	uint64_t get_id() const {
		return _ptr->get_id();
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

	void print_info() const {
		_ptr->print_info();
	}

	auto modification_time() const -> uint32_t
	{
		return _ptr->modification_time();
	}

	auto comment() const -> char const *
	{
		return _ptr->comment();
	}

	auto element_size() const -> uint64_t
	{
		return _ptr->element_size();
	}

	auto list_chunk() const {
		return _ptr->list_chunk();
	}

	template<typename ... ARGS>
	void read(ARGS ... args);

};

} // h5ng

#endif /* SRC_H5NG_HXX_ */
