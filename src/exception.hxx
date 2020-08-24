/*
 * exception.hxx
 *
 *  Created on: 24 août 2020
 *      Author: benoit.gschwind
 */

#ifndef SRC_EXCEPTION_HXX_
#define SRC_EXCEPTION_HXX_

#include <exception>
#include <cstring>

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

}

// We must use macro to have __FILE__ and __LINE__
#define EXCEPTION(fmt, ...) exception(fmt " at %s:%d", ##__VA_ARGS__, __FILE__, __LINE__)



#endif /* SRC_EXCEPTION_HXX_ */
