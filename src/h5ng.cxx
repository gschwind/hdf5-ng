/*
 * h5ng.cxx
 *
 *  Created on: 6 mai 2018
 *      Author: benoit.gschwind
 */

#include "h5ng.hxx"
#include "hdf5-ng.hxx"

namespace h5ng {

h5obj::h5obj(string const & filename) {
	_ptr = make_shared<_h5file>(filename);
}

} // h5ng


