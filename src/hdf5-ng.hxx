/*
 * hdf5-ng.hxx
 *
 *  Created on: 25 avr. 2018
 *      Author: benoit.gschwind
 */

#ifndef SRC_HDF5_NG_HXX_
#define SRC_HDF5_NG_HXX_

#include <cstdint>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <cassert>

#include <map>
#include <string>
#include <iostream>
#include <stdexcept>
#include <memory>
#include <typeinfo>
#include <vector>
#include <list>
#include <stack>
#include <type_traits>
#include <limits>
#include <iomanip>
#include <algorithm>
#include <bitset>

#include "h5ng-spec.hxx"
#include "h5ng.hxx"

#define _STR(x) #x
#define STR(x) _STR(x)

namespace h5ng {

using namespace std;

static uint64_t const OFFSET_VERSION           =  8;

static uint64_t const OFFSET_V0_SIZE_OF_OFFSET = 13;
static uint64_t const OFFSET_V0_SIZE_OF_LENGTH = 14;

static uint64_t const OFFSET_V1_SIZE_OF_OFFSET = 13;
static uint64_t const OFFSET_V1_SIZE_OF_LENGTH = 14;

static uint64_t const OFFSET_V2_SIZE_OF_OFFSET =  9;
static uint64_t const OFFSET_V2_SIZE_OF_LENGTH = 10;

static uint64_t const OFFSET_V3_SIZE_OF_OFFSET =  9;
static uint64_t const OFFSET_V3_SIZE_OF_LENGTH = 10;

static uint64_t const OFFSET_V1_OBJECT_HEADER_VERSION = 0;
static uint64_t const OFFSET_V2_OBJECT_HEADER_VERSION = 4;

enum message_typeid_e : uint16_t {
	MSG_NIL                                = 0x0000u,
	MSG_DATASPACE                          = 0x0001u,
	MSG_LINK_INFO                          = 0x0002u,
	MSG_DATATYPE                           = 0x0003u,
	MSG_FILL_VALUE                         = 0x0004u,
	MSG_DATA_STORAGE_FILL_VALUE            = 0x0005u,
	MSG_LINK                               = 0x0006u,
	MSG_DATA_STORAGE                       = 0x0007u,
	MSG_DATA_LAYOUT                        = 0x0008u,
	MSG_BOGUS                              = 0x0009u,
	MSG_GROUP_INFO                         = 0x000Au,
	MSG_DATA_STORAGE_FILTER_PIPELINE       = 0x000Bu,
	MSG_ATTRIBUTE                          = 0x000Cu,
	MSG_OBJECT_COMMENT                     = 0x000Du,
	MSG_OBJECT_MODIFICATION_TIME_OLD       = 0x000Eu,
	MSG_SHARED_MESSAGE_TABLE               = 0x000Fu,
	MSG_OBJECT_HEADER_CONTINUATION         = 0x0010u,
	MSG_SYMBOL_TABLE                       = 0x0011u,
	MSG_OBJECT_MODIFICATION_TIME           = 0x0012u,
	MSG_BTREE_K_VALUE                      = 0x0013u,
	MSG_DRIVER_INFO                        = 0x0014u,
	MSG_ATTRIBUTE_INFO                     = 0x0015u,
	MSG_OBJECT_REFERENCE_COUNT             = 0x0016u
};


struct _named_tuple_0 {
	uint64_t offset_of_size_offset;
	uint64_t offset_of_size_length;
};

static _named_tuple_0 OFFSETX[4] = {
		{OFFSET_V0_SIZE_OF_OFFSET, OFFSET_V0_SIZE_OF_LENGTH},
		{OFFSET_V1_SIZE_OF_OFFSET, OFFSET_V1_SIZE_OF_LENGTH},
		{OFFSET_V2_SIZE_OF_OFFSET, OFFSET_V2_SIZE_OF_LENGTH},
		{OFFSET_V3_SIZE_OF_OFFSET, OFFSET_V3_SIZE_OF_LENGTH}
};

template<typename T>
static T & read_at(uint8_t * const addr)
{
	return *reinterpret_cast<T*>(addr);
}

template<unsigned alignement>
uint64_t align_forward(uint64_t ptr)
{
	uint64_t const mask = alignement-1;
	if (not (ptr&mask))
		return ptr;
	return ((ptr-1)&~mask)+alignement;
}



template<typename T>
ostream& operator<<(ostream &o, vector<T> const &v)
{
	o << "[";
	if (v.size() > 0) {
		for (unsigned i = 0; i < v.size() - 1; ++i) {
			o << std::dec << v[i] << ",";
		}
		o << std::dec << v.back();
	}
	o << "]";
	return o;

}

template<typename T>
string vector_to_hex_string(vector<T> const &v)
{
	ostringstream o;
	o << "0x";
	for (auto x: v) {
		o << std::hex << std::setw(sizeof(T)*2) << std::setfill('0') << static_cast<uint64_t>(x);
	}
	return o.str();
}

struct addr_reader {
	uint8_t * cur;
	addr_reader(uint8_t * cur) : cur{cur} { }

	addr_reader(addr_reader const &) = default;
	addr_reader & operator=(addr_reader const &) = default;

	template<typename T>
	inline T read()
	{
		T ret = *reinterpret_cast<T*>(cur);
		cur += sizeof(T);
		return ret;
	}

	inline uint64_t read_int(size_t s)
	{
		uint64_t ret;
		switch(s) {
		case 1:
			ret = read<uint8_t>();
			break;
		case 2:
			ret = read<uint16_t>();
			break;
		case 4:
			ret = read<uint32_t>();
			break;
		case 8:
			ret = read<uint64_t>();
			break;
		default:
			throw EXCEPTION("Unexpected read size");
		}
		return ret;
	}

	// Read string, will be zero padded if neadded
	inline string read_string(size_t size)
	{
		string ret = string{reinterpret_cast<char *>(cur), size};
		cur += size;
		return ret;
	}

	template<typename T>
	vector<T> read_array(size_t size)
	{
		auto ret = vector<T>{reinterpret_cast<T *>(cur), reinterpret_cast<T *>(cur) + size};
		cur += size*sizeof(T);
		return ret;
	}


};


template<typename T>
static bitset<sizeof(T)*8> make_bitset(T v) {
	return v;
}

struct slc {
	int bgn, end, inc; // note: bgn = 0 mean begin, but end = 0 mean end.
	slc() : bgn{0}, end{0}, inc{1} { } // all
	slc(int x) : bgn{x}, end{x+1}, inc{1} { } // one value
	slc(int bgn, int end) : bgn{bgn}, end{end}, inc{1} { } // range of value [bgn, end[
	slc(int bgn, int end, int inc) : bgn{bgn}, end{end}, inc{inc} { } // range of value with inc

	slc(slc const &) = default;
	slc & operator=(slc const &) = default;

	slc & update_norm_with_dims(int d) {
		*this = norm_with_dims(d);
		return *this;
	}

	slc norm_with_dims(int d) const {
		slc r{*this};
		if (inc >= 0) {
			if (bgn < 0) { r.bgn = bgn + d; }
			if (end <= 0) { r.end = end + d; } // 0 mean end
		} else {
			if (bgn <= 0) { r.bgn = bgn + d; } // 0 mean end
			if (end < 0) { r.end = end + d; }
		}
		return r;
	}

	friend ostream & operator<<(ostream & o, slc const & s) {
		return o << "<slc (" << s.bgn << "," << s.end << "," << s.inc << ") >";
	}

};


using data_reader_func = uint64_t (*)(uint8_t*);
static inline data_reader_func get_reader_for(uint8_t size) {
	switch(size) {
	case 1:
		return [](uint8_t * addr) -> uint64_t { return read_at<uint8_t>(addr); };
	case 2:
		return [](uint8_t * addr) -> uint64_t { return read_at<uint16_t>(addr); };
	case 4:
		return [](uint8_t * addr) -> uint64_t { return read_at<uint32_t>(addr); };
	case 8:
		return [](uint8_t * addr) -> uint64_t { return read_at<uint64_t>(addr); };
	default:
		return nullptr;
	}
}

/*****************************************************
 * HIGH LEVEL API
 *****************************************************/

struct file_handler_interface;

struct superblock_interface;
struct object_interface;

struct superblock_interface {

	virtual ~superblock_interface() = default;

	virtual int version() = 0;
	virtual int offset_size() = 0;
	virtual int length_size() = 0;
	virtual int group_leaf_node_K() = 0;
	virtual int group_internal_node_K() = 0;
	virtual int indexed_storage_internal_node_K() = 0;
	virtual int object_version() = 0;
	virtual uint64_t base_address() = 0;
	virtual uint64_t file_free_space_info_address() = 0;
	virtual uint64_t end_of_file_address() = 0;
	virtual uint64_t driver_information_address() = 0;
	virtual uint64_t root_node_object_address() = 0;

	virtual shared_ptr<object_interface> get_root_object() = 0;

};

enum object_type_e {
	UNDEF,
	DATASET,
	GROUP,
	SUPERBLOCKEXTENSION
};

enum dataset_layout_e {
	UNDEFX
};



struct btree {

	virtual ~btree() = default;
	virtual int version() = 0;

};

// object can be :
//  - superblock extension
//  - dataset
//  - group
struct object_interface : public _h5obj {

	virtual ~object_interface() = default;

	virtual auto get_id() const -> uint64_t override {
		throw EXCEPTION("Not implemented");
	}

	virtual auto operator[](string const & name) const -> h5obj override {
		throw EXCEPTION("Not implemented");
	}

	virtual vector<size_t> shape() const override {
		throw EXCEPTION("Not implemented");
	}

	virtual size_t shape(int i) const override {
		throw EXCEPTION("Not implemented");
	}

	virtual auto keys() const -> vector<string> override {
		throw EXCEPTION("Not implemented");
	}

	virtual auto list_attributes() const -> vector<string> override {
		throw EXCEPTION("Not implemented");
	}

	virtual void print_info() const override {
		throw EXCEPTION("Not implemented");
	}

	virtual auto modification_time() const -> uint32_t override
	{
		throw EXCEPTION("Not implemented");
	}

	virtual auto comment() const -> char const * override
	{
		throw EXCEPTION("Not implemented");
	}

	virtual auto element_size() const -> uint64_t {
		throw EXCEPTION("Not implemented");
	}

	virtual uint8_t * continuous_data() const {
		throw EXCEPTION("Not implemented");
	}

	virtual uint8_t * fill_value() const {
		throw EXCEPTION("Not implemented");
	}

	virtual uint8_t data_layout() const {
		throw EXCEPTION("Not implemented");
	}

	virtual vector<size_t> shape_of_chunk() const {
		throw EXCEPTION("Not implemented");
	}

	virtual uint8_t * dataset_find_chunk(uint64_t const * key) const {
		throw EXCEPTION("Not implemented");
	}

	virtual vector<chunk_desc_t> list_chunk() const
	{
		throw EXCEPTION("Not implemented");
	}

	template<int R>
	struct iterator {
		array<int64_t, R> shape;
		array<int64_t, R> strides;
		array<int64_t, R> current;
		uint8_t * offset;

        /* the begin iterator */
		iterator(uint8_t * offset, array<int64_t, R> shape, array<int64_t, R> stride) :
			shape{shape},
			strides{stride},
			current{shape},
			offset{offset}
		{

		}

		iterator(iterator const &) = default;

		iterator & operator=(iterator const &) = default;

		void _incr() {
			for(size_t x = R-1; x >= 0; --x) {
				current[x] -= 1;
				offset += strides[x];
				if(current[x] == 0 and x != 0) {
					current[x] = shape[x];
					offset -= shape[x]*strides[x];
				} else {
					break;
				}
			}
		}

		iterator& operator++() { // prefix
			_incr();
			return (*this);
		}

		iterator operator++(int) { // postfix
			iterator ret{*this};
			_incr();
			return ret;
		}

		uint8_t * operator *() {
			return offset;
		}

		uint8_t * operator *() const {
			return offset;
		}

		bool operator==(iterator const & x) const {
			return offset == x.offset;
		}

		bool operator!=(iterator const & x) const {
			return offset != x.offset;
		}

		void print() {
			for(auto x: strides) {
				std::cout << x << " ";
			}
			for(auto x: current) {
				std::cout << x << " ";
			}
			std::cout << std::endl;
		}

		friend std::ostream & operator<<(std::ostream & o, iterator const & r) {
			return o << "<iterator " << r.offset << " >";
		}

	};


	template<int R>
	struct simple_iterator {
		array<slc, R> const slices;
		array<uint64_t, R> current;

        /* the begin iterator */
		simple_iterator(array<slc, R> const & slices) :
			slices{slices}
		{
			for(int i = 0; i < R; ++i) { current[i] = slices[i].bgn; }
		}

		simple_iterator(simple_iterator const &) = default;
		simple_iterator & operator=(simple_iterator const &) = default;

		void incr() {
			for(size_t x = R-1; x >= 0; --x) {
				current[x] += slices[x].inc;
				if(current[x] >= slices[x].end and x != 0) {
					current[x] = slices[x].bgn;
				} else {
					break;
				}
			}
		}

		bool is_end() {
			return current[0] >= slices[0].end;
		}

	};


	template<size_t R>
	void _read_continuous(array<slc, R> const & selection, void * output)
	{
		// ony for continuous or compact.
		uint64_t element_size = this->element_size();

		auto data_shape = shape();
		if (data_shape.size() != R)
			throw EXCEPTION("dataset rank (%d) does not match selection rank (%d)", data_shape.size(), R);

		array<int64_t, R> data_stride;
		data_stride[R-1] = element_size;
		for(size_t i = R-1; i > 0; --i) { data_stride[i-1] = data_stride[i]*data_shape[i]; }

		array<int64_t, R> shape;
		array<int64_t, R> stride;
		uint8_t * offset = continuous_data();
		for(size_t i = 0; i < R; ++i) {
			auto s = selection[i].norm_with_dims(data_shape[i]);
			offset += data_stride[i]*s.bgn;
			stride[i] = data_stride[i]*s.inc;
			shape[i] = ((s.end - s.bgn) - 1)/s.inc + 1;
		}

		iterator<R> iter{offset, shape, stride};
		iterator<R> end{offset + stride[0]*shape[0], shape, stride};

		auto cursor = reinterpret_cast<uint8_t *>(output);
		while(iter != end) {
			std::copy(&(*iter)[0], &(*iter)[element_size], cursor);
			cursor += element_size;
			++iter;
		}

	}


	template<size_t R>
	void _read_chunked(array<slc, R> selection, void * output)
	{
		// ony for continuous or compact.
		uint64_t element_size = this->element_size();

		auto data_shape = shape();
		if (data_shape.size() != R)
			throw EXCEPTION("dataset rank (%d) does not match selection rank (%d)", data_shape.size(), R);

		auto chunk_shape = shape_of_chunk();

		// normalize
		for(size_t i = 0; i < R; ++i) {
			selection[i] = selection[i].norm_with_dims(data_shape[i]);
		}

		array<uint64_t, R> chunk_stride;
		chunk_stride[R-1] = element_size;
		for(size_t i = R-1; i > 0; --i) { chunk_stride[i-1] = chunk_shape[i]*chunk_stride[i]; }

		auto cursor = reinterpret_cast<uint8_t *>(output);
		for(simple_iterator<R> iter{selection}; not iter.is_end(); iter.incr()) {
			array<uint64_t, R> chunk_offset;
			for(size_t i = 0; i < R; ++i) { chunk_offset[i] = iter.current[i] - iter.current[i]%chunk_shape[i]; }

			uint8_t * chunk = dataset_find_chunk(&chunk_offset[0]);
			if (chunk) {
				for(size_t i = 0; i < R; ++i) {
					chunk += chunk_stride[i]*(iter.current[i]-chunk_offset[i]);
				}
				std::copy(&chunk[0], &chunk[element_size], cursor);
			} else {
				if (fill_value()) {
					uint8_t * x = fill_value();
					std::copy(&x[0], &x[element_size], cursor);
				} else {
					std::fill(&cursor[0], &cursor[element_size], -1);
				}
			}
			cursor += element_size;
		}

	}


	template<size_t R>
	void _read(array<slc, R> const & selection, void * output)
	{
		switch(data_layout()) {
		case 0: // compact
			_read_continuous(selection, output);
			break;
		case 1: // continuous
			_read_continuous(selection, output);
			break;
		case 2: // chunked
			// TODO
			_read_chunked(selection, output);
			break;
		case 3: // virtual
			// TODO
			throw runtime_error("VIRTUAL data layout not implemented");
			break;
		default:
			throw runtime_error("unknown data layout");
		}

	}

	template<typename ... ARGS>
	struct _dispatch_read;

	template<typename ... ARGS>
	struct _dispatch_read<void *, ARGS...> {
		static void exec(object_interface * obj, void * output, ARGS ... args) {
			obj->_read(array<slc, sizeof...(ARGS)>{slc{args}...}, output);
		}
	};

	template<typename ... ARGS>
	void _read_0(ARGS ... args) {
		_dispatch_read<ARGS...>::exec(this, args...);
	}

};

struct file_handler_interface {
	virtual ~file_handler_interface() = default;

	virtual auto get_superblock() -> shared_ptr<superblock_interface> = 0;

	virtual auto make_superblock(uint64_t offset) -> shared_ptr<superblock_interface> = 0;
	virtual auto make_object(uint64_t offset) -> shared_ptr<object_interface> = 0;

};

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
struct _impl {

using spec_defs = typename h5ng::spec_defs<SIZE_OF_OFFSET, SIZE_OF_LENGTH>;

using offset_type = typename get_type_for_size<SIZE_OF_OFFSET>::type;
using length_type = typename get_type_for_size<SIZE_OF_LENGTH>::type;

static offset_type constexpr undef_offset = std::numeric_limits<offset_type>::max();
static length_type constexpr undef_length = std::numeric_limits<length_type>::max();


static uint64_t get_minimum_storage_size_for(uint64_t count) {
	if (count == 0) return 0;
	uint64_t n = 1;
	while((/*256^n*/(0x1ul<<(n<<3))-1)<count) {
		n <<= 1; /* n*=2 */
		if (n == 8) return n; // reach the maximum storage capacity;
	}
	return n;
}

struct file_handler_t : public h5ng::file_handler_interface {
	mutable map<uint64_t, shared_ptr<superblock_interface>> superblock_cache;
	mutable map<uint64_t, shared_ptr<object_interface>> object_cache;

	uint8_t * memaddr;

	int version;
	uint64_t superblock_offset;

	file_handler_t(uint8_t * memaddr, uint64_t superblock_offset, int version) :
		memaddr{memaddr},
		version{version},
		superblock_offset{superblock_offset}
	{

	}

	auto to_address(uint64_t offset) -> uint8_t * {
		return &memaddr[offset];
	}

	auto to_offset(uint8_t * address) -> uint64_t {
		return address-memaddr;
	}

	virtual auto get_superblock() -> shared_ptr<superblock_interface> override
	{
		return make_superblock(superblock_offset);
	}

	// h5ng::file_handler_interface

	virtual auto make_superblock(uint64_t offset) -> shared_ptr<superblock_interface> override;
	virtual auto make_object(uint64_t offset) -> shared_ptr<object_interface> override;

};

struct object {
	file_handler_t * file;
	uint8_t * memory_addr;

	object() = delete;
	object(file_handler_t * file, uint8_t * memory_addr) : file{file}, memory_addr{memory_addr} { }

};

struct superblock_v0 : public object, public superblock_interface {
	using spec = typename spec_defs::superblock_v0_spec;

	using object::file;
	using object::memory_addr;

	superblock_v0(file_handler_t * file, uint8_t * addr) : object{file, addr} { }

	virtual int version() {
		return spec::superblock_version::get(memory_addr);
	}

	virtual int offset_size() {
		return spec::size_of_offsets::get(memory_addr);
	}

	virtual int length_size() {
		return spec::size_of_length::get(memory_addr);
	}

	virtual int group_leaf_node_K() {
		return spec::group_leaf_node_K::get(memory_addr);
	}

	virtual int group_internal_node_K() {
		return spec::group_internal_node_K::get(memory_addr);
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual int object_version() {
		return -1;
	}

	virtual uint64_t base_address() {
		return spec::base_address::get(memory_addr);
	}

	virtual uint64_t file_free_space_info_address() {
		return spec::free_space_info_address::get(memory_addr);
	}
	virtual uint64_t end_of_file_address() {
		return spec::end_of_file_address::get(memory_addr);
	}

	virtual uint64_t driver_information_address() {
		return spec::driver_information_address::get(memory_addr);
	}

	virtual uint64_t root_node_object_address() {
		return reinterpret_cast<group_symbol_table_entry*>(&spec::root_group_symbol_table_entry::get(memory_addr))->offset_header_address();
	}

	virtual shared_ptr<object_interface> get_root_object() {
		return file->make_object(root_node_object_address());
	}

};

struct superblock_v1 : public object, public superblock_interface {
	using spec = typename spec_defs::superblock_v1_spec;

	using object::file;
	using object::memory_addr;

	superblock_v1(file_handler_t * file, uint8_t * addr) : object{file, addr} { }

	virtual int version() {
		return spec::superblock_version::get(memory_addr);
	}

	virtual int offset_size() {
		return spec::size_of_offsets::get(memory_addr);
	}

	virtual int length_size() {
		return spec::size_of_length::get(memory_addr);
	}

	virtual int group_leaf_node_K() {
		return spec::group_leaf_node_K::get(memory_addr);
	}

	virtual int group_internal_node_K() {
		return spec::group_internal_node_K::get(memory_addr);
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual int object_version() {
		return -1;
	}

	virtual uint64_t base_address() {
		return spec::base_address::get(memory_addr);
	}

	virtual uint64_t file_free_space_info_address() {
		return spec::base_address::get(memory_addr);
	}
	virtual uint64_t end_of_file_address() {
		return spec::end_of_file_address::get(memory_addr);
	}

	virtual uint64_t driver_information_address() {
		return spec::driver_information_address::get(memory_addr);
	}

	virtual uint64_t root_node_object_address() {
		return reinterpret_cast<group_symbol_table_entry*>(&spec::root_group_symbol_table_entry::get(memory_addr))->offset_header_address();
	}

	virtual shared_ptr<object_interface> get_root_object() {
		return file->make_object(root_node_object_address());
	}

	template<int R>
	void _read(array<slc, R> const & selection) {

	}

};

struct superblock_v2 : public object, public superblock_interface {
	using spec = typename spec_defs::superblock_v2_spec;

	using object::file;
	using object::memory_addr;

	superblock_v2(file_handler_t * file, uint8_t * addr) : object{file, addr} { }

	virtual int version() {
		return spec::superblock_version::get(memory_addr);
	}

	virtual int offset_size() {
		return spec::size_of_offsets::get(memory_addr);
	}

	virtual int length_size() {
		return spec::size_of_length::get(memory_addr);
	}

	virtual int group_leaf_node_K() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual int group_internal_node_K() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual int object_version() {
		return -1;
	}

	virtual uint64_t base_address() {
		return spec::base_address::get(memory_addr);
	}

	virtual uint64_t file_free_space_info_address() {
		return spec::base_address::get(memory_addr);
	}
	virtual uint64_t end_of_file_address() {
		return spec::end_of_file_address::get(memory_addr);
	}

	virtual uint64_t driver_information_address() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual uint64_t root_node_object_address() {
		return spec::root_group_object_header_address::get(memory_addr);
	}

	virtual shared_ptr<object_interface> get_root_object() {
		return file->make_object(root_node_object_address());
	}

};

struct superblock_v3 : public object, public superblock_interface {
	using spec = typename spec_defs::superblock_v3_spec;

	using object::file;
	using object::memory_addr;

	superblock_v3(file_handler_t * file, uint8_t * addr) : object{file, addr} { }

	virtual int version() {
		return spec::superblock_version::get(memory_addr);
	}

	virtual int offset_size() {
		return spec::size_of_offsets::get(memory_addr);
	}

	virtual int length_size() {
		return spec::size_of_length::get(memory_addr);
	}

	virtual int group_leaf_node_K() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual int group_internal_node_K() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual int object_version() {
		return -1;
	}

	virtual uint64_t base_address() {
		return spec::base_address::get(memory_addr);
	}

	virtual uint64_t file_free_space_info_address() {
		return spec::base_address::get(memory_addr);
	}
	virtual uint64_t end_of_file_address() {
		return spec::end_of_file_address::get(memory_addr);
	}

	virtual uint64_t driver_information_address() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual uint64_t root_node_object_address() {
		return spec::root_group_object_header_address::get(memory_addr);
	}

	virtual shared_ptr<object_interface> get_root_object() {
		return file->make_object(root_node_object_address());
	}

};

struct local_heap_v0 : public object {
	using spec = typename spec_defs::local_heap_spec;

	using object::file;
	using object::memory_addr;

	local_heap_v0(file_handler_t * file, uint8_t * addr) : object{file, addr} { }

	auto get_data(uint64_t offset) const -> uint8_t *
	{
		return file->to_address(spec::data_segment_address::get(memory_addr))+offset;
	}

	auto data_segment_size() -> length_type &
	{
		return spec::data_segment_size::get(memory_addr);
	}

	auto offset_of_head_of_free_list() -> length_type &
	{
		return spec::offset_to_head_free_list::get(memory_addr);
	}

};

struct b_tree_xxx_t {
	uint64_t maximum_child_node;
	uint64_t total_maximum_child_node;
	uint64_t maximum_child_node_size;
	uint64_t total_maximum_child_node_size;
	uint64_t (*read_maximum_child_node)(uint8_t *);
	uint64_t (*read_total_maximum_child_node)(uint8_t *);
};

template<typename NODE_TYPE>
struct btree_v2_node : public object {
	using spec = typename spec_defs::b_tree_v2_node_spec;

	using object::file;
	using object::memory_addr;

	b_tree_xxx_t const & layout;
	uint64_t child_node_count;

	btree_v2_node(file_handler_t * file, uint8_t * addr, b_tree_xxx_t const & layout, uint64_t child_node_count) :
		object{file, addr}, layout{layout}, child_node_count{child_node_count} { }
	btree_v2_node(btree_v2_node const &) = default;
	btree_v2_node & operator=(btree_v2_node const & x) = default;

	uint32_t signature() const
	{
		return spec::signature::get(memory_addr);
	}

	uint8_t version() const
	{
		return spec::version::get(memory_addr);
	}

	uint8_t type() const
	{
		return spec::type::get(memory_addr);
	}

	uint8_t * get_record(int n) const
	{
		return memory_addr + spec::size + (n*NODE_TYPE::size);
	}

	uint8_t * get_subnode(int n) const {
		return memory_addr + spec::size + (layout.maximum_child_node*NODE_TYPE::size) + (n*(SIZE_OF_OFFSET+layout.maximum_child_node_size+layout.total_maximum_child_node_size));
	}

	uint64_t get_subnode_offset(int n) const {
		return read_at<offset_type>(get_subnode(n));
	}

	uint64_t get_subnode_child_count(int n) const {
		return layout.read_maximum_child_node(get_subnode(n)+SIZE_OF_OFFSET);
	}

	uint64_t get_subnode_total_child_count(int n) const {
		return layout.read_total_maximum_child_node(get_subnode(n)+SIZE_OF_OFFSET+layout.maximum_child_node_size);
	}


};

template<typename NOTE_TYPE>
struct btree_v2_header : public object {
	using spec = typename spec_defs::b_tree_v2_hdr_spec;

	using object::file;
	using object::memory_addr;

	btree_v2_header(file_handler_t * file, uint8_t * addr) : object{file, addr} { }
	btree_v2_header(btree_v2_header const &) = default;
	btree_v2_header & operator=(btree_v2_header const & x) = default;

	uint8_t * signature() const
	{
		return spec::signature::get(memory_addr);
	}

	uint8_t version() const
	{
		return spec::version::get(memory_addr);
	}

	uint8_t type() const
	{
		return spec::type::get(memory_addr);
	}

	uint32_t node_size() const
	{
		return spec::node_size::get(memory_addr);
	}

	uint16_t record_size() const
	{
		return spec::record_size::get(memory_addr);
	}

	uint16_t depth() const
	{
		return spec::depth::get(memory_addr);
	}

	uint8_t split_percent() const
	{
		return spec::split_percent::get(memory_addr);
	}

	uint8_t merge_percent() const
	{
		return spec::merge_percent::get(memory_addr);
	}

	uint64_t root_node_address() const
	{
		return spec::root_node_address::get(memory_addr);
	}

	uint16_t number_of_record_in_root_node() const
	{
		return spec::number_of_records_in_root_node::get(memory_addr);
	}

	uint64_t total_number_of_records_in_b_tree() const
	{
		return spec::total_number_of_record_in_b_tree::get(memory_addr);
	}

	uint32_t checksum() const
	{
		return spec::checksum::get(memory_addr);
	}

	void maximum_stored_records(uint64_t depth, uint64_t & maximum_child_node, uint64_t & total_maximum_child_node) {
		if (depth == 0) {
			maximum_child_node = 0;
			total_maximum_child_node = 0;
		} else if (depth == 1) {
			maximum_child_node = (node_size() - 10) / record_size();
			total_maximum_child_node = maximum_child_node;
		} else {
			uint64_t maximum_child_node_count_0;
			uint64_t total_maximum_child_node_count_0;

			maximum_stored_records(depth-1, maximum_child_node_count_0, total_maximum_child_node_count_0);

			uint64_t n0 = get_minimum_storage_size_for(maximum_child_node_count_0);
			uint64_t n1 = get_minimum_storage_size_for(total_maximum_child_node_count_0);
			maximum_child_node = (node_size() - 10 - n1 - n0 - SIZE_OF_OFFSET) / (record_size() + n1 + n0 + SIZE_OF_OFFSET);
			total_maximum_child_node = total_maximum_child_node_count_0*maximum_child_node;

		}
	}

	auto list_records() const -> vector<uint8_t *>
	{
		cout << "ZZZ=" << signature()[0] << signature()[1] << signature()[2] << signature()[3] << endl;
		cout << "ZZZ=" <<  total_number_of_records_in_b_tree() << endl;

		// pre-compute some b_tree data
		vector<b_tree_xxx_t> xxx{depth()};
		for(int d = 0; d < depth(); ++d) {
			if (d == 0) {
				xxx[d].maximum_child_node = 0;
				xxx[d].total_maximum_child_node = 0;
				xxx[d].maximum_child_node_size = 0;
				xxx[d].total_maximum_child_node_size = 0;
				xxx[d].read_maximum_child_node = 0;
				xxx[d].read_total_maximum_child_node = 0;
			} else {
				xxx[d].maximum_child_node_size = get_minimum_storage_size_for(xxx[d-1].maximum_child_node);
				xxx[d].total_maximum_child_node_size = get_minimum_storage_size_for(xxx[d-1].total_maximum_child_node);
				xxx[d].maximum_child_node = (node_size() - 10 - xxx[d].total_maximum_child_node_size - xxx[d].maximum_child_node_size - SIZE_OF_OFFSET) / (record_size() + xxx[d].total_maximum_child_node_size + xxx[d].maximum_child_node_size + SIZE_OF_OFFSET);
				xxx[d].total_maximum_child_node = xxx[d-1].total_maximum_child_node*xxx[d].maximum_child_node;
				xxx[d].read_maximum_child_node = get_reader_for(xxx[d].maximum_child_node_size);
				xxx[d].read_total_maximum_child_node = get_reader_for(xxx[d].total_maximum_child_node_size);
			}
		}


		vector<uint8_t *> ret{total_number_of_records_in_b_tree()};
		stack<pair<uint64_t, btree_v2_node<NOTE_TYPE>>> s;

		s.emplace(0, btree_v2_node<NOTE_TYPE>{file, file->to_address(root_node_address()), xxx[depth()-1], number_of_record_in_root_node()});

		uint64_t cur = 0;
		while(not s.empty()) {
			auto & iter = s.top().first;
			auto const & node = s.top().second;

			uint64_t d = depth() - s.size();

			if (iter < node.child_node_count) {
				if (d != 0) {
					uint8_t * next = file->to_address(node.get_subnode_offset(iter));
					s.emplace(0, btree_v2_node<NOTE_TYPE>{file, next, xxx[d], node.get_subnode_child_count(iter)});
					++(iter);
				} else {
					ret[cur++] = node.get_record(iter);
					++(iter);
				}
			} else {
				s.pop();
				if (s.top().first < s.top().second.child_node_count-1) {
					ret[cur++] = s.top().second.get_record(s.top().first);
				}
			}
		}

		return ret;

	}


};


struct btree_v1 {
	using spec = typename spec_defs::b_tree_v1_hdr_spec;

	uint8_t * memory_addr;

	btree_v1(uint8_t * memory_addr) : memory_addr{memory_addr} { }

	btree_v1(btree_v1 const &) = default;
	btree_v1 & operator=(btree_v1 const & x) = default;

	/* Note : there is K+1 keys */
	uint8_t * get_key(uint64_t key_length, int i) const
	{
		return &memory_addr[spec::size+i*(SIZE_OF_OFFSET+key_length)];
	}

	/* Note: there is K nodes */
	offset_type get_node(uint64_t key_length, int i) const
	{
		return read_at<offset_type>(&memory_addr[spec::size+i*(SIZE_OF_OFFSET+key_length)+key_length]);
	}

	uint8_t get_depth() const
	{
		return spec::node_level::get(memory_addr);
	}

	uint16_t get_entries_count() const
	{
		return spec::entries_used::get(memory_addr);
	}

};


struct group_btree_v1 : public btree_v1 {

	group_btree_v1(uint8_t * addr) : btree_v1{addr} { }
	group_btree_v1(group_btree_v1 const &) = default;
	group_btree_v1 & operator=(group_btree_v1 const &) = default;

	length_type get_key(uint64_t key_length, int i) const
	{
		return *reinterpret_cast<length_type*>(btree_v1::get_key(key_length, i));
	}

};


struct group_symbol_table_entry {
	using spec = typename spec_defs::group_symbol_table_entry_spec;

	group_symbol_table_entry() = delete;

	offset_type & link_name_offset() {
		return spec::link_name_offset::get(reinterpret_cast<uint8_t*>(this));
	}

	offset_type & offset_header_address() {
		return spec::object_header_address::get(reinterpret_cast<uint8_t*>(this));
	}

	uint32_t & cache_type() {
		return spec::cache_type::get(reinterpret_cast<uint8_t*>(this));
	}

	uint8_t * scratch_pad() {
		return &spec::scratch_pad_space::get(reinterpret_cast<uint8_t*>(this));
	}

};

template<typename RETURN, size_t const SIZE>
struct raw_entry_list {
	uint8_t * addr;
	uint64_t size;

	raw_entry_list(uint8_t * addr, uint64_t size) : addr{addr}, size{size} { }

	struct iterator {
		uint8_t * addr;

		iterator(uint8_t * addr) : addr{addr} { }

		iterator & operator++() {
			addr += SIZE;
			return *this;
		}

		iterator operator++(int) {
			iterator ret{addr};
			addr += SIZE;
			return ret;
		}

		RETURN * operator*() {
			return reinterpret_cast<RETURN*>(addr);
		}

		bool operator==(iterator const & x) {
			return addr == x.addr;
		}

		bool operator!=(iterator const & x) {
			return addr != x.addr;
		}

	};

	iterator begin() const {
		return iterator{addr};
	}

	iterator end() const {
		return iterator{addr + size*SIZE};
	}

};


struct group_symbol_table {
	using spec = typename spec_defs::group_symbol_table_spec;

	uint8_t * addr;

	group_symbol_table(uint8_t * addr) : addr{addr} { }

	uint8_t & version() {
		return spec::version::get(addr);
	}

	uint16_t & number_of_symbols() {
		return spec::number_of_symbols::get(addr);
	}

	group_symbol_table_entry get_symbol_table_entry(int i) {
		return group_symbol_table_entry{addr + spec::size + i * spec_defs::group_symbol_table_entry_spec::size};
	}

	using symbol_table_entry_list = raw_entry_list<group_symbol_table_entry, spec_defs::group_symbol_table_entry_spec::size>;

	symbol_table_entry_list get_symbole_entry_list() {
		return symbol_table_entry_list{addr+spec::size, number_of_symbols()};
	}

};

template<typename record_type>
struct group_btree_v2_node;
template<typename record_type>
struct group_btree_v2;

template<typename record_type>
struct group_btree_v2_node {
	using spec = typename spec_defs::b_tree_v2_node_spec;

	group_btree_v2<record_type> * root;
	uint8_t * memory_addr;
	uint64_t record_count;
	uint64_t depth;

	uint64_t size_of_number_of_child_node;
	uint64_t size_of_total_number_of_child_node;

	data_reader_func read_number_of_child_node;
	data_reader_func read_total_number_of_child_node;

	group_btree_v2_node(group_btree_v2<record_type> * root, uint8_t * memory_addr, uint64_t record_count, uint64_t depth);

	auto get_record(int i) -> uint8_t *;
	auto get_node(int i) -> offset_type;
	auto get_number_of_child_node(int i) -> uint64_t;
	auto get_total_number_of_child_node(int i) -> uint64_t;

};


template<typename record_spec>
struct group_btree_v2 : public object {

	using object::file;
	using object::memory_addr;

	using spec = typename spec_defs::b_tree_v2_hdr_spec;
	using node_type = group_btree_v2_node<record_spec>;

	group_btree_v2(uint8_t * addr) {
		this->memory_addr = addr;
	}

	group_btree_v2(group_btree_v2 const &) = default;

	group_btree_v2 & operator=(group_btree_v2 const & x) = default;


	uint64_t get_depth() {
		return *spec::depth::get(memory_addr);
	}

	uint64_t record_size() {
		return *spec::record_size::get(memory_addr);
	}

	uint64_t node_size() {
		return *spec::node_size::get(memory_addr);
	}

	void maximum_stored_records(uint64_t depth, uint64_t & maximum_child_node, uint64_t & total_maximum_child_node) {
		if (depth == 0) {
			maximum_child_node = 0;
			total_maximum_child_node = 0;
		} else if (depth == 1) {
			maximum_child_node = (node_size() - 10) / record_size();
			total_maximum_child_node = maximum_child_node;
		} else {
			uint64_t maximum_child_node_count_0;
			uint64_t total_maximum_child_node_count_0;

			maximum_stored_records(depth-1, maximum_child_node_count_0, total_maximum_child_node_count_0);

			uint64_t n0 = get_minimum_storage_size_for(maximum_child_node_count_0);
			uint64_t n1 = get_minimum_storage_size_for(total_maximum_child_node_count_0);
			maximum_child_node = (node_size() - 10 - n1 - n0 - SIZE_OF_OFFSET) / (record_size() + n1 + n0 + SIZE_OF_OFFSET);
			total_maximum_child_node = total_maximum_child_node_count_0*maximum_child_node;

		}
	}

	// the return value must be temporary, a call of (undef function) can
	// invalidate this pointer.
//	shared_ptr<node_type> get_btree_node(uint64_t offset, uint64_t record_count, uint64_t depth) {
//		auto x = _node_tree_cache.find(offset);
//		if(x != _node_tree_cache.end())
//			return x->second;
//		return (_node_tree_cache[offset] = make_shared<node_type>(this, &base_addr()+offset, record_count, depth)).get();
//	}

};

struct object_message_handler_t {
	uint16_t type;   //< actual type
	uint16_t size;   //< size of message in the current message block, not the actual size, may be shared or padded
	bitset<8> flags; //< flags of current messages
	uint8_t * data;  //< pointer to message data
};


struct object_symbol_table_t {
	file_handler_t * file;

	offset_type local_heap;
	offset_type group_btree_v1_root;

	// Create the symbole table from message.
	object_symbol_table_t(file_handler_t * file, uint8_t * msg) : file{file}
	{
//		cout << "parse_symbol_table " << std::dec
//				<< spec_defs::message_symbole_table_spec::local_heap_address::get(msg) << " "
//				<< spec_defs::message_symbole_table_spec::b_tree_v1_address::get(msg)
//				<< endl;
		local_heap = spec_defs::message_symbole_table_spec::local_heap_address::get(msg);
		group_btree_v1_root = spec_defs::message_symbole_table_spec::b_tree_v1_address::get(msg);
	}

	char const * _get_link_name(uint64_t offset) const {
		local_heap_v0 h{file, file->to_address(local_heap)};
		return reinterpret_cast<char *>(h.get_data(offset));
	}

	offset_type operator[](string const & key) const
	{
		return this->operator [](key.c_str());
	}

	offset_type operator[](char const * key) const
	{
		auto group_symbol_table_offset = _group_find_symbol_table(key);
		group_symbol_table table{file->to_address(group_symbol_table_offset)};
		for(auto symbol_table_entry: table.get_symbole_entry_list()) {
			char const * link_name = _get_link_name(symbol_table_entry->link_name_offset());
			cout << "check link name = " << link_name << endl;
			if (std::strcmp(key, link_name) == 0)
				return symbol_table_entry->offset_header_address();
		}

		throw EXCEPTION("key `%s' not found", key);

	}

	/**
	 * lookup through the b-tree table to find the index of the next b-tree node.
	 **/
	size_t _group_find_symbol_table_index(group_btree_v1 const & cur, char const * key) const
	{
		for(size_t i = 0; i < cur.get_entries_count(); ++i) {
			char const * link_name = _get_link_name(cur.get_key(SIZE_OF_LENGTH, i+1));
			if (std::strcmp(key, link_name) <= 0)
				return i;
		}
		throw EXCEPTION("Ill-formed group_btree_v1");
	}

	/** return group_symbole_table that should content the key **/
	offset_type _group_find_symbol_table(char const * key) const {
		group_btree_v1 cur{file->to_address(group_btree_v1_root)};
		if (std::strcmp(key, _get_link_name(cur.get_key(SIZE_OF_LENGTH, cur.get_entries_count()))) > 0)
			throw EXCEPTION("Key `%s' not found", key);

		while(cur.get_depth() != 0) {
			size_t i = _group_find_symbol_table_index(cur, key);
			cur = group_btree_v1{file->to_address(cur.get_node(SIZE_OF_LENGTH, i))};
		}

		size_t i = _group_find_symbol_table_index(cur, key);
		return cur.get_node(SIZE_OF_LENGTH, i);

	}

	void ls(vector<string> & ret) const
	{
		stack<group_btree_v1> stack;

		vector<offset_type> group_symbole_tables;
		cout << "btree-root = " << std::hex << group_btree_v1_root << std::dec << endl;
		stack.push(group_btree_v1{file->to_address(group_btree_v1_root)});
		while(not stack.empty()) {
			group_btree_v1 cur = stack.top();
			cout << std::dec << "process node depth=" << cur.get_depth() << " entries_count=" << cur.get_entries_count() << endl;
			stack.pop();
			if (cur.get_depth() == 0) {
				//assert(cur.get_entries_count() <= lK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					group_symbole_tables.push_back(cur.get_node(SIZE_OF_LENGTH, i));
				}
			} else {
				//assert(cur.get_entries_count() <= nK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					stack.push(group_btree_v1{file->to_address(cur.get_node(SIZE_OF_LENGTH, i))});
				}
			}
		}

		for(auto offset: group_symbole_tables) {
			group_symbol_table table{file->to_address(offset)};
			for(auto symbol_table_entry: table.get_symbole_entry_list()) {
				ret.push_back(_get_link_name(symbol_table_entry->link_name_offset()));
			}
		}

	}


	vector<string> ls() const
	{
		vector<string> ret;
		ls(ret);
		return ret;
	}


};

// Attributes info
struct object_attribute_info_t {
	file_handler_t * file;

	uint64_t maximum_creation_index;
	uint64_t fractal_heap_address;
	uint64_t attribute_name_btree_address;
	uint64_t attribute_creation_order_btree_address;


	object_attribute_info_t(file_handler_t * file, uint8_t * msg) : file{file}
	{
		auto flags = make_bitset(spec_defs::message_attribute_info_spec::flags::get(msg));
		auto cur = addr_reader{msg+spec_defs::message_attribute_info_spec::size};

		if (flags.test(0)) {
			maximum_creation_index = cur.read<uint16_t>();
		}

		fractal_heap_address = cur.read<offset_type>();
		attribute_name_btree_address = cur.read<offset_type>();

		if (flags.test(1)) {
			attribute_creation_order_btree_address = cur.read<offset_type>();
		}
	}

	void ls(vector<string> & ret) {

		auto btree = btree_v2_header<typename spec_defs::btree_v2_record_type8>(file, file->to_address(attribute_name_btree_address));
		auto xx = btree.list_records();
		cout << "XXXXX" << endl;
		for(auto i: xx) {
			cout << (void*)i << endl;
		}
		cout << "TTTTT" << endl;
	}


};


struct object_dataspace_t {
	uint8_t rank;
	vector<uint64_t> shape;
	vector<uint64_t> max_shape;
	vector<uint64_t> permutation; // Never implement in official lib.

	object_dataspace_t(uint8_t * msg) {
	//		cout << "parse_dataspace " << std::dec <<
	//				" version="<< static_cast<int>(spec_defs::message_dataspace_spec::version::get(msg)) << " "
	//				" rank=" << static_cast<int>(spec_defs::message_dataspace_spec::rank::get(msg)) << " "
	//				" flags=0b" << make_bitset(spec_defs::message_dataspace_spec::flags::get(msg)) << endl;

		uint8_t version = spec_defs::message_dataspace_spec::version::get(msg);

		rank = spec_defs::message_dataspace_spec::rank::get(msg);

		auto flags = make_bitset(spec_defs::message_dataspace_spec::flags::get(msg));


		switch(version) {
		case 1: {
			// version 1

			auto cur = msg + spec_defs::message_dataspace_spec::size_v1;

			auto shape_ptr = reinterpret_cast<length_type*>(cur);
			shape = vector<uint64_t>{&shape_ptr[0], &shape_ptr[rank]};
			cur += rank*SIZE_OF_LENGTH;

			if (flags.test(0)) {
				auto max_shape_ptr = reinterpret_cast<length_type*>(cur);
				max_shape = vector<uint64_t>{&max_shape_ptr[0], &max_shape_ptr[rank]};
				cur += rank*SIZE_OF_LENGTH;
			} else {
				max_shape = vector<uint64_t>(rank, numeric_limits<uint64_t>::max());
			}

			if (flags.test(1)) {
				auto permutation_ptr = reinterpret_cast<length_type*>(cur);
				permutation = vector<uint64_t>{&permutation_ptr[0], &permutation_ptr[rank]};
				cur += rank*SIZE_OF_LENGTH;
			} else {
				permutation = vector<uint64_t>(rank, numeric_limits<uint64_t>::max());
			}
			break;
		}
		case 2: {
			// version 2

			auto cur = msg + spec_defs::message_dataspace_spec::size_v2;

			auto shape_ptr = reinterpret_cast<length_type*>(cur);
			shape = vector<uint64_t>{&shape_ptr[0], &shape_ptr[rank]};
			cur += rank*SIZE_OF_LENGTH;

			if (flags.test(0)) {
				auto max_shape_ptr = reinterpret_cast<length_type*>(cur);
				max_shape = vector<uint64_t>{&max_shape_ptr[0], &max_shape_ptr[rank]};
				cur += rank*SIZE_OF_LENGTH;
			} else {
				max_shape = vector<uint64_t>(rank, numeric_limits<uint64_t>::max());
			}

			// in version 2 permutation is removed, because it was never implemented.
			permutation = vector<uint64_t>(rank, numeric_limits<uint64_t>::max());
			break;
		}
		default:
			throw EXCEPTION("Unknown dataspace version");
		}

	}

	friend ostream & operator<<(ostream & o,  object_dataspace_t const & dataspace)
	{
		o << "dataspace.rank = " << static_cast<int>(dataspace.rank) << endl;
		o << "dataspace.shape = " << dataspace.shape << endl;
		o << "dataspace.max_shape = " << dataspace.max_shape << endl;
		o << "dataspace.permutation = " << dataspace.permutation << endl;

		return o;
	}

};

struct object_datalayout_t {
	file_handler_t * file;

	uint8_t version;

	enum layout_class_e : uint8_t {
		LAYOUT_COMPACT       = 0u,
		LAYOUT_CONTIGUOUS    = 1u,
		LAYOUT_CHUNKED       = 2u,
		LAYOUT_VIRTUAL       = 3u,
	};

	uint8_t layout_class;

	// COMPACT LAYOUT
	uint8_t           compact_dimensionality;      //< same as dataspace.rank+1
	offset_type       compact_data_address;
	uint32_t          compact_data_size;
	vector<uint32_t>  compact_shape;               //< same as dataspace.shape

	// CONTIGUOUS LAYOUT
	uint8_t           contiguous_dimensionality;   //< same as dataspace.rank+1
	offset_type       contiguous_data_address;
	vector<uint32_t>  contiguous_shape;            //< same as dataspace.shape
	length_type       contiguous_data_size;

	// CHUNKED LAYOUT
	uint8_t           chunk_flags;
	uint8_t           chunk_dimensionality;        //< same as dataspace.rank+1
	uint8_t           chunk_indexing_type;
	vector<uint32_t>  chunk_shape;
	uint32_t          chunk_size_of_element;       //< same as datatype.size_of_elements

	enum chunk_indexing_type_e : uint8_t {
		CHUNK_INDEXING_BTREE_V1          = 0u,
		CHUNK_INDEXING_SINGLE_CHUNK      = 1u,
		CHUNK_INDEXING_IMPLICIT          = 2u,
		CHUNK_INDEXING_FIXED_ARRAY       = 3u,
		CHUNK_INDEXING_EXTENSIBLE_ARRAY  = 4u,
		CHUNK_INDEXING_BTREE_V2          = 5u
	};

	union {
		// Undef
		struct {
			offset_type       data_address;
		} chunk_btree_v1;

		// Single Chunk data
		struct {
			length_type       size_of_filtered_chunk;
			uint32_t          filters;
			offset_type       data_address;
		} chunk_single_chunk;

		struct {
			offset_type       data_address;
		} chunk_implicit;

		// Fixed Array data
		struct {
			uint8_t           page_bits;
			offset_type       data_address;
		} chunk_fixed_array;

		// Extensible Array data
		struct {
			uint8_t           max_bits;
			uint8_t           index_elements;
			uint8_t           min_pointers;
			uint8_t           min_elements;
			uint16_t          page_bits;
			offset_type       data_address;
		} chunk_extensible_array;

		// B-Tree V2 data
		struct {
			uint32_t          node_size;
			uint8_t           split_percent;
			uint8_t           merge_percent;
			offset_type       data_address;
		} chunk_btree_v2;

	};

	// TODO: VIRTUAL LAYOUT


	object_datalayout_t(file_handler_t * file, uint8_t * msg) : file{file}
	{
	//		cout << "parse_datalayout " << std::dec <<
	//				" version=" << static_cast<unsigned>(spec_defs::message_data_layout_v1_spec::version::get(msg)) <<
	//				endl;

		version = spec_defs::message_data_layout_v1_spec::version::get(msg);

		switch(version) {
		case 1:
		case 2: { // version 1 & 2

			layout_class = spec_defs::message_data_layout_v1_spec::layout_class::get(msg);

			auto cur = addr_reader{msg+spec_defs::message_data_layout_v1_spec::size};

			switch(layout_class) {
			case LAYOUT_COMPACT: {
				// in case of compact layout, data are stored within the message.
				compact_dimensionality = spec_defs::message_data_layout_v1_spec::dimensionnality::get(msg);
				auto compact_shape_ptr = reinterpret_cast<uint32_t*>(cur.cur);
				compact_shape = vector<uint32_t>{&compact_shape_ptr[0], &compact_shape_ptr[compact_dimensionality]};
				cur.cur += compact_dimensionality*sizeof(uint32_t);
				compact_data_size = cur.read<uint32_t>();
				compact_data_address = file->to_offset(cur.cur);
				break;
			}
			case LAYOUT_CONTIGUOUS: {
				contiguous_dimensionality = spec_defs::message_data_layout_v1_spec::dimensionnality::get(msg);
				contiguous_data_address = cur.read<offset_type>();
				auto contiguous_shape_ptr = reinterpret_cast<uint32_t*>(cur.cur);
				contiguous_shape = vector<uint32_t>{&contiguous_shape_ptr[0], &contiguous_shape_ptr[contiguous_dimensionality]};
				cur.cur += contiguous_dimensionality*sizeof(uint32_t);
				contiguous_data_size = undef_length;
				break;
			}
			case LAYOUT_CHUNKED: {
				chunk_indexing_type = CHUNK_INDEXING_BTREE_V1;
				chunk_dimensionality = spec_defs::message_data_layout_v1_spec::dimensionnality::get(msg);
				chunk_btree_v1.data_address = cur.read<offset_type>();
				auto chunk_shape_ptr = reinterpret_cast<uint32_t*>(cur.cur);
				chunk_shape = vector<uint32_t>{&chunk_shape_ptr[0], &chunk_shape_ptr[chunk_dimensionality]};
				cur.cur += chunk_dimensionality*sizeof(uint32_t);
				chunk_size_of_element = cur.read<uint32_t>();
				break;
			}
			default:
				// This should never append in well formed file.
				throw EXCEPTION("Unexpected layout_class");
			}

			break;
		}

		case 3:	{
			layout_class = spec_defs::message_data_layout_v3_spec::layout_class::get(msg);

			auto cur = addr_reader{msg+spec_defs::message_data_layout_v3_spec::size};

			switch(layout_class) {
			case LAYOUT_COMPACT:
				compact_data_size = cur.read<uint16_t>();
				compact_data_address = file->to_offset(cur.cur);
				break;
			case LAYOUT_CONTIGUOUS:
				contiguous_data_address = cur.read<offset_type>();
				contiguous_data_size = cur.read<length_type>();
				break;
			case LAYOUT_CHUNKED: {
				chunk_indexing_type = CHUNK_INDEXING_BTREE_V1;
				chunk_dimensionality = cur.read<uint8_t>();
				chunk_btree_v1.data_address = cur.read<offset_type>();
				auto chunk_shape_ptr = reinterpret_cast<uint32_t*>(cur.cur);
				chunk_shape = vector<uint32_t>{&chunk_shape_ptr[0], &chunk_shape_ptr[chunk_dimensionality]};
				cur.cur += chunk_dimensionality*sizeof(uint32_t);
				chunk_size_of_element = cur.read<uint32_t>();
				break;
			}
			default:
				// This should never append in well formed file.
				throw EXCEPTION("Unexpected layout_class");
			}

			break;
		}

		case 4: {
			layout_class = spec_defs::message_data_layout_v4_spec::layout_class::get(msg);

			auto cur = addr_reader{msg+spec_defs::message_data_layout_v4_spec::size};

			switch(layout_class) {
			case LAYOUT_COMPACT: // same as v3
				compact_data_size = cur.read<uint16_t>();
				compact_data_address = file->to_offset(cur.cur);
				break;
			case LAYOUT_CONTIGUOUS: // same as v3
				contiguous_data_address = cur.read<offset_type>();
				contiguous_data_size = cur.read<length_type>();
				break;
			case LAYOUT_CHUNKED: { // /!\ not the same as v3

				chunk_flags = cur.read<uint8_t>();
				chunk_dimensionality = cur.read<uint8_t>();

				uint8_t dimensionality_encoded_size = cur.read<uint8_t>();
				chunk_shape.resize(chunk_dimensionality);

				for (unsigned i = 0; i < chunk_dimensionality; ++i)
					chunk_shape[i] = cur.read_int(dimensionality_encoded_size);

				chunk_indexing_type = cur.read<uint8_t>();

				uint64_t size_of_index_data = 0;
				switch(chunk_indexing_type) {
				case CHUNK_INDEXING_SINGLE_CHUNK:
					chunk_single_chunk.size_of_filtered_chunk = cur.read<length_type>();
					chunk_single_chunk.filters = cur.read<uint32_t>();
					chunk_single_chunk.data_address = cur.read<offset_type>();
					break;
				case CHUNK_INDEXING_IMPLICIT:
					chunk_implicit.data_address = cur.read<offset_type>();
					break;
				case CHUNK_INDEXING_FIXED_ARRAY:
					chunk_fixed_array.page_bits = cur.read<uint8_t>();
					chunk_fixed_array.data_address = cur.read<offset_type>();
					break;
				case CHUNK_INDEXING_EXTENSIBLE_ARRAY:
					chunk_extensible_array.max_bits = cur.read<uint8_t>();
					chunk_extensible_array.index_elements = cur.read<uint8_t>();
					chunk_extensible_array.min_pointers = cur.read<uint8_t>();
					chunk_extensible_array.min_elements = cur.read<uint8_t>();
					chunk_extensible_array.page_bits = cur.read<uint16_t>();
					chunk_extensible_array.data_address = cur.read<offset_type>();
					break;
				case CHUNK_INDEXING_BTREE_V2:
					chunk_btree_v2.node_size = cur.read<uint32_t>();
					chunk_btree_v2.split_percent = cur.read<uint8_t>();
					chunk_btree_v2.merge_percent = cur.read<uint8_t>();
					chunk_btree_v2.data_address = cur.read<offset_type>();
					break;
				}

				break;
			}
			case LAYOUT_VIRTUAL:
				throw EXCEPTION("Unimplemented virtual dataset");
			default:
				throw EXCEPTION("Unexpected layout_class");
		}

		}
		default:
			throw EXCEPTION("Unexpected layout_class");
	}

	}


	// A is the key entry in btree_v1
	// B is the key that we looking for
	inline int chunk_btree_v1_key_cmp(uint8_t const * a, uint8_t const * b)
	{
		// for the key we have to skip the 8 first bits that contain
		return std::memcmp(a+spec_defs::b_tree_v1_chunk_key_spec::size, b, chunk_dimensionality*sizeof(uint64_t));
	}

	static int chunk_btree_v1_find_index(btree_v1 const & cur, uint64_t key_length, uint64_t const * key)
	{
		for (int i = cur.get_entries_count()-1; i >= 0; --i) {
			if (chunk_btree_v1_key_cmp(cur.get_key(key_length, i), key) <= 0) {
				return i;
			}
		}
		return -1;
	}

	chunk_desc_t chunk_btree_v1_find(uint64_t const * key) const
	{
		uint64_t key_length = spec_defs::b_tree_v1_chunk_key_spec::size+chunk_dimensionality*sizeof(uint64_t);

		// Chunk aren't allocated
		if (chunk_btree_v1.data_address == undef_offset)
			return chunk_desc_t{0u,0u,undef_offset};

		btree_v1 cur{file->to_address(chunk_btree_v1.data_address)};

		// Are we bellow the first chunk ?
		if (chunk_btree_v1_key_cmp(cur.get_key(key_length, 0), key) > 0) {
			return chunk_desc_t{0u,0u,undef_offset};
		}

		while(cur.get_depth() != 0) {
			size_t i = chunk_btree_v1_find_index(cur, key);
			cur = btree_v1{file->to_address(cur.get_node(key_length, i))};
		}

		int i = chunk_btree_v1_find_index(cur, key);
		if (i < 0) {
			return chunk_desc_t{0u,0u,undef_offset};
		}

		return chunk_desc_t{
			spec_defs::b_tree_v1_chunk_key_spec::chunk_size::get(cur.get_key(key_length, i)),
			spec_defs::b_tree_v1_chunk_key_spec::filter_mask::get(cur.get_key(key_length, i)),
			static_cast<uint64_t>(cur.get_node(key_length, i))
		};

	}

	void chunk_btree_v1_ls(vector<chunk_desc_t> & ret) const
	{
		uint64_t const key_length = spec_defs::b_tree_v1_chunk_key_spec::size+chunk_dimensionality*sizeof(uint64_t);

		stack<btree_v1> stack;

		// Chunk aren't allocated
		if (chunk_btree_v1.data_address == undef_offset)
			return;

		stack.push(btree_v1{file->to_address(chunk_btree_v1.data_address)});
		while(not stack.empty()) {
			btree_v1 cur = stack.top();
			stack.pop();

			if (cur.get_depth() == 0) {
				//assert(cur.get_entries_count() <= lK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					ret.emplace_back(
							spec_defs::b_tree_v1_chunk_key_spec::chunk_size::get(cur.get_key(key_length, i)),
							spec_defs::b_tree_v1_chunk_key_spec::filter_mask::get(cur.get_key(key_length, i)),
							static_cast<uint64_t>(cur.get_node(key_length, i)));
				}
			} else {
				//assert(cur.get_entries_count() <= nK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					stack.push(btree_v1{file->to_address(cur.get_node(key_length, i))});
				}
			}
		}
	}

	vector<chunk_desc_t> chunk_btree_v1_ls() const
	{
		vector<chunk_desc_t> ret;
		chunk_btree_v1_ls(ret);
		return ret;
	}

	friend ostream & operator<<(ostream & o, object_datalayout_t const & datalayout)
	{
		o << "datalayout.version = " << static_cast<int>(datalayout.version) << endl;

		switch(datalayout.layout_class) {
		case object_datalayout_t::LAYOUT_COMPACT: {
			o << "datalayout.layout_class = COMPACT" << endl;
			o << "datalayout.compact_dimensionality = " << static_cast<int>(datalayout.compact_dimensionality) << endl;
			o << "datalayout.compact_data_address = " << datalayout.compact_data_address << endl;
			o << "datalayout.compact_data_size = " << datalayout.compact_data_size << endl;
			o << "datalayout.compact_shape = " << datalayout.compact_shape << endl;
			 break;
		}
		case object_datalayout_t::LAYOUT_CONTIGUOUS: {
			o << "datalayout.layout_class = CONTIGUOUS" << endl;
			o << "datalayout.contiguous_dimensionality = " << static_cast<int>(datalayout.contiguous_dimensionality) << endl;
			o << "datalayout.contiguous_data_address = " << datalayout.contiguous_data_address << endl;
			o << "datalayout.contiguous_shape = " << datalayout.contiguous_shape << endl;
			o << "datalayout.contiguous_data_size = " << datalayout.contiguous_data_size << endl;
			break;
		}
		case object_datalayout_t::LAYOUT_CHUNKED: {
			o << "datalayout.layout_class = CHUNKED" << endl;
			o << "datalayout.chunk_flags = " << static_cast<int>(datalayout.chunk_flags) << endl;
			o << "datalayout.chunk_dimensionality = " << static_cast<int>(datalayout.chunk_dimensionality) << endl;
			o << "datalayout.chunk_shape = " << datalayout.chunk_shape << endl;
			o << "datalayout.chunk_size_of_element = " << datalayout.chunk_size_of_element << endl;

			switch (datalayout.chunk_indexing_type) {
				case CHUNK_INDEXING_BTREE_V1:
					o << "datalayout.chunk_indexing_type = BTREE_V1" << endl;
					o << "datalayout.chunk_btree_v1.data_address = " << datalayout.chunk_btree_v1.data_address << endl;
					break;
				case CHUNK_INDEXING_SINGLE_CHUNK:
					o << "datalayout.chunk_indexing_type = SINGLE_CHUNK" << endl;
					o << "datalayout.chunk_single_chunk.size_of_filtered_chunk = " << datalayout.chunk_single_chunk.size_of_filtered_chunk << endl;
					o << "datalayout.chunk_single_chunk.filters = " << datalayout.chunk_single_chunk.filters << endl;
					o << "datalayout.chunk_single_chunk.data_address = " << datalayout.chunk_single_chunk.data_address << endl;
					break;
				case CHUNK_INDEXING_IMPLICIT:
					o << "datalayout.chunk_indexing_type = IMPLICIT" << endl;
					o << "datalayout.chunk_implicit.data_address = " << datalayout.chunk_implicit.data_address << endl;
					break;
				case CHUNK_INDEXING_FIXED_ARRAY:
					o << "datalayout.chunk_indexing_type = FIXED_ARRAY" << endl;
					o << "datalayout.chunk_fixed_array.page_bits = " << datalayout.chunk_fixed_array.page_bits << endl;
					o << "datalayout.chunk_fixed_array.data_address = " << datalayout.chunk_fixed_array.data_address << endl;
					break;
				case CHUNK_INDEXING_EXTENSIBLE_ARRAY:
					o << "datalayout.chunk_indexing_type = EXTENSIBLE_ARRAY" << endl;
					o << "datalayout.chunk_extensible_array.max_bits = " << datalayout.chunk_extensible_array.max_bits << endl;
					o << "datalayout.chunk_extensible_array.index_elements = " << datalayout.chunk_extensible_array.index_elements << endl;
					o << "datalayout.chunk_extensible_array.min_elements = " << datalayout.chunk_extensible_array.min_elements << endl;
					o << "datalayout.chunk_extensible_array.min_pointers = " << datalayout.chunk_extensible_array.min_pointers << endl;
					o << "datalayout.chunk_extensible_array.page_bits = " << datalayout.chunk_extensible_array.page_bits << endl;
					break;
				case CHUNK_INDEXING_BTREE_V2:
					o << "datalayout.chunk_indexing_type = BTREE_V2" << endl;
					o << "datalayout.chunk_btree_v2.node_size = " << datalayout.chunk_btree_v2.node_size << endl;
					o << "datalayout.chunk_btree_v2.split_percent = " << datalayout.chunk_btree_v2.split_percent << endl;
					o << "datalayout.chunk_btree_v2.merge_percent = " << datalayout.chunk_btree_v2.merge_percent << endl;
					o << "datalayout.chunk_btree_v2.data_address = " << datalayout.chunk_btree_v2.data_address << endl;
					break;
				default:
					o << "datalayout.chunk_indexing_type = UNSUPPORTED" << endl;
					break;
			}

			break;
		}
		case object_datalayout_t::LAYOUT_VIRTUAL: {
			o << "datalayout.layout_class = VIRTUAL" << endl;
			break;
		}
		default:
			o << "datalayout.layout_class = UNSUPPORTED" << endl;
			break;
		}

		return o;
	}

};

struct object_datatype_t {
	uint32_t size_of_elements;

	object_datatype_t(uint8_t * msg) {
	//		cout << "parse_datatype " << std::dec <<
	//				" version="<< static_cast<int>((spec_defs::message_datatype_spec::class_and_version::get(msg)&0xf0) >> 4) << " "
	//				" size=" << static_cast<unsigned>(spec_defs::message_datatype_spec::size_of_elements::get(msg))
	//				<< endl;

		size_of_elements = spec_defs::message_datatype_spec::size_of_elements::get(msg);
	}

	friend ostream & operator<<(ostream & o, object_datatype_t const & datatype)
	{
		o << "datatype.size_of_elements = " << datatype.size_of_elements << endl;
		return o;
	}
};


struct object_link_info_t {
	bitset<8> flags;
	uint64_t maximum_creation_index;
	uint64_t fractal_head_address;
	uint64_t name_index_b_tree_address;
	uint64_t creation_order_index_address;

	object_link_info_t(uint8_t * msg)
	{
		uint8_t version = spec_defs::message_link_info_spec::version::get(msg);
		if (version != 0) {
			throw EXCEPTION("Unknown Link Info version (%d)", version);
		}

		flags = spec_defs::message_link_info_spec::flags::get(msg);

		uint64_t offset = spec_defs::message_link_info_spec::size;

		auto cur = addr_reader{msg+spec_defs::message_link_info_spec::size};

		if (flags.test(0)) {
			maximum_creation_index = cur.read<uint64_t>();
		} else {
			maximum_creation_index = 0;
		}

		fractal_head_address = cur.read<offset_type>();
		name_index_b_tree_address = cur.read<offset_type>();

		if (flags.test(1)) {
			creation_order_index_address = cur.read<offset_type>();
		} else {
			creation_order_index_address = undef_offset;
		}

	}

	friend ostream & operator<<(ostream & o, object_link_info_t const & link_info)
	{
		o << "link_info.flags = " << link_info.flags << endl;
		o << "link_info.maximum_creation_index = " << link_info.maximum_creation_index << endl;
		o << "link_info.fractal_head_address = " << link_info.fractal_head_address << endl;
		o << "link_info.name_index_b_tree_address = " << link_info.name_index_b_tree_address << endl;
		o << "link_info.creation_order_index_address = " << link_info.creation_order_index_address << endl;
		return o;
	}

};

struct object_data_storage_filter_pipeline_t {

	struct filter {
		uint16_t id;
		string name;
		bitset<16> flags;
		vector<uint32_t> params;

		filter(uint16_t id, string name, bitset<16> flags, vector<uint32_t> const & params) :
			id{id},
			name{name},
			flags{flags},
			params{params}
		{

		}

		friend ostream & operator<< (ostream & o, filter const & f)
		{
			return o << "<filter id=" << f.id <<", name=" << f.name << ", flags=" << f.flags << ", client data=" << f.params << ">";
		}

	};

	vector<filter> filters;

	object_data_storage_filter_pipeline_t(uint8_t * msg)
	{

		auto version = spec_defs::message_data_storage_filter_pipeline_v1::version::get(msg);
		switch (version) {
		case 1: {
			auto nfilter = spec_defs::message_data_storage_filter_pipeline_v1::munber_of_filters::get(msg);
			auto cur = addr_reader{msg+spec_defs::message_data_storage_filter_pipeline_v1::size};

			for (int i = 0; i < nfilter; ++i) {
				auto filter_identifier = cur.read<uint16_t>();
				auto name_length = cur.read<uint16_t>(); // Heigth byte padded.
				auto flags = cur.read<uint16_t>();
				auto number_client_data_value = cur.read<uint16_t>();
				auto name = cur.read_string(name_length);
				auto client_data = cur.read_array<uint32_t>(number_client_data_value);

				// skip padding if necessary
				if (number_client_data_value%2) {
					cur.cur += 4;
				}

				filters.emplace_back(filter_identifier, name, flags, client_data);
//				cout << filters.back() << endl;

			}

			break;
		}
		case 2: {
			auto nfilter = spec_defs::message_data_storage_filter_pipeline_v2::munber_of_filters::get(msg);
			auto cur = addr_reader{msg+spec_defs::message_data_storage_filter_pipeline_v2::size};

			for (int i = 0; i < nfilter; ++i) {
				auto filter_identifier = cur.read<uint16_t>();
				auto name_length = cur.read<uint16_t>();
				auto flags = cur.read<uint16_t>();
				auto number_client_data_value = cur.read<uint16_t>();
				auto name = cur.read_string(name_length);
				auto client_data = cur.read_array<uint32_t>(number_client_data_value);

				filters.emplace_back(filter_identifier, name, flags, client_data);
//				cout << filters.back() << endl;

			}
			break;
		}
		default:
			throw EXCEPTION("Unsupported data_storage_filter_pipeline message (%d)", version);
		}

	}


	friend ostream & operator<< (ostream & o, object_data_storage_filter_pipeline_t const & f)
	{
		for (int i = 0; i < f.filters.size(); ++i) {
			o << "data_storage_filter_pipeline.filter["<<i<<"] = " << f.filters[i] << endl;
		}
		return o;
	}

};

struct object_fill_value_t {
	vector<uint8_t> value;

	object_fill_value_t(uint8_t * msg)
	{
		auto cur = addr_reader{msg+spec_defs::message_fillvalue_old_spec::size};
		value = cur.read_array<uint8_t>(spec_defs::message_fillvalue_old_spec::size_of_fillvalue::get(msg));
	}

	friend ostream & operator<< (ostream & o, object_fill_value_t const & f)
	{
		return o << "fillvalue_old.value = " << vector_to_hex_string(f.value) << endl;
	}
};

struct object_data_storage_fill_value_t {
	bitset<8> flags;
	vector<uint8_t> value;

	object_data_storage_fill_value_t(uint8_t * msg)
	{
		auto cur = addr_reader{msg};
		auto version = cur.read<uint8_t>();

		switch (version) {
		case 1: {
			auto space_allocation_time = cur.read<uint8_t>();
			flags |= (space_allocation_time&0b0000'0011u);

			auto fill_value_write_time = cur.read<uint8_t>();
			flags |= (fill_value_write_time&0b0000'0011u)<<2u;

			auto fill_value_defined = cur.read<uint8_t>();
			flags |= (fill_value_write_time&0b0000'0001u)<<5u;

			if (not flags.test(5))
				flags.set(4);

			auto fillvalue_size = cur.read<uint32_t>();
			value = cur.read_array<uint8_t>(fillvalue_size);
			break;
		}
		case 2: {
			auto space_allocation_time = cur.read<uint8_t>();
			flags |= (space_allocation_time&0b0000'0011u);

			auto fill_value_write_time = cur.read<uint8_t>();
			flags |= (fill_value_write_time&0b0000'0011u)<<2u;

			auto fill_value_defined = cur.read<uint8_t>();
			flags |= (fill_value_write_time&0b0000'0001u)<<5u;

			if (not flags.test(5))
				flags.set(4);

			if (flags.test(5)) {
				auto fillvalue_size = cur.read<uint32_t>();
				value = cur.read_array<uint8_t>(fillvalue_size);
			}

			break;
		}
		case 3: {
			flags = cur.read<uint8_t>();

			if (flags.test(5)) {
				auto fillvalue_size = cur.read<uint32_t>();
				value = cur.read_array<uint8_t>(fillvalue_size);
			}
			break;
		}
		default:
			throw EXCEPTION("Unsuported data_storage_fillvalue version (%d)", version);
		}


	}

	bool has_fillvalue() const
	{
		return flags.test(5);
	}

	friend ostream & operator<< (ostream & o, object_data_storage_fill_value_t const & f)
	{
		o << "data_storage_fillvalue.flags = 0b" << f.flags << endl;
		if (f.has_fillvalue()) {
			return o << "data_storage_fillvalue.value = " << vector_to_hex_string(f.value) << endl;
		} else {
			return o << "data_storage_fillvalue.value = undef" << endl;
		}
	}

};


struct object_base : public object, public object_interface
{
	using object::file;
	using object::memory_addr;

	struct {
		uint8_t * value;
	} fillvalue;

	void parse_fillvalue_old(uint8_t * msg) {
//		cout << "parse_datatype " << std::dec <<
//				" size=" << static_cast<unsigned>(spec_defs::message_fillvalue_old_spec::size_of_fillvalue::get(msg))
//				<< endl;
		// TODO
		fillvalue.value = msg + spec_defs::message_fillvalue_old_spec::size;
	}

	void parse_fillvalue(uint8_t * msg) {
//		cout << "parse_datatype " << std::dec <<
//				" size=" << static_cast<unsigned>(spec_defs::message_fillvalue_spec::version::get(msg)) <<
//				" space_allocation_time=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::space_allocation_time::get(msg)) << std::dec <<
//				" fillvalue_write_time=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::fillvalue_write_time::get(msg)) << std::dec <<
//				" fillvalue_defined=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::fillvalue_defined::get(msg)) << std::dec <<
//				endl;
		// TODO
		if (spec_defs::message_fillvalue_spec::version::get(msg) == 1 or spec_defs::message_fillvalue_spec::fillvalue_defined::get(msg) != 0) {
			fillvalue.value = msg + spec_defs::message_fillvalue_spec::size + sizeof(uint32_t);
		}
	}


	// Parse shared message to get the actual message offset
	static uint64_t parse_shared(uint8_t * msg)
	{
		uint8_t version = spec_defs::message_shared_vX_spec::version::get(msg);

		uint8_t xtype = spec_defs::message_shared_vX_spec::type::get(msg); // this parameters seems not used.

		// The actual address of the message.
		uint64_t offset = numeric_limits<uint64_t>::max();
//		cout << std::dec << "<shared message version="<<static_cast<unsigned>(version)
//			 <<" , type=" << static_cast<unsigned>(xtype) << ">" << endl;
		switch (version) {
		case 1: {
			offset = spec_defs::message_shared_v1_spec::address::get(msg);
			break;
		}
		case 2: {
			offset = spec_defs::message_shared_v2_spec::address::get(msg);
			break;
		}
		default:
			throw EXCEPTION("Unsupported shared message version (%d)", version);
		}

		return offset;

	}

	uint32_t _modification_time;

	void parse_object_modifcation_time(uint8_t * msg) {
//		cout << "parse_object_modifcation_time " << std::dec <<
//				" version=" << static_cast<unsigned>(spec_defs::message_object_modification_time_spec::version::get(msg)) <<
//				" time=" << static_cast<unsigned>(spec_defs::message_object_modification_time_spec::time::get(msg)) <<
//				endl;
		uint8_t version = spec_defs::message_object_modification_time_spec::version::get(msg);
		if (version != 1)
			throw EXCEPTION("unknown modification time version (%d)", version);
		_modification_time = spec_defs::message_object_modification_time_spec::time::get(msg);
	}

	char const * _comment;

	void parse_comment(uint8_t * msg) {
		_comment = reinterpret_cast<char *>(msg);
	}


	pair<string, uint64_t> parse_link(uint8_t * msg) const
	{
		uint8_t version = spec_defs::message_link_spec::version::get(msg);
		if (version != 1) {
			throw EXCEPTION("Unknown Link Info version");
		}

		auto flags = make_bitset(spec_defs::message_link_info_spec::flags::get(msg));

		auto cur = addr_reader{msg+spec_defs::message_link_info_spec::size};

		uint8_t type = 0u;
		if (flags.test(3)) {
			type = cur.read<uint8_t>();
		}

		uint64_t creation_order = 0xffff'ffff'ffff'ffffu;
		if (flags.test(2)) {
			creation_order = cur.read<uint64_t>();
		}

		// default to zero.
		uint8_t charset = 0x00u;
		if (flags.test(4)) {
//			cout << "has charset" << endl;
			charset = cur.read<uint8_t>();
		}

		uint64_t size_of_length_of_name = 1u<<(flags.to_ulong()&0x0000'0011u);
		uint64_t length_of_name = cur.read_int(size_of_length_of_name);

		string link_name = cur.read_string(length_of_name);

		// TODO: Link information.
//		cout << "parse_link" << endl;
//		cout << "flags = " << flags << endl;
//		cout << "type = " << static_cast<int>(type) << endl;
//		cout << "creation_order = " << creation_order << endl;
//		cout << "charset = " << (charset?"UTF-8":"ASCII") << endl;
//		cout << "size_of_length_of_name = " << size_of_length_of_name << endl;
//		cout << "length_of_name = " << length_of_name << endl;
//		cout << "name = " << link_name << endl;

		uint64_t object_addr = undef_offset;

		switch (type) {
		case 0: { // Hard link
			object_addr = cur.read<uint64_t>();
//			cout << "hardlink address = " << object_addr << endl;
			break;
		}
		case 1: { // soft link
			auto size = cur.read<uint16_t>();
			string soft_link_value = cur.read_string(size);
			cout << "soft link = " << soft_link_value << endl;
			EXCEPTION("Soft link aren't implemented yet");
			break;
		}
		case 64: { // external link
			EXCEPTION("External link aren't implemented yet");
			break;
		}
		default:
			throw EXCEPTION("Unhandled link type");
		}

		return {link_name, object_addr};

	}


	void parse_group_info(uint8_t * msg)
	{
		uint8_t version = spec_defs::message_link_spec::version::get(msg);
		if (version != 0) {
			throw EXCEPTION("Unknown Group Info version");
		}

		auto flags = make_bitset(spec_defs::message_link_info_spec::flags::get(msg));

		uint64_t offset = spec_defs::message_link_info_spec::size;

		uint16_t maximum_compact_value = 0xffffu;
		uint16_t minimum_dense_value = 0xffffu;
		if (flags.test(0)) {
			maximum_compact_value = read_at<uint16_t>(&msg[offset]);
			offset += 2;
			minimum_dense_value = read_at<uint16_t>(&msg[offset]);
			offset += 2;
		}

		uint16_t estimated_number_of_entry = 0xffffu;
		uint16_t estimated_link_name_length_entry = 0xffffu;
		if (flags.test(1)) {
			estimated_number_of_entry = read_at<uint16_t>(&msg[offset]);
			offset += 2;
			estimated_link_name_length_entry = read_at<uint16_t>(&msg[offset]);
			offset += 2;
		}

//		cout << "parse_global_info" << endl;
//		cout << "version = " << static_cast<int>(version) << endl;
//		cout << "flags = " << flags << endl;
//		cout << "maximum_compact_value = " << maximum_compact_value << endl;
//		cout << "minimum_dense_value = " << minimum_dense_value << endl;
//		cout << "estimated_number_of_entry = " << estimated_number_of_entry << endl;
//		cout << "estimated_link_name_length_entry = " << estimated_link_name_length_entry << endl;

	}

	object_base(file_handler_t * file, uint8_t * addr) : object{file, addr}
	{
		_modification_time = 0;
		_comment = nullptr;
	}

	void dispatch_message(uint16_t type, uint8_t * data)
	{
		switch(type) {
		case MSG_NIL:
			break;
		case MSG_DATASPACE:
			break;
		case MSG_LINK_INFO:
			break;
		case MSG_DATATYPE:
			break;
		case MSG_FILL_VALUE:
			parse_fillvalue_old(data);
			break;
		case MSG_DATA_STORAGE_FILL_VALUE:
			parse_fillvalue(data);
			break;
		case MSG_LINK:
			parse_link(data);
			break;
		case MSG_DATA_STORAGE:
			break;
		case MSG_DATA_LAYOUT:
			break;
		case MSG_BOGUS:
			break;
		case MSG_GROUP_INFO:
			parse_group_info(data);
			break;
		case MSG_DATA_STORAGE_FILTER_PIPELINE: {
			auto filters = object_data_storage_filter_pipeline_t{data};
			break;
		}
		case MSG_ATTRIBUTE:
			// ignore attribute message
			break;
		case MSG_OBJECT_COMMENT:
			parse_comment(data);
			break;
		case MSG_OBJECT_MODIFICATION_TIME_OLD:
			break;
		case MSG_SHARED_MESSAGE_TABLE:
			break;
		case MSG_OBJECT_HEADER_CONTINUATION:
			break;
		case MSG_SYMBOL_TABLE:
			break;
		case MSG_OBJECT_MODIFICATION_TIME:
			parse_object_modifcation_time(data);
			break;
		case MSG_BTREE_K_VALUE:
			break;
		case MSG_DRIVER_INFO:
			break;
		case MSG_ATTRIBUTE_INFO:
			break;
		default:
//			cout << "found message <@" << static_cast<void*>(data)
//					<< " type = 0x" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(type) << std::hex
//					<< " size=" << header.size
//					<< " flags=" << std::hex << static_cast<int>(header.flags) << ">" << endl;
			break;//		cout << "parse_global_info" << endl;
			//		cout << "version = " << static_cast<int>(version) << endl;
			//		cout << "flags = " << flags << endl;
			//		cout << "maximum_compact_value = " << maximum_compact_value << endl;
			//		cout << "minimum_dense_value = " << minimum_dense_value << endl;
			//		cout << "estimated_number_of_entry = " << estimated_number_of_entry << endl;
			//		cout << "estimated_link_name_length_entry = " << estimated_link_name_length_entry << endl;
		}
	}

};


struct object_v1_trait : public object_base {
	using spec = typename spec_defs::object_header_v1_spec;

	using object::file;
	using object::memory_addr;

	using object_base::parse_shared;
	using object_base::dispatch_message;

	object_v1_trait(file_handler_t * file, uint8_t * memory_addr) : object_base{file, memory_addr}
	{
		cout << "Creating object v1" << endl;
	}

	struct message_iterator_t {
		using message_spec = typename spec_defs::message_header_v1_spec;

		// TODO: check if message go out of block boundary.
		file_handler_t * file;

		struct block {
			uint8_t * bgn; uint8_t * end;
			block (uint8_t * bgn, uint64_t length) : bgn{bgn}, end{&bgn[length]} { }
		};

		list<block> message_block_queue;

		uint8_t * _cur;
		uint8_t * _end;

		message_iterator_t(file_handler_t * file, uint8_t * bgn, uint64_t length) :
			file{file}
		{
			_safe_append_block(bgn, length);
			if (not message_block_queue.empty()) {
				auto & block = message_block_queue.front();
				_cur = block.bgn;
				_end = block.end;
			}
		}

		object_message_handler_t operator*() const
		{
			object_message_handler_t ret = {
				.type  = message_spec::type::get(_cur),
				.size  = message_spec::size_of_message::get(_cur),
				.flags = message_spec::flags::get(_cur),
				.data  = _cur + message_spec::size
			};

			if (ret.flags.test(1)) { // if the message is shared
				ret.data = file->to_address(parse_shared(ret.data));
			}

			return ret;

		}

		void _safe_append_block(uint8_t * bgn, uint64_t length)
		{
			if (length > 0) {
				message_block_queue.emplace_back(bgn, length);
			}
		}

		message_iterator_t & operator++() {

			// Check current message,
			auto msg = **this;

			if (msg.type == MSG_OBJECT_HEADER_CONTINUATION) {
				if (spec_defs::message_object_header_continuation_spec::length::get(msg.data) > 0) {
					message_block_queue.emplace_back(
							file->to_address(spec_defs::message_object_header_continuation_spec::offset::get(msg.data)),
							spec_defs::message_object_header_continuation_spec::length::get(msg.data)
					);
				}
			}

			_cur += message_spec::size
				 +  message_spec::size_of_message::get(_cur);

			if ( _cur >= _end) {
				message_block_queue.pop_front();
				if (message_block_queue.empty()) // no more message to read.
					return *this;
				auto & block = message_block_queue.front();
				_cur = block.bgn;
				_end = block.end;
			}

			return *this;
		}

		bool end() const {
			return message_block_queue.empty();
		}

	};


	message_iterator_t get_message_iterator() const
	{
		uint64_t offset = file->to_offset(&memory_addr[spec::size]);
		uint64_t align_offset =  align_forward<8>(offset);

		// length - 4 because of checksum.
		uint64_t length = spec::header_size::get(memory_addr);

		return message_iterator_t{file, file->to_address(align_offset), length-(align_offset-offset)};

	}

	void parse_messages()
	{
		uint64_t msg_count = spec::total_number_of_header_message::get(memory_addr);
		cout << "parsing "<< msg_count <<" messages" << endl;
		cout << "object header size = " << spec::header_size::get(memory_addr) << endl;

		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;
			dispatch_message(msg.type, msg.data);
			--msg_count;
		}

		if (msg_count != 0)
			throw EXCEPTION("Message count is invalid (%d)", msg_count);
	}

};


struct object_v2_trait : public object_base {
	using spec = typename spec_defs::object_header_v2_spec;

	using object::file;
	using object::memory_addr;

	using object_base::parse_shared;
	using object_base::dispatch_message;

	object_v2_trait(file_handler_t * file, uint8_t * memory_addr) : object_base{file, memory_addr}
	{
		cout << "Creating object v2" << endl;
	}

	uint64_t get_message_header_size() const {
		return spec_defs::message_header_v2_spec::size + (is_attribute_creation_order_tracked()?2:0);
	}

	uint64_t get_object_header_size() const {
		return spec::size
				+ (has_time_fields()?16:0)
				+ (is_non_default_attribute_storage_phase_change_stored()?4:0)
				+ get_size_of_size_of_chunk();
	}

	struct message_iterator_t {
		using message_spec = typename spec_defs::message_header_v2_spec;

		// TODO: check if message go out of block boundary.

		file_handler_t * file;
		uint64_t header_size;

		struct block {
			uint8_t * bgn; uint8_t * end;
			block (uint8_t * bgn, uint64_t length) : bgn{bgn}, end{&bgn[length]} { }
		};

		list<block> message_block_queue;

		uint8_t * _cur;
		uint8_t * _end;

		message_iterator_t(file_handler_t * file, uint64_t header_size, uint8_t * bgn, uint64_t length) :
			file{file},
			header_size{header_size}
		{
			_safe_append_block(bgn, length);
			if (not message_block_queue.empty()) {
				auto & block = message_block_queue.front();
				_cur = block.bgn;
				_end = block.end;
			}
		}

		object_message_handler_t operator*() const {
			object_message_handler_t ret = {
				.type  = message_spec::type::get(_cur),
				.size  = message_spec::size_of_message::get(_cur),
				.flags = message_spec::flags::get(_cur),
				.data  = _cur + header_size
			};

			if (ret.flags.test(1)) { // if the message is shared
				ret.data = file->to_address(parse_shared(ret.data));
			}

			return ret;

		}

		void _safe_append_block(uint8_t * bgn, uint64_t length)
		{
			if (length > 0) {
				message_block_queue.emplace_back(bgn, length);
			}
		}


		message_iterator_t & operator++() {

			auto msg = **this;

			if (msg.type == MSG_OBJECT_HEADER_CONTINUATION) {
				if (spec_defs::message_object_header_continuation_spec::length::get(msg.data) > 0) {
					message_block_queue.emplace_back(
							file->to_address(spec_defs::message_object_header_continuation_spec::offset::get(msg.data)),
							spec_defs::message_object_header_continuation_spec::length::get(msg.data)
					);
				}
			}

			_cur += header_size
				 +  message_spec::size_of_message::get(_cur);

			if ( _cur >= _end) {
				message_block_queue.pop_front();
				if (message_block_queue.empty()) // no more message to read.
					return *this;
				auto & block = message_block_queue.front();
				_cur = block.bgn;
				_end = block.end;
			}

			return *this;
		}

		bool end() const {
			return message_block_queue.empty();
		}

	};

	message_iterator_t get_message_iterator() const
	{
		uint64_t offset = get_object_header_size();
		// length - 4 because of checksum.
		uint64_t length = get_reader_for(get_size_of_size_of_chunk())(&memory_addr[offset-get_size_of_size_of_chunk()]) - 4;

		return message_iterator_t{file, spec_defs::message_header_v2_spec::size + (is_attribute_creation_order_tracked()?2:0), &memory_addr[offset], length};

	}

	void parse_messages()
	{
		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;
			dispatch_message(msg.type, msg.data);
		}
	}

	uint8_t get_size_of_size_of_chunk() const {
		return 1<<(0b0000'0011u&spec::flags::get(memory_addr));
	}

	bool is_attribute_creation_order_tracked() const {
		return 0b0000'0100u & spec::flags::get(memory_addr);
	}

	bool is_attribute_creation_order_indexed() const {
		return 0b0000'1000u & spec::flags::get(memory_addr);
	}

	bool is_non_default_attribute_storage_phase_change_stored() const {
		return 0b0001'0000u & spec::flags::get(memory_addr);
	}

	bool has_time_fields() const {
		return 0b0010'0000u & spec::flags::get(memory_addr);
	}

	uint32_t access_time() {
		assert(has_time_fields());
		return read_at<uint32_t>(memory_addr + spec::size + 0);
	}

	uint32_t modification_time() {
		assert(has_time_fields());
		return read_at<uint32_t>(memory_addr + spec::size + 4);
	}

	uint32_t change_time() {
		assert(has_time_fields());
		return read_at<uint32_t>(memory_addr + spec::size + 8);
	}

	uint32_t birth_time() {
		assert(has_time_fields());
		return read_at<uint32_t>(memory_addr + spec::size + 12);
	}

	uint16_t maximum_compact_attribute_count() {
		assert(is_non_default_attribute_storage_phase_change_stored());
		return read_at<uint16_t>(memory_addr + spec::size + (has_time_fields()?16:0) + 0);
	}

	uint16_t minimum_compact_attribute_count() {
		assert(is_non_default_attribute_storage_phase_change_stored());
		return read_at<uint16_t>(memory_addr + spec::size + (has_time_fields()?16:0) + 2);
	}

	uint8_t * first_message() {
		return memory_addr
				+ spec::size
				+ (has_time_fields()?16:0)
				+ (is_non_default_attribute_storage_phase_change_stored()?4:0)
				+ get_size_of_size_of_chunk();
	}

	uint64_t size_of_chunk() {
		uint8_t * addr = memory_addr
				+ spec::size
				+ (has_time_fields()?16:0)
				+ (is_non_default_attribute_storage_phase_change_stored()?4:0);
		return get_reader_for(get_size_of_size_of_chunk())(addr);
	}

};

template<typename TRAIT>
struct object_template : public TRAIT {

	using TRAIT::object::file;
	using TRAIT::object::memory_addr;

	using TRAIT::parse_messages;
	using TRAIT::get_message_iterator;

	object_template(file_handler_t * file, uint8_t * addr) : TRAIT{file, addr}
	{
		parse_messages();
	}

	virtual ~object_template() { }


	virtual auto get_id() const -> uint64_t override {
		return file->to_offset(memory_addr);
	}

	virtual auto operator[](string const & name) const -> h5obj override
	{

		uint64_t offset = undef_offset;

		for (auto i = get_message_iterator(); not i.end() and offset == undef_offset; ++i) {
			auto msg = *i;

			switch (msg.type) {
			case MSG_LINK: {
					auto link = TRAIT::object_base::parse_link(msg.data);
					offset = link.second;
					break;
				}
			case MSG_SYMBOL_TABLE: {
					offset = object_symbol_table_t{file, msg.data}[name];
					break;
				}
			}

		}

		if (offset == undef_offset)
			throw EXCEPTION("KeyError: Key not found `%s'", name.c_str());

		return h5obj{file->make_object(offset)};
	}

	virtual auto shape() const -> vector<size_t> override
	{
		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;
			if (msg.type == MSG_DATASPACE) {
				auto dataspace = object_dataspace_t{msg.data};
				return vector<size_t>{&dataspace.shape[0], &dataspace.shape[dataspace.rank]};
			}
		}
		throw EXCEPTION("Shape is not defined");
	}

	virtual size_t shape(int i) const override
	{
		return shape()[i];
	}



	virtual auto keys() const -> vector<string> override
	{

		vector<string> ret;

		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;

			switch (msg.type) {
			case MSG_LINK: {
					auto link = TRAIT::object_base::parse_link(msg.data);
					ret.push_back(link.first);
					break;
				}
			case MSG_SYMBOL_TABLE: {
					object_symbol_table_t{file, msg.data}.ls(ret);
					break;
				}
			}

		}

		return ret;

	}

	virtual auto list_attributes() const -> vector<string> override
	{
		vector<string> ret;

		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;
			if (msg.type == MSG_ATTRIBUTE) {
				uint8_t * message_body = msg.data;
				auto version = spec_defs::message_attribute_v1_spec::version::get(message_body);

				switch (version) {
				case 1: {
					ret.push_back(reinterpret_cast<char const *>(message_body+spec_defs::message_attribute_v1_spec::size));
					break;
				}
				case 2: {
					ret.push_back(reinterpret_cast<char const *>(message_body+spec_defs::message_attribute_v2_spec::size));
					break;
				}
				case 3: {
					ret.push_back(reinterpret_cast<char const *>(message_body+spec_defs::message_attribute_v3_spec::size));
					break;
				}
				default: {
					throw EXCEPTION("Not implemented");
				}
				}
			} else if (msg.type == MSG_ATTRIBUTE_INFO) {
				cout << "WARNING: attribute info is not tested yet." << endl;
				auto attribute_info = object_attribute_info_t(file, msg.data);
				auto btree = btree_v2_header<typename spec_defs::btree_v2_record_type8>(file, file->to_address(attribute_info.attribute_name_btree_address));
				auto xx = btree.list_records();
				for(auto i: xx) {
					cout << (void*)i << endl;
				}
			}
		}

		return ret;
	}

	virtual void print_info() const override
	{
		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;

			switch(msg.type) {
			case MSG_DATASPACE:
				cout << object_dataspace_t{msg.data};
				break;
			case MSG_DATA_LAYOUT:
				cout << object_datalayout_t{file, msg.data};
				break;
			case MSG_DATA_STORAGE_FILTER_PIPELINE:
				cout << object_data_storage_filter_pipeline_t{msg.data};
				break;
			case MSG_FILL_VALUE:
				cout << object_fill_value_t{msg.data};
				break;
			case MSG_DATA_STORAGE_FILL_VALUE:
				cout << object_data_storage_fill_value_t{msg.data};
				break;
			}

		}

	}

	virtual vector<chunk_desc_t> list_chunk() const override
	{
		for (auto i = get_message_iterator(); not i.end(); ++i) {
			auto msg = *i;
			if (msg.type == MSG_DATA_LAYOUT) {
				auto datalayout = object_datalayout_t{file, msg.data};
				switch (datalayout.layout_class) {
				case object_datalayout_t::LAYOUT_CHUNKED:
					switch(datalayout.chunk_indexing_type) {
					case object_datalayout_t::CHUNK_INDEXING_BTREE_V1:
						return datalayout.chunk_btree_v1_ls();
					default:
						throw EXCEPTION("Unsupported chunk layout");
					}
					break;
				default:
					throw EXCEPTION("Dataset is not chunked");
				}
			}
		}

		throw EXCEPTION("Dataset is not chunked");
	}

};

}; // struct _impl

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::file_handler_t::make_superblock(uint64_t offset) -> shared_ptr<superblock_interface>
{
	auto x = superblock_cache.find(offset);
	if (x != superblock_cache.end())
		return dynamic_pointer_cast<superblock_interface>(x->second);

	switch(version) {
	case 0:
		return superblock_cache[offset] = make_shared<superblock_v0>(this, &memaddr[offset]);
	case 1:
		return superblock_cache[offset] = make_shared<superblock_v1>(this, &memaddr[offset]);
	case 2:
		return superblock_cache[offset] = make_shared<superblock_v2>(this, &memaddr[offset]);
	case 3:
		return superblock_cache[offset] = make_shared<superblock_v3>(this, &memaddr[offset]);
	}

	throw runtime_error("TODO" STR(__LINE__));

}

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::file_handler_t::make_object(uint64_t offset) -> shared_ptr<object_interface>
{
	auto x = object_cache.find(offset);
	if (x != object_cache.end())
		return x->second;

	int version = memaddr[offset+OFFSET_V1_OBJECT_HEADER_VERSION];
	if (version == 1) {
		cout << "creating object at " << std::hex << offset << endl;
		return (object_cache[offset] = make_shared<object_template<object_v1_trait>>(this, &memaddr[offset]));
	} else if (version == 'O') {
		uint32_t sign = *reinterpret_cast<uint32_t*>(&memaddr[offset]);
		if (sign != 0x5244484ful)
			throw EXCEPTION("Unexpected signature (0x%08x)", sign);
		version = memaddr[offset+OFFSET_V2_OBJECT_HEADER_VERSION];
		if (version != 2)
			throw EXCEPTION("Not implemented object version %d", version);
		cout << "creating object at " << std::hex << offset << endl;
		return (object_cache[offset] = make_shared<object_template<object_v2_trait>>(this, &memaddr[offset]));
	}

	throw runtime_error("TODO " STR(__LINE__));
}

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH> template <typename record_type>
_impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::group_btree_v2_node<record_type>::group_btree_v2_node(group_btree_v2<record_type> * root, uint8_t * memory_addr, uint64_t record_count, uint64_t depth) :
	root{root},
	memory_addr{memory_addr},
	record_count{record_count},
	depth{depth}
{
	uint64_t number_of_child_node;
	uint64_t total_number_of_child_node;
	root->maximum_stored_records(depth, number_of_child_node, total_number_of_child_node);
	size_of_number_of_child_node = get_minimum_storage_size_for(number_of_child_node);
	size_of_total_number_of_child_node = get_minimum_storage_size_for(total_number_of_child_node);

	read_number_of_child_node = get_reader_for(size_of_number_of_child_node);
	read_total_number_of_child_node = get_reader_for(size_of_total_number_of_child_node);

}

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH> template <typename record_type>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::group_btree_v2_node<record_type>::get_record(int i) -> uint8_t *
{
	return memory_addr[spec::size+i*(root->record_size())];
}

/* K key */
template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH> template <typename record_type>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::group_btree_v2_node<record_type>::get_node(int i) -> offset_type
{
	return *reinterpret_cast<offset_type*>(&memory_addr[spec::size+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)]);
}

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH> template <typename record_type>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::group_btree_v2_node<record_type>::get_number_of_child_node(int i) -> uint64_t
{
	return *reinterpret_cast<offset_type*>(&memory_addr[spec::size+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)+root->record_size()]);
}

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH> template <typename record_type>
auto _impl<SIZE_OF_OFFSET, SIZE_OF_LENGTH>::group_btree_v2_node<record_type>::get_total_number_of_child_node(int i) -> uint64_t
{
	return *reinterpret_cast<offset_type*>(&memory_addr[spec::size+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)+root->record_size()+size_of_number_of_child_node]);
}

struct superblock_interface;

template<int ... ARGS>
struct _for_each1;

template<int J, int I, int ... ARGS>
struct _for_each1<J, I, ARGS...> {
	static shared_ptr<file_handler_interface> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_length) {
		if (I == size_of_length) {
			return make_shared<typename _impl<J, I>::file_handler_t>(data, superblock_offset, version);
		} else {
			return _for_each1<J, ARGS...>::create(data, version, superblock_offset, size_of_length);
		}
	}
};

template<int J>
struct _for_each1<J> {
	static shared_ptr<file_handler_interface> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_length) {
		throw runtime_error("TODO" STR(__LINE__));
	}
};

template<int ... ARGS>
struct _for_each0;

template<int J, int ... ARGS>
struct _for_each0<J, ARGS...> {
	static shared_ptr<file_handler_interface> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_offset, int size_of_length) {
		if (J == size_of_offset) {
			return _for_each1<J,2,4,8>::create(data, version, superblock_offset, size_of_length);
		} else {
			return _for_each0<ARGS...>::create(data, version, superblock_offset, size_of_offset, size_of_length);
		}
	}
};

template<>
struct _for_each0<> {
	static shared_ptr<file_handler_interface> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_offset, int size_of_length) {
		throw runtime_error("TODO" STR(__LINE__));
	}
};




struct _h5dset : public _h5obj {
//	_h5dset(_h5dset const &) = delete;
//	_h5dset & operator=(_h5dset const &) = delete;
//
//	_h5dset(hid_t id, hid_t parent, string const & dsetname);
//
//	virtual ~_h5dset();
//
//	virtual vector<size_t> const & shape() const;
//	virtual size_t const & shape(int i) const;
//
//	template<typename T, typename ... ARGS>
//	T * read(ARGS ... args) const;
//
//	template<typename T>
//	vector<T> read_attribute(string const & name) const;
};

struct _h5group : public _h5obj {
//	_h5group(_h5group const &) = default;
//	_h5group & operator=(_h5group const &) = default;
//	_h5group(hid_t id, hid_t parent, string const & name);
//	virtual ~_h5group();
};

struct _h5file : public _h5obj {
	string filename;
	int fd;
	uint8_t * data;
	uint64_t file_size;
	shared_ptr<file_handler_interface> _file_impl;
	shared_ptr<_h5obj> _root_object;

	template<typename T>
	T& get(uint64_t offset) {
		return *reinterpret_cast<T*>(&data[offset]);
	}

	uint64_t lookup_for_superblock() {
		uint64_t const sign = 0x0a1a0a0d46444889UL;
		uint64_t block_offset = 0;
		if (sign == get<uint64_t>(block_offset)) {
			return block_offset;
		}
		block_offset = 512;
		while(block_offset < file_size-8) {
			if (sign == get<uint64_t>(block_offset)) {
				return block_offset;
			}
			block_offset <<= 1;
		}
		throw runtime_error("Not superblock found");
	}


	_h5file(string const & filename) {
		fd = open(filename.c_str(), O_RDONLY);
		if (fd < 0)
			throw runtime_error("TODO");
		struct stat st;
		fstat(fd, &st);
		data = static_cast<uint8_t*>(mmap(0, st.st_size, PROT_READ, MAP_SHARED, fd, 0));
		if (data == MAP_FAILED)
			throw runtime_error("TODO");
		file_size = st.st_size;

		uint64_t superblock_offset = lookup_for_superblock();
		cout << "superblock found @" << superblock_offset << endl;

		int version = get<uint8_t>(superblock_offset+OFFSET_VERSION);
		if(version > 3) {
			throw runtime_error("invalid hdf5 file version");
		}

		int size_of_offset = get<uint8_t>(superblock_offset+OFFSETX[version].offset_of_size_offset);
		int size_of_length = get<uint8_t>(superblock_offset+OFFSETX[version].offset_of_size_length);

		cout << "version = " << version << endl;
		cout << "size_of_offset = " << size_of_offset << endl;
		cout << "size_of_length = " << size_of_length << endl;

		/* folowing HDF5 ref implementation size_of_offset and size_of_length must be
		 * 2, 4, 8, 16 or 32. our implementation is limited to 2, 4 and 8 bytes, uint64_t
		 */
		_file_impl = _for_each0<2,4,8>::create(data, version, superblock_offset, size_of_offset, size_of_length);
		_root_object = _file_impl->get_superblock()->get_root_object();
	}

	virtual ~_h5file() = default;

	virtual auto get_id() const -> uint64_t override
	{
		return 0u;
	}

	virtual auto operator[](string const & name) const -> h5obj override
	{
		return _root_object->operator [](name);
	}

	virtual auto shape() const -> vector<size_t> override
	{
		return _root_object->shape();
	}

	virtual auto shape(int i) const -> size_t override
	{
		return _root_object->shape(i);
	}


	virtual auto keys() const -> vector<string> override
	{
		return _root_object->keys();
	}

	virtual auto list_attributes() const -> vector<string> override
	{
		return _root_object->list_attributes();
	}

	virtual void print_info() const override
	{
		_root_object->print_info();
	}

	virtual auto modification_time() const -> uint32_t override
	{
		return _root_object->modification_time();
	}

	virtual auto comment() const -> char const * override
	{
		return _root_object->comment();
	}

	virtual auto element_size() const -> uint64_t override
	{
		return _root_object->element_size();
	}

};

//template<typename T>
//struct _h5obj::_attr {
//	static T read(_h5obj const & obj, string const & name) {
//		hid_t attr = H5Aopen(obj.id, name.c_str(), H5P_DEFAULT);
//		if(attr < 0)
//			throw runtime_error("invalid attribute");
//		hid_t aspc = H5Aget_space(attr);
//		hsize_t size = H5Aget_storage_size(attr);
//		/* TODO: check for type matching */
//		if ((size%sizeof(T)) != 0)
//			throw runtime_error("invalid attribute type or size");
//		vector<T> ret{size/sizeof(T), vector<char>::allocator_type()};
//		H5Aread(attr, h5type<T>::type(), &ret[0]);
//		return ret[0];
//	}
//};

template<typename ... ARGS>
void h5obj::read(ARGS ... args) {
	dynamic_pointer_cast<object_interface>(_ptr)->_read_0(args...);
}

} // hdf5ng

#endif /* SRC_HDF5_NG_HXX_ */
