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

enum message_typeid_e : uint64_t {
	MSG_NIL                                = 0x0000u,
	MSG_DATASPACE                          = 0x0001u,
	MSG_LINK_INFO                          = 0x0002u,
	MSG_DATATYPE                           = 0x0003u,
	MSG_FILL_VALUE_OLD                     = 0x0004u,
	MSG_FILL_VALUE                         = 0x0005u,
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
static uint64_t read_at(uint8_t * addr) {
	return *reinterpret_cast<T*>(addr);
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
		return read_at<uint8_t>;
	case 2:
		return read_at<uint16_t>;
	case 4:
		return read_at<uint32_t>;
	case 8:
		return read_at<uint64_t>;
	default:
		return nullptr;
	}
}

/*****************************************************
 * HIGH LEVEL API
 *****************************************************/

struct file_impl;

struct file_object {
	file_impl * file;
	uint8_t * memory_addr;

	file_object() = delete;
	file_object(file_impl * file, uint8_t * addr) : file{file}, memory_addr{addr} { }
	virtual ~file_object() { }
};

struct superblock;
struct object;
struct local_heap;
struct global_heap;

struct superblock : public file_object {

	superblock(file_impl * file, uint8_t * addr) : file_object{file, addr} {}

	virtual ~superblock() = default;

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

	virtual shared_ptr<object> get_root_object() = 0;

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



struct btree : public file_object {

	virtual ~btree() = default;
	virtual int version() = 0;

};

// object can be :
//  - superblock extension
//  - dataset
//  - group
struct object : public file_object, public _h5obj {

	object(file_impl * file, uint8_t * addr) : file_object{file, addr} { }

	virtual ~object() = default;

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

	virtual auto keys() const -> vector<char const *> override {
		throw EXCEPTION("Not implemented");
	}

	virtual auto list_attributes() const -> vector<char const *> override {
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
		static void exec(object * obj, void * output, ARGS ... args) {
			obj->_read(array<slc, sizeof...(ARGS)>{slc{args}...}, output);
		}
	};

	template<typename ... ARGS>
	void _read_0(ARGS ... args) {
		_dispatch_read<ARGS...>::exec(this, args...);
	}

};


struct local_heap : public file_object {

	local_heap(file_impl * file, uint8_t * addr) : file_object{file, addr} { }

	virtual ~local_heap() = default;

	virtual uint8_t * get_data(uint64_t offset) const = 0;

};

struct global_heap : public file_object {

	virtual ~global_heap() = default;
	virtual int version() = 0;

};

struct object_data : public file_object {

	virtual ~object_data() = default;
	virtual int version() = 0;

};

struct free_space : public file_object {

	virtual ~free_space() = default;
	virtual int version() = 0;

};

struct file_impl {
	virtual ~file_impl() = default;
	virtual shared_ptr<superblock> get_superblock() = 0;
	virtual auto make_superblock(uint64_t offset) -> shared_ptr<superblock> = 0;
	virtual auto make_btree(uint64_t offset) -> shared_ptr<btree> = 0;
	virtual auto make_object(uint64_t offset) -> shared_ptr<object> = 0;
	virtual auto make_local_heap(uint64_t offset) -> shared_ptr<local_heap> = 0;
	virtual auto make_global_heap(uint64_t offset) -> shared_ptr<global_heap> = 0;

	virtual auto to_address(uint64_t offset) -> uint8_t * = 0;
	virtual auto to_offset(uint8_t * address) -> uint64_t = 0;
};

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
struct _impl {
using spec_defs = typename h5ng::spec_defs<SIZE_OF_OFFSET, SIZE_OF_LENGTH>;

using offset_type = typename get_type_for_size<SIZE_OF_OFFSET>::type;
using length_type = typename get_type_for_size<SIZE_OF_LENGTH>::type;

static offset_type constexpr undef_offset = std::numeric_limits<offset_type>::max();
static offset_type constexpr undef_length = std::numeric_limits<length_type>::max();


static uint64_t get_minimum_storage_size_for(uint64_t count) {
	if (count == 0) return 0;
	uint64_t n = 1;
	while((/*256^n*/(0x1ul<<(n<<3))-1)<count) {
		n <<= 1; /* n*=2 */
		if (n == 8) return n; // reach the maximum storage capacity;
	}
	return n;
}

struct superblock_v0 : public superblock {
	using spec = typename spec_defs::superblock_v0_spec;

	superblock_v0(file_impl * file, uint8_t * addr) : superblock{file, addr} { }

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

	virtual shared_ptr<object> get_root_object() {
		return file->make_object(root_node_object_address());
	}

};

struct superblock_v1 : public superblock {
	using spec = typename spec_defs::superblock_v1_spec;

	superblock_v1(file_impl * file, uint8_t * addr) : superblock{file, addr} { }

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

	virtual shared_ptr<object> get_root_object() {
		return file->make_object(root_node_object_address());
	}

	template<int R>
	void _read(array<slc, R> const & selection) {

	}

};

struct superblock_v2 : public superblock {
	using spec = typename spec_defs::superblock_v2_spec;

	superblock_v2(file_impl * file, uint8_t * addr) : superblock{file, addr} { }

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

	virtual shared_ptr<object> get_root_object() {
		return file->make_object(root_node_object_address());
	}

};

struct superblock_v3 : public superblock {
	using spec = typename spec_defs::superblock_v3_spec;

	superblock_v3(file_impl * file, uint8_t * addr) : superblock{file, addr} { }

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

	virtual shared_ptr<object> get_root_object() {
		return file->make_object(root_node_object_address());
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
struct btree_v2_node : public file_object {
	using spec = typename spec_defs::b_tree_v2_node_spec;

	b_tree_xxx_t const & layout;
	uint64_t child_node_count;

	btree_v2_node(file_impl * file, uint8_t * addr, b_tree_xxx_t const & layout, uint64_t child_node_count) :
		file_object{file, addr}, layout{layout}, child_node_count{child_node_count} { }
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
struct btree_v2_header : public file_object {
	using spec = typename spec_defs::b_tree_v2_hdr_spec;
	btree_v2_header(file_impl * file, uint8_t * addr) : file_object{file, addr} { }
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

struct btree_v1 : public file_object {
	using spec = typename spec_defs::b_tree_v1_hdr_spec;
	uint64_t key_length;

	btree_v1(file_impl * file, uint8_t * addr, uint64_t key_length) : file_object{file, addr}, key_length{key_length} { }

	btree_v1(btree_v1 const &) = default;

	btree_v1 & operator=(btree_v1 const & x) = default;

	/* K+1 keys */
	uint8_t * get_key(int i) const
	{
		return &memory_addr[spec::size+i*(SIZE_OF_OFFSET+key_length)];
	}

	/* K key */
	offset_type get_node(int i) const
	{
		return *reinterpret_cast<offset_type*>(&memory_addr[spec::size+i*(SIZE_OF_OFFSET+key_length)+key_length]);
	}

	uint64_t get_depth() const
	{
		return spec::node_level::get(memory_addr);
	}

	uint64_t get_entries_count() const
	{
		return spec::entries_used::get(memory_addr);
	}

};


struct group_btree_v1 : public btree_v1 {

	group_btree_v1(file_impl * file, uint8_t * addr) : btree_v1{file, addr, SIZE_OF_LENGTH} { }
	group_btree_v1(group_btree_v1 const &) = default;
	group_btree_v1 & operator=(group_btree_v1 const &) = default;

	length_type get_key(int i) const
	{
		return *reinterpret_cast<length_type*>(btree_v1::get_key(i));
	}

};

struct chunk_btree_v1 : public btree_v1 {

	chunk_btree_v1(file_impl * file, uint8_t * addr, uint64_t dimensionality) : btree_v1{file, addr, 8+8*(dimensionality)} { }
	chunk_btree_v1(chunk_btree_v1 const &) = default;
	chunk_btree_v1 & operator=(chunk_btree_v1 const &) = default;

	length_type * get_offset(int i) const
	{
		return reinterpret_cast<length_type*>(btree_v1::get_key(i)+spec_defs::b_tree_v1_chunk_key_spec::size);
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
struct group_btree_v2 : public file_object {
	using spec = typename spec_defs::b_tree_v2_hdr_spec;
	using node_type = group_btree_v2_node<record_spec>;

	map<uint64_t, shared_ptr<node_type>> _node_tree_cache;

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


struct message_v1 {
	uint8_t * addr;

	using spec = typename spec_defs::message_header_v1_spec;

	message_v1(uint8_t * addr) : addr{addr} { }
	message_v1(message_v1 const &) = default;
	message_v1 & operator=(message_v1 const &) = default;

	uint16_t type() const
	{
		return spec::type::get(addr);
	}

	uint8_t flags() const
	{
		return spec::flags::get(addr);
	}

	uint16_t size() const
	{
		return spec::size_of_message::get(addr);
	}

	uint8_t * data() const
	{
		return addr+spec::size;
	}

};

struct object_v1 : public object {
	using spec = typename spec_defs::object_header_v1_spec;

	bool has_group_btree;
	bool has_data_set;

	uint64_t K;
	shared_ptr<local_heap> lheap;
	offset_type group_btree_v1_root;
	offset_type chunk_btree_v1_root;

	uint8_t * rank;
	length_type * _shape;
	length_type * max_shape;
	length_type * permutation; // Never implement in official lib.

	uint32_t * size_of_elements;

	uint8_t * fillvalue;

	uint8_t layout_class;

	//
	offset_type data_address;

	uint8_t * dimensionality;
	uint32_t * _shape_of_chunk;
	uint32_t * size_of_data_element_of_chunk;

	// compact data optionnal elements
	uint32_t compact_data_size;

	uint64_t size_of_continuous_data;


	// Attributes info
	bool has_attribute_btree;
	uint64_t maximum_creation_index;
	uint64_t fractal_heap_address;
	uint64_t attribute_name_btree_address;
	uint64_t attribute_creation_order_btree_address;

	uint64_t first_message() const {
		uint64_t first_message_offset = file->to_offset(&memory_addr[spec::size]);
		// message in v1 are aligned, compute alignment to 8-bytes boundary
		first_message_offset -= 1ul;
		first_message_offset &= ~0x0000000000000007ul;
		first_message_offset += 0x0000000000000008ul;
		return first_message_offset;
	}

	object_v1(file_impl * file, uint8_t * addr) : object{file, addr} {
		group_btree_v1_root = undef_offset;
		has_group_btree = false;
		rank = 0;
		layout_class = -1;
		dimensionality = 0;
		size_of_continuous_data = 0;

		has_attribute_btree = false;

		cout << "creating object v1, object cache size = TODO" << endl;
		foreach_messages([this](uint8_t * m) { this->parse_message(m); });

	}

	virtual ~object_v1() { }

	void parse_attribute_info(uint8_t * msg) {
		has_attribute_btree = true;

		uint8_t flags = spec_defs::message_attribute_info_spec::flags::get(msg);

		if (flags&0x01u) {
			maximum_creation_index = read_at<uint16_t>(msg+spec_defs::message_attribute_info_spec::size);
		}

		fractal_heap_address = read_at<offset_type>(msg+spec_defs::message_attribute_info_spec::size+((flags&0x01u)?2:0));
		attribute_name_btree_address = read_at<offset_type>(msg+spec_defs::message_attribute_info_spec::size+((flags&0x01u)?2:0)+SIZE_OF_OFFSET);

		if (flags&0x02u) {
			attribute_creation_order_btree_address = read_at<offset_type>(msg+spec_defs::message_attribute_info_spec::size+((flags&0x01u)?2:0)+SIZE_OF_OFFSET+SIZE_OF_OFFSET);
		}

	}

	void parse_dataspace(uint8_t * msg) {
		cout << "parse_dataspace " << std::dec <<
				" version="<< static_cast<int>(spec_defs::message_dataspace_spec::version::get(msg)) << " "
				" rank=" << static_cast<int>(spec_defs::message_dataspace_spec::rank::get(msg)) << " "
				" flags=0x" << std::hex << static_cast<int>(spec_defs::message_dataspace_spec::flags::get(msg)) << std::dec
				<< endl;

		rank = &spec_defs::message_dataspace_spec::rank::get(msg);


		if (spec_defs::message_dataspace_spec::version::get(msg) == 1) {
			// version 1

			_shape = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v1);

			if (spec_defs::message_dataspace_spec::flags::get(msg)&0x01) {
				max_shape = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v1 + *rank*SIZE_OF_LENGTH);
			} else {
				max_shape = nullptr;
			}

			if (spec_defs::message_dataspace_spec::flags::get(msg)&0x02) {
				if (spec_defs::message_dataspace_spec::flags::get(msg)&0x01) {
					permutation = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v1 + (*rank*SIZE_OF_LENGTH)*2);
				} else {
					permutation = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v1 + *rank*SIZE_OF_LENGTH);
				}
			} else {
				permutation = nullptr;
			}

		} else if (spec_defs::message_dataspace_spec::version::get(msg) == 2) {
			// version 2

			_shape = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v2);
			if (spec_defs::message_dataspace_spec::flags::get(msg)&0x01) {
				max_shape = reinterpret_cast<length_type*>(msg + spec_defs::message_dataspace_spec::size_v2 + *rank*SIZE_OF_LENGTH);
			} else {
				max_shape = nullptr;
			}

			// in version 2 permutation is removed, because it was never implemented.
			permutation = nullptr;
		}

	}

	void parse_datatype(uint8_t * msg) {
		cout << "parse_datatype " << std::dec <<
				" version="<< static_cast<int>((spec_defs::message_datatype_spec::class_and_version::get(msg)&0xf0) >> 4) << " "
				" size=" << static_cast<unsigned>(spec_defs::message_datatype_spec::size_of_elements::get(msg))
				<< endl;

		// TODO
		size_of_elements = &spec_defs::message_datatype_spec::size_of_elements::get(msg);

	}

	void parse_fillvalue_old(uint8_t * msg) {
		cout << "parse_datatype " << std::dec <<
				" size=" << static_cast<unsigned>(spec_defs::message_fillvalue_old_spec::size_of_fillvalue::get(msg))
				<< endl;
		// TODO
		fillvalue = msg + spec_defs::message_fillvalue_old_spec::size;
	}

	void parse_fillvalue(uint8_t * msg) {
		cout << "parse_datatype " << std::dec <<
				" size=" << static_cast<unsigned>(spec_defs::message_fillvalue_spec::version::get(msg)) <<
				" space_allocation_time=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::space_allocation_time::get(msg)) << std::dec <<
				" fillvalue_write_time=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::fillvalue_write_time::get(msg)) << std::dec <<
				" fillvalue_defined=0x" << std::hex << static_cast<int>(spec_defs::message_fillvalue_spec::fillvalue_defined::get(msg)) << std::dec <<
				endl;
		// TODO
		if (spec_defs::message_fillvalue_spec::version::get(msg) == 1 or spec_defs::message_fillvalue_spec::fillvalue_defined::get(msg) != 0) {
			fillvalue = msg + spec_defs::message_fillvalue_spec::size + sizeof(uint32_t);
		}
	}

	void parse_datalayout(uint8_t * msg) {
		cout << "parse_datalayout " << std::dec <<
				" version=" << static_cast<unsigned>(spec_defs::message_data_layout_v1_spec::version::get(msg)) <<
				endl;

		auto version = spec_defs::message_data_layout_v1_spec::version::get(msg);
		if (version == 1 or version == 2)
		{
			layout_class = spec_defs::message_data_layout_v1_spec::layout_class::get(msg);
			dimensionality = &spec_defs::message_data_layout_v1_spec::dimensionnality::get(msg);

			if (layout_class != 0) {
				data_address = read_at<offset_type>(msg+spec_defs::message_data_layout_v1_spec::size);
			} else {
				data_address = file->to_offset(msg
						+ spec_defs::message_data_layout_v1_spec::size
						+ *dimensionality * sizeof(offset_type)
						+ sizeof(uint32_t));
			}

			_shape_of_chunk = reinterpret_cast<uint32_t*>(
					msg + spec_defs::message_data_layout_v1_spec::size + ((layout_class==0)?0:sizeof(offset_type)));

			if(layout_class == 2) { // chunked
				size_of_data_element_of_chunk = reinterpret_cast<uint32_t*>(
						msg + spec_defs::message_data_layout_v1_spec::size
						+ sizeof(offset_type)
						+ *dimensionality * sizeof(uint32_t)
						+ sizeof(uint32_t));
			} else if (layout_class == 0) { // compact
				compact_data_size = read_at<uint32_t>(
						msg + spec_defs::message_data_layout_v1_spec::size
						+ *dimensionality * sizeof(offset_type));
			}


		} else if (version == 3) {
			layout_class = spec_defs::message_data_layout_v3_spec::layout_class::get(msg);
			if (layout_class == 0) { // COMPACT
				compact_data_size = read_at<uint16_t>(msg + spec_defs::message_data_layout_v3_spec::size);
				data_address = file->to_offset(msg
								+ spec_defs::message_data_layout_v3_spec::size
								+ sizeof(uint16_t));
			} else if (layout_class == 1) { // CONTINUOUS
				data_address = read_at<offset_type>(msg + spec_defs::message_data_layout_v3_spec::size);
				size_of_continuous_data = read_at<length_type>(msg + spec_defs::message_data_layout_v3_spec::size + sizeof(offset_type));
			} else if (layout_class == 2) { // CHUNKED
				dimensionality = msg + spec_defs::message_data_layout_v3_spec::size;
				chunk_btree_v1_root = read_at<offset_type>(msg + spec_defs::message_data_layout_v3_spec::size + sizeof(uint8_t));
				_shape_of_chunk = reinterpret_cast<uint32_t*>(
						msg
						+ spec_defs::message_data_layout_v3_spec::size
						+ sizeof(uint8_t) // dimensionality
						+ sizeof(offset_type) // data_address
						);
				size_of_data_element_of_chunk = reinterpret_cast<uint32_t*>(
						msg + spec_defs::message_data_layout_v3_spec::size
						+ sizeof(uint8_t)
						+ sizeof(offset_type)
						+ *dimensionality * sizeof(uint32_t));
			}
		} else if (version == 4) {
			layout_class = spec_defs::message_data_layout_v4_spec::layout_class::get(msg);
			if (layout_class == 0) { // COMPACT same as V3
				compact_data_size = read_at<uint16_t>(msg + spec_defs::message_data_layout_v4_spec::size);
				data_address = file->to_offset(msg
								+ spec_defs::message_data_layout_v4_spec::size
								+ sizeof(uint16_t));
			} else if (layout_class == 1) { // CONTINUOUS same as V3
				data_address = read_at<offset_type>(msg + spec_defs::message_data_layout_v4_spec::size);
				size_of_continuous_data = read_at<length_type>(msg
						+ spec_defs::message_data_layout_v4_spec::size
						+ sizeof(offset_type));
			} else if (layout_class == 2) { // CHUNKED /!\ not the same as V3
				// TODO: flags
				dimensionality = msg + spec_defs::message_data_layout_v4_spec::size + sizeof(uint8_t);
				// TODO: Dimension Size Encoded Length
				_shape_of_chunk = reinterpret_cast<uint32_t*>(
						msg
						+ spec_defs::message_data_layout_v4_spec::size // header
						+ sizeof(uint8_t) // flags
						+ sizeof(uint8_t) // dimentionnality
						+ sizeof(uint8_t) // Dimension Size Encoded Length
						);

				// TODO: chunk indexing type
				uint8_t chunk_indexing_type = read_at<uint8_t>(
						msg
						+ spec_defs::message_data_layout_v4_spec::size // header
						+ sizeof(uint8_t) // flags
						+ sizeof(uint8_t) // dimentionnality
						+ sizeof(uint8_t) // Dimension Size Encoded Length
						+ *dimensionality * sizeof(uint32_t) // shape_of_chunk;
						);

				// TODO: parse indexing data
				uint64_t size_of_index_data = 0;
				switch(chunk_indexing_type) {
				case 1:
					size_of_index_data = sizeof(length_type) + sizeof(uint32_t);
					break;
				case 2:
					size_of_index_data = 0;
					break;
				case 3:
					size_of_index_data = sizeof(uint8_t);
					break;
				case 4:
					size_of_index_data = 6;
					break;
				case 5:
					size_of_index_data = 6;
					break;
				}

				data_address = read_at<offset_type>(
						msg
						+ spec_defs::message_data_layout_v4_spec::size // header
						+ sizeof(uint8_t) // flags
						+ sizeof(uint8_t) // dimentionnality
						+ sizeof(uint8_t) // Dimension Size Encoded Length
						+ *dimensionality * sizeof(uint32_t) // shape_of_chunk;
						+ sizeof(uint8_t) // index_type
						+ size_of_index_data
						);

			} else if (layout_class == 3) { // VIRTUAL
				// TODO: defined in HDF5-1.10
			}
		}


		// TODO
	}

	void parse_object_modifcation_time(uint8_t * msg) {
		cout << "parse_object_modifcation_time " << std::dec <<
				" version=" << static_cast<unsigned>(spec_defs::message_object_modification_time_spec::version::get(msg)) <<
				" time=" << static_cast<unsigned>(spec_defs::message_object_modification_time_spec::time::get(msg)) <<
				endl;
		// TODO
	}

	void parse_symbol_table(uint8_t * msg) {
		cout << "parse_symbol_table " << std::dec
				<< spec_defs::message_symbole_table_spec::local_heap_address::get(msg) << " "
				<< spec_defs::message_symbole_table_spec::b_tree_v1_address::get(msg)
				<< endl;
		lheap = file->make_local_heap(spec_defs::message_symbole_table_spec::local_heap_address::get(msg));
		group_btree_v1_root = spec_defs::message_symbole_table_spec::b_tree_v1_address::get(msg);
	}

	void parse_message(uint8_t * current_message) {
		uint8_t message_type    = spec_defs::message_header_v1_spec::type::get(current_message);
		uint16_t message_size   = spec_defs::message_header_v1_spec::size_of_message::get(current_message);
		uint8_t message_flags   = spec_defs::message_header_v1_spec::flags::get(current_message);

		switch(message_type) {
		case 0x0001:
			parse_dataspace(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0003:
			parse_datatype(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0004:
			parse_fillvalue_old(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0005:
			parse_fillvalue(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0008:
			parse_datalayout(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x000C:
			// ignore attribute message
			break;
		case 0x0012:
			parse_object_modifcation_time(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0011:
			parse_symbol_table(current_message+spec_defs::message_header_v1_spec::size);
			break;
		case 0x0015:
			parse_attribute_info(current_message+spec_defs::message_header_v1_spec::size);
			break;
		default:
			cout << "found message <@" << static_cast<void*>(current_message)
					<< " type = 0x" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(message_type) << std::hex
					<< " size=" << message_size
					<< " flags=" << std::hex << static_cast<int>(message_flags) << ">" << endl;
			break;
		}
	}

	struct message_list {
		file_impl *   _file;
		uint64_t      _msg_count;
		uint8_t *     _bgn;
		uint8_t *     _end;

		message_list(file_impl * file, uint64_t msg_count, uint8_t * bgn, uint8_t * end) :
			_file{file}, _msg_count{msg_count}, _bgn{bgn}, _end{end} { }

		struct iterator : public std::iterator<std::forward_iterator_tag, message_v1> {
			file_impl * file;
			uint64_t msg_count;
			list<pair<uint8_t *, uint8_t *>> message_block_queue;

			// end iterator
			iterator() : file{nullptr}, msg_count{0ul} { }

			// begin iterator
			iterator(file_impl * file, uint64_t msg_count, uint8_t * bgn, uint8_t * end) :
				file{file},
				msg_count{msg_count}
			{
				message_block_queue.emplace_front(bgn, end);
			}

			message_v1 operator*() const
			{
				return message_v1{message_block_queue.front().first};
			}

			void _next_msg()
			{
				if (msg_count <= 0ul or message_block_queue.empty())
					throw EXCEPTION("message iterator overflow");
				message_block_queue.front().first += spec_defs::message_header_v1_spec::size
					+  spec_defs::message_header_v1_spec::size_of_message::get(message_block_queue.front().first);
				--msg_count;
				while(not message_block_queue.empty()) {
					if (message_block_queue.front().first >= message_block_queue.front().second) {
						message_block_queue.pop_front();
					} else {
						break;
					}
				}
			}

			// iterate throw message, handling MSG_OBJECT_HEADER_CONTINUATION
			iterator & operator++()
			{
				_next_msg();

				// go to next message
				while (not message_block_queue.empty()) {
					uint8_t * msg = message_block_queue.front().first;
					uint8_t message_type = spec_defs::message_header_v1_spec::type::get(msg);
					if (message_type == MSG_OBJECT_HEADER_CONTINUATION) {
						_next_msg();
						// go to the sub list.
						uint64_t offset = spec_defs::message_object_header_continuation_spec::offset::get(msg+spec_defs::message_header_v1_spec::size);
						uint64_t length = spec_defs::message_object_header_continuation_spec::length::get(msg+spec_defs::message_header_v1_spec::size);
						message_block_queue.emplace_front(file->to_address(offset), file->to_address(offset+length));
					} else {
						return *this;
					}
				}

				return *this;

			}

			bool operator==(iterator const & x) const
			{
				return msg_count == x.msg_count;
			}

			bool operator!=(iterator const & x) const
			{
				return msg_count != x.msg_count;
			}

		};

		iterator begin() const {
			return iterator{_file, _msg_count, _bgn, _end};
		}

		iterator end() const {
			return iterator{};
		}

	};


	template<typename F>
	void foreach_messages(F func) const {

		uint64_t msg_count = spec::total_number_of_header_message::get(memory_addr);
		cout << "parsing "<< msg_count <<" messages" << endl;
		cout << "object header size = " << spec::header_size::get(memory_addr) << endl;

		list<pair<offset_type, offset_type>> message_block_queue;
		message_block_queue.push_back(pair<offset_type, offset_type>{
			first_message(),
			file->to_offset(memory_addr)+spec::size+spec::header_size::get(memory_addr)}
		);

		while(!message_block_queue.empty()) {
			uint8_t * msg = file->to_address(message_block_queue.front().first);
			uint8_t * end = file->to_address(message_block_queue.front().second);
			while(msg < end and msg_count > 0ul) {
				uint8_t message_type    = spec_defs::message_header_v1_spec::type::get(msg);
				if (message_type == 0x0010) {
					uint64_t offset = spec_defs::message_object_header_continuation_spec::offset::get(msg+spec_defs::message_header_v1_spec::size);
					uint64_t length = spec_defs::message_object_header_continuation_spec::length::get(msg+spec_defs::message_header_v1_spec::size);
					message_block_queue.push_back(pair<offset_type, offset_type>{offset, offset+length});
				} else {
					func(msg);
				}
				msg += spec_defs::message_header_v1_spec::size
					+  spec_defs::message_header_v1_spec::size_of_message::get(msg);
				--msg_count;
			}
			message_block_queue.pop_front();
		}

		if (msg_count != 0)
			throw EXCEPTION("Message count is invalid (%d)", msg_count);
	}

	virtual auto get_id() const -> uint64_t override {
		return file->to_offset(memory_addr);
	}

	virtual auto operator[](string const & name) const -> h5obj override {
		auto offset = _group_find(name.c_str());
		return h5obj{file->make_object(offset)};
	}

	virtual vector<size_t> shape() const override {
		if (_shape) {
			return vector<size_t>{&_shape[0], &_shape[*rank]};
		}
		throw EXCEPTION("shape is not defined");
	}

	virtual size_t shape(int i) const override {
		if (not _shape)
			throw EXCEPTION("Shape is not defined");
		if (i >= * rank)
			throw EXCEPTION("request shape is out of bounds (%d)", static_cast<int>(*rank));
		return _shape[i];
	}

	virtual uint64_t element_size() const override {
		return *size_of_elements;
	}

	virtual uint8_t * continuous_data() const override {
		return file->to_address(data_address);
	}

	virtual uint8_t * fill_value() const override {
		return fillvalue;
	}

	virtual uint8_t data_layout() const override {
		return layout_class;
	}

	virtual vector<size_t> shape_of_chunk() const override {
		if (_shape_of_chunk) {
			return vector<size_t>{&_shape_of_chunk[0], &_shape_of_chunk[*rank]};
		}
		throw EXCEPTION("shape_of_chunk is not defined");
	}

	char const * _get_link_name(uint64_t offset) const {
		return reinterpret_cast<char *>(lheap->get_data(offset));
	}

	offset_type _group_find(char const * key) const
	{
		auto group_symbol_table_offset = _group_find_key(key);
		group_symbol_table table{file->to_address(group_symbol_table_offset)};
		for(auto symbol_table_entry: table.get_symbole_entry_list()) {
			char const * link_name = _get_link_name(symbol_table_entry->link_name_offset());
			cout << "check link name = " << link_name << endl;
			if (std::strcmp(key, link_name) == 0)
				return symbol_table_entry->offset_header_address();
		}

		throw EXCEPTION("key `%s' not found", key);

	}

	size_t _find_sub_group(group_btree_v1 const & cur, char const * key) const
	{
		for(size_t i = 0; i < cur.get_entries_count(); ++i) {
			char const * link_name = _get_link_name(cur.get_key(i+1));
			if (std::strcmp(key, link_name) <= 0)
				return i;
		}
		throw EXCEPTION("This should never append");
	}

	/** return group_symbole_table that content the key **/
	offset_type _group_find_key(char const * key) const {
		group_btree_v1 cur{file, file->to_address(group_btree_v1_root)};
		if (std::strcmp(key, _get_link_name(cur.get_key(cur.get_entries_count()))) > 0)
			throw EXCEPTION("Key `%s' not found", key);

		while(cur.get_depth() != 0) {
			size_t i = _find_sub_group(cur, key);
			cur = group_btree_v1{file, file->to_address(cur.get_node(i))};
		}

		size_t i = _find_sub_group(cur, key);
		return cur.get_node(i);

	}

	template<typename T0, typename T1>
	static int chunk_offset_cmp(T0 const * a, T1 const * b, uint64_t rank) {
		for(uint64_t i = 0; i < rank; ++i) {
			if(a[i] == b[i]) continue;
			else if (a[i] > b[i]) { return 1; }
			else if (a[i] < b[i]) { return -1; }
		}
		return 0;
	}

	bool key_is_in_chunk(uint64_t const * key, length_type const * offset) const {
		for(int i = 0; i < *rank; ++i) {
			if(key[i] >= (offset[i]+_shape_of_chunk[i])) {
				return false;
			}
		}
		return true;
	}

	size_t _find_sub_chunk(chunk_btree_v1 const & cur, uint64_t const * key) const {
		size_t i = cur.get_entries_count()-1;
		while (i >= 0) {
			length_type * offset = cur.get_offset(i);
			if (chunk_offset_cmp(key, offset, *rank) >= 0) {
				break;
			}
			--i;
		}
		return i;
	}

	virtual uint8_t * dataset_find_chunk(uint64_t const * key) const override {

		chunk_btree_v1 cur{file, file->to_address(chunk_btree_v1_root), *dimensionality};

		// Are we bellow the first chunk ?
		if (chunk_offset_cmp(key, cur.get_offset(0), *rank) < 0) {
			return nullptr;
		}

		while(cur.get_depth() != 0) {
			size_t i = _find_sub_chunk(cur, key);
			cur = chunk_btree_v1{file, file->to_address(cur.get_node(i)), *dimensionality};
		}

		size_t i = _find_sub_chunk(cur, key);
		if (chunk_offset_cmp(key, cur.get_offset(i), *rank) == 0) { // check if we are within the chunk
			return file->to_address(cur.get_node(i));
		} else {
			return nullptr;
		}

	}

	virtual auto keys() const -> vector<char const *> override {

		if(group_btree_v1_root == undef_offset)
			return vector<char const *>{};

		vector<char const *> ret;
		stack<group_btree_v1> stack;

		vector<offset_type> group_symbole_tables;
		cout << "btree-root = " << std::hex << group_btree_v1_root << std::dec << endl;
		stack.push(group_btree_v1{file, file->to_address(group_btree_v1_root)});
		while(not stack.empty()) {
			group_btree_v1 cur = stack.top();
			cout << std::dec << "process node depth=" << cur.get_depth() << " entries_count=" << cur.get_entries_count() << endl;
			stack.pop();
			if (cur.get_depth() == 0) {
				//assert(cur.get_entries_count() <= lK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					group_symbole_tables.push_back(cur.get_node(i));
				}
			} else {
				//assert(cur.get_entries_count() <= nK);
				for(int i = 0; i < cur.get_entries_count(); ++i) {
					stack.push(group_btree_v1{file, file->to_address(cur.get_node(i))});
				}
			}
		}

		for(auto offset: group_symbole_tables) {
			group_symbol_table table{file->to_address(offset)};
			for(auto symbol_table_entry: table.get_symbole_entry_list()) {
				ret.push_back(_get_link_name(symbol_table_entry->link_name_offset()));
			}
		}

		return ret;

	}

	message_list messages() const
	{
		uint64_t msg_count = spec::total_number_of_header_message::get(memory_addr);
		uint8_t * bgn = file->to_address(first_message());
		uint8_t * end = memory_addr+spec::size+spec::header_size::get(memory_addr);
		return message_list{file, msg_count, bgn, end};
	}


	virtual auto list_attributes() const -> vector<char const *> override {
		vector<char const *> ret;

		for (auto msg: messages()) {
			if (msg.type() == MSG_ATTRIBUTE) {
				uint8_t * message_body = msg.data();
				auto version = spec_defs::message_attribute_v1_spec::version::get(message_body);

				if(version == 1) {
					ret.push_back(reinterpret_cast<char *>(message_body+spec_defs::message_attribute_v1_spec::size));
				} else {
					throw EXCEPTION("Not implemented");
				}
			}
		}

		if (has_attribute_btree) {
			auto btree = btree_v2_header<typename spec_defs::btree_v2_record_type8>(file, file->to_address(attribute_name_btree_address));
			auto xx = btree.list_records();
			cout << "XXXXX" << endl;
			for(auto i: xx) {
				cout << (void*)i << endl;
			}
			cout << "TTTTT" << endl;
		}

		return ret;
	}

	virtual void print_info() const override {
		cerr << "XXXXXXXXXXXXXXXX" << endl;
		if(has_data_set) {
			cout << std::dec;
			cout << "rank = " << static_cast<unsigned>(*rank) << endl;

			if (_shape) {
				cout << "shape= {";
				for(unsigned i = 0; i < *rank-1; ++i) {
					cout << _shape[i] << ",";
				}
				cout << _shape[*rank-1] << "}" << endl;
			}

			if (max_shape) {
				cout << "max_shape= {";
				for(unsigned i = 0; i < *rank-1; ++i) {
					cout << max_shape[i] << ",";
				}
				cout << max_shape[*rank-1] << "}" << endl;
			}

			if (permutation) {
				cout << "permutation= {";
				for(unsigned i = 0; i < *rank-1; ++i) {
					cout << max_shape[i] << ",";
				}
				cout << max_shape[*rank-1] << "}" << endl;
			}

			if (size_of_elements) {
				cout << "size_of_elements= " << static_cast<unsigned>(*size_of_elements) << endl;
			}

			if (fillvalue) {
				cout << "has_fillvalue" << endl;
			} else {
				cout << "do not has fillvalue" << endl;
			}

			if (_shape_of_chunk) {
				cout << "dimensionality=" << static_cast<unsigned>(*dimensionality) << endl;
				cout << "shape_of_chunk= {";
				for(unsigned i = 0; i < *dimensionality-1; ++i) {
					cout << _shape_of_chunk[i] << ",";
				}
				cout << _shape_of_chunk[*dimensionality-1] << "}" << endl;
			}

			if (layout_class == 0) {
				cout << "compact layout" << endl;
				cout << "size of compact data =" << compact_data_size << endl;
				cout << "dataset address =" << data_address << endl;
			} else if (layout_class == 1) {
				cout << "continuous layout" << endl;
				cout << "dataset address = " << data_address << endl;
			} else if (layout_class == 2) {
				cout << "chunked layout (btree-v1)" << endl;
				cout << "dataset address = " << data_address << endl;
			}
		}

		auto attributes = list_attributes();

		cout << "Attributes:" << endl;
		for (auto a: attributes) {
			cout << a << endl;
		}

	}

	virtual auto modification_time() const -> uint32_t override
	{
		auto l = messages();
		auto msg = std::find_if(l.begin(), l.end(), [](message_v1 m) -> bool { return m.type() == MSG_OBJECT_MODIFICATION_TIME; });
		if (msg == l.end()) {
			return numeric_limits<uint32_t>::max();
		} else {
			uint8_t * message_body = (*msg).data();
			uint8_t version = spec_defs::message_object_modification_time_spec::version::get(message_body);
			if (version != 1)
				throw EXCEPTION("unknown modification time version (%d)", version);
			return spec_defs::message_object_modification_time_spec::time::get(message_body);
		}
	}

	virtual auto comment() const -> char const * override
	{
		char const * comment = nullptr;
		foreach_messages([&comment] (uint8_t * current_message) {
			uint8_t message_type    = spec_defs::message_header_v1_spec::type::get(current_message);
			uint16_t message_size   = spec_defs::message_header_v1_spec::size_of_message::get(current_message);
			uint8_t message_flags   = spec_defs::message_header_v1_spec::flags::get(current_message);
			if (message_type == MSG_OBJECT_COMMENT) {
				uint8_t * message_body = current_message+spec_defs::message_header_v1_spec::size;
				comment = reinterpret_cast<char *>(message_body);
			}
		});
		return comment;
	}

};


struct object_v2 : public object {
	using spec = typename spec_defs::object_header_v2_spec;

	object_v2(file_impl * file, uint8_t * addr) : object{file, addr} {
		cout << "creating object v2, object cache size = TODO" << endl;

		uint64_t data_size = size_of_chunk();
		uint8_t * msg = first_message();

		uint8_t * end = (first_message()+data_size);
		while(msg < end) {
			msg = parse_message(msg);
		}

	}

	virtual ~object_v2() { }

	uint8_t get_size_of_size_of_chunk() {
		return 0x03u&spec::flags::get(memory_addr);
	}

	bool is_attribute_creation_order_tracked() {
		return (0x01<<2)&spec::flags::get(memory_addr);
	}

	bool is_attribute_creation_order_indexed() {
		return (0x01<<3)&spec::flags::get(memory_addr);
	}

	bool is_non_default_attribute_storage_phase_change_stored() {
		return (0x01<<4)&spec::flags::get(memory_addr);
	}

	bool has_times_field() {
		return (0x01<<5)&spec::flags::get(memory_addr);
	}

	uint32_t access_time() {
		assert(has_times_field());
		return read_at<uint32_t>(memory_addr + spec::size + 0);
	}

	uint32_t modification_time() {
		assert(has_times_field());
		return read_at<uint32_t>(memory_addr + spec::size + 4);
	}

	uint32_t change_time() {
		assert(has_times_field());
		return read_at<uint32_t>(memory_addr + spec::size + 8);
	}

	uint32_t birth_time() {
		assert(has_times_field());
		return read_at<uint32_t>(memory_addr + spec::size + 12);
	}

	uint16_t maximum_compact_attribute_count() {
		assert(is_non_default_attribute_storage_phase_change_stored());
		return read_at<uint16_t>(memory_addr + spec::size + (has_times_field()?16:0) + 0);
	}

	uint16_t minimum_compact_attribute_count() {
		assert(is_non_default_attribute_storage_phase_change_stored());
		return read_at<uint16_t>(memory_addr + spec::size + (has_times_field()?16:0) + 2);
	}

	uint8_t * first_message() {
		return memory_addr
				+ spec::size
				+ (has_times_field()?16:0)
				+ (is_non_default_attribute_storage_phase_change_stored()?4:0)
				+ get_size_of_size_of_chunk();
	}

	uint64_t size_of_chunk() {
		uint8_t * addr = memory_addr
				+ spec::size
				+ (has_times_field()?16:0)
				+ (is_non_default_attribute_storage_phase_change_stored()?4:0);
		return get_reader_for(get_size_of_size_of_chunk())(addr);
	}

	uint8_t * parse_message(uint8_t * current_message) {
		uint8_t message_type = spec_defs::message_header_v2_spec::type::get(current_message);
		cout << "found mesage <@" << current_message << " type = " << message_type << ">" << endl;

		switch(message_type) {
		case 1:
			break;
		case 2:
			break;
		case 3:
			break;
		case 4:
			break;
		case 5:
			break;
		case 6:
			break;
		case 7:
			break;
		case 8:
			break;
		case 9:
			break;
		case 10:
			break;
		case 11:
			break;
		case 12:
			break;
		case 13:
			break;
		case 14:
			break;
		case 15:
			break;
		}

		// next message
		return current_message
				+ spec_defs::message_header_v2_spec::size
				+ (is_attribute_creation_order_tracked()?2:0)
				+ spec_defs::message_header_v2_spec::size_of_message_data::get(current_message);

	}

};

struct local_heap_v0 : public local_heap {
	using spec = typename spec_defs::local_heap_spec;

	local_heap_v0(file_impl * file, uint8_t * addr) : local_heap{file, addr} { }

	virtual auto get_data(uint64_t offset) const -> uint8_t *
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


struct file_impl : public h5ng::file_impl {
	mutable map<uint64_t, shared_ptr<superblock>> superblock_cache;
	mutable map<uint64_t, shared_ptr<object>> object_cache;
	mutable map<uint64_t, shared_ptr<local_heap>> local_heap_cache;
	mutable map<uint64_t, shared_ptr<btree>> btree_cache;

	uint8_t * data;
	int version;
	uint64_t superblock_offset;

	file_impl(uint8_t * data, uint64_t superblock_offset, int version) :
		data{data},
		version{version},
		superblock_offset{superblock_offset}
	{

	}

	virtual auto make_superblock(uint64_t offset) -> shared_ptr<superblock>
	{
		auto x = superblock_cache.find(offset);
		if (x != superblock_cache.end())
			return dynamic_pointer_cast<superblock>(x->second);

		switch(version) {
		case 0:
			return superblock_cache[offset] = make_shared<superblock_v0>(this, &data[offset]);
		case 1:
			return superblock_cache[offset] = make_shared<superblock_v1>(this, &data[offset]);
		case 2:
			return superblock_cache[offset] = make_shared<superblock_v2>(this, &data[offset]);
		case 3:
			return superblock_cache[offset] = make_shared<superblock_v3>(this, &data[offset]);
		}

		throw runtime_error("TODO" STR(__LINE__));

	}

	shared_ptr<superblock> get_superblock() {
		return make_superblock(superblock_offset);
	}

	virtual auto make_object(uint64_t offset) -> shared_ptr<object> {
		auto x = object_cache.find(offset);
		if (x != object_cache.end())
			return x->second;

		int version = data[offset+OFFSET_V1_OBJECT_HEADER_VERSION];
		if (version == 1) {
			cout << "creating object at " << std::hex << offset << endl;
			return (object_cache[offset] = make_shared<object_v1>(this, &data[offset]));
		} else if (version == 'O') {
			uint32_t sign = *reinterpret_cast<uint32_t*>(&data[offset]);
			if (sign != 0x5244484ful)
				throw EXCEPTION("Unexpected signature (0x%08x)", sign);
			version = data[offset+OFFSET_V2_OBJECT_HEADER_VERSION];
			if (version != 2)
				throw EXCEPTION("Not implemented object version %d", version);
			cout << "creating object at " << std::hex << offset << endl;
			return (object_cache[offset] = make_shared<object_v2>(this, &data[offset]));
		}

		throw runtime_error("TODO " STR(__LINE__));
	}


	virtual auto make_btree(uint64_t offset) -> shared_ptr<btree> {
		throw runtime_error("TODO " STR(__LINE__));
	}

	virtual auto make_local_heap(uint64_t offset) -> shared_ptr<local_heap> {
		auto x = local_heap_cache.find(offset);
		if (x != local_heap_cache.end())
			return x->second;
		return (local_heap_cache[offset] = make_shared<local_heap_v0>(this, &data[offset]));
	}

	virtual auto make_global_heap(uint64_t offset) -> shared_ptr<global_heap> {
		throw runtime_error("TODO " STR(__LINE__));
	}

	virtual auto to_address(uint64_t offset) -> uint8_t * override {
		return &data[offset];
	}

	virtual auto to_offset(uint8_t * address) -> uint64_t override {
		return address-data;
	}

}; // struct _impl

};


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

struct superblock;

template<int ... ARGS>
struct _for_each1;

template<int J, int I, int ... ARGS>
struct _for_each1<J, I, ARGS...> {
	static shared_ptr<file_impl> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_length) {
		if (I == size_of_length) {
			return make_shared<typename _impl<J, I>::file_impl>(data, superblock_offset, version);
		} else {
			return _for_each1<J, ARGS...>::create(data, version, superblock_offset, size_of_length);
		}
	}
};

template<int J>
struct _for_each1<J> {
	static shared_ptr<file_impl> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_length) {
		throw runtime_error("TODO" STR(__LINE__));
	}
};

template<int ... ARGS>
struct _for_each0;

template<int J, int ... ARGS>
struct _for_each0<J, ARGS...> {
	static shared_ptr<file_impl> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_offset, int size_of_length) {
		if (J == size_of_offset) {
			return _for_each1<J,2,4,8>::create(data, version, superblock_offset, size_of_length);
		} else {
			return _for_each0<ARGS...>::create(data, version, superblock_offset, size_of_offset, size_of_length);
		}
	}
};

template<>
struct _for_each0<> {
	static shared_ptr<file_impl> create(uint8_t * data, int version, uint64_t superblock_offset, int size_of_offset, int size_of_length) {
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
	shared_ptr<file_impl> _file_impl;
	shared_ptr<_h5obj> _root_object;

	template<typename T>
	T get(uint64_t offset) {
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


	virtual auto keys() const -> vector<char const *> override
	{
		return _root_object->keys();
	}

	virtual auto list_attributes() const -> vector<char const *> override
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
	dynamic_pointer_cast<object>(_ptr)->_read_0(args...);
}

} // hdf5ng

#endif /* SRC_HDF5_NG_HXX_ */
