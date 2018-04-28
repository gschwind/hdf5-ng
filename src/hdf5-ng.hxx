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
#include <type_traits>

#include "h5ng-spec.hxx"

#define STR(x) #x

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
static uint64_t const OFFSET_V2_OBJECT_HEADER_VERSION = 8;

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

	uint8_t * base_addr();

};

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
struct object : public file_object {
//	enum object_type_e type; // store the guessed real type.
//
//	// dataset message
//	vector<uint64_t> shape;
//	vector<uint64_t> shape_max;
//	vector<uint64_t> permutation; // never been implemented.
//
//	dataset_layout_e dset_layout;
//	uint64_t data_offset;
//
//	// link info message
//	uint64_t maximum_creation_index;
//	uint64_t fractal_heap;
//	uint64_t btree_v2_address;
//	uint64_t create_order_btree_address;
//
//	// Datatype
//	uint64_t type_size;
//
//	// fillvalue
//	vector<uint8_t> fillvalue_data;
//
//	// Link Messages
//	list<void*> link_list;

	// DataStorage
	// TODO

	// Data layout
	// TODO

	// Group info message
	// TODO

	// Filter pipe line
	// Only used for chunked dataset.

	// Attrubute message
//	map<string, vector<uint8_t>> attributes;

	// Comments
	// TODO

	// object modification time

	object(file_impl * file, uint8_t * addr) : file_object{file, addr} { }

	virtual ~object() = default;

};

struct local_heap : public file_object {

	virtual ~local_heap() = default;

	virtual int version() = 0;

	virtual char const * get_data(uint64_t offset) {
		return nullptr;
	}

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
};

template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
struct _impl {
using spec_defs = typename h5ng::spec_defs<SIZE_OF_OFFSET, SIZE_OF_LENGTH>;

using offset_type = typename get_type_for_size<SIZE_OF_OFFSET>::type;
using length_type = typename get_type_for_size<SIZE_OF_LENGTH>::type;


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
		return spec_defs::symbol_table_entry_spec::object_header_address::get(&spec::root_group_symbol_table_entry::get(memory_addr));
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
		return spec_defs::symbol_table_entry_spec::object_header_address::get(&spec::root_group_symbol_table_entry::get(memory_addr));
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
};

struct group_btree_v1 : public file_object {
	using spec = typename spec_defs::b_tree_v1_hdr_spec;

	group_btree_v1(uint8_t * addr) {
		this->memory_addr = addr;
	}

	group_btree_v1(group_btree_v1 const &) = default;

	group_btree_v1 & operator=(group_btree_v1 const & x) = default;

	/* K+1 keys */
	length_type get_key(int i) {
		return *reinterpret_cast<length_type*>(&memory_addr[spec::size+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)]);
	}

	/* K key */
	offset_type get_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[spec::size+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)+SIZE_OF_LENGTH]);
	}

	uint64_t get_depth() {
		return *spec::node_level::get(memory_addr);
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
			maximum_child_node = (node_size() - 10 - n1 - n0) / (record_size() + n1 + n0);
			total_maximum_child_node = total_maximum_child_node_count_0*maximum_child_node;

		}
	}

	// the return value must be temporary, a call of (undef function) can
	// invalidate this pointer.
	shared_ptr<node_type> get_btree_node(uint64_t offset, uint64_t record_count, uint64_t depth) {
		auto x = _node_tree_cache.find(offset);
		if(x != _node_tree_cache.end())
			return x->second;
		return (_node_tree_cache[offset] = make_shared<node_type>(this, &base_addr()+offset, record_count, depth)).get();
	}

};

struct object_v1 : public object {
	using spec = typename spec_defs::object_header_v1_spec;

	uint64_t K;
	shared_ptr<local_heap> lheap;
	offset_type btree_root;

	object_v1(file_impl * file, uint8_t * addr) : object{file, addr} {
		uint64_t msg_count = spec::total_number_of_header_message::get(memory_addr);
		uint8_t * msg = &memory_addr[spec::size];

		while(msg_count > 0) {
			msg = parse_message(msg);
			--msg_count;
		}

	}

	virtual ~object_v1() { }

	uint8_t * parse_message(uint8_t * current_message) {
		uint8_t message_type = spec_defs::message_header_v1_spec::type::get(current_message);
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
				+ spec_defs::message_header_v1_spec::size
				+ spec_defs::message_header_v1_spec::size_of_message::get(current_message);

	}

	shared_ptr<object> find_object(char const * key) {
		auto offset = _find(group_btree_v1(base_addr()+btree_root));
		return file->make_object(offset);
	}

	offset_type _find(group_btree_v1 cur, char const * key) {
		for(int i = 1; i <= K; ++i) {
			char const * xkey = lheap->get_data(cur.get_key(i));
			int cmp = std::strcmp(xkey, key);
			if (cmp <= 0) {
				if (cur.get_depth() == 0) {
					if (cmp == 0)
						return cur.get_node(i-1);
					else
						throw runtime_error("TODO " STR(__LINE__));
				} else {
					return _find(group_btree_v1(), key);
				}
			}
		}
		return 0xfffffffffffffffful;
	}


};


struct object_v2 : public object {
	using spec = typename spec_defs::object_header_v2_spec;

	object_v2(file_impl * file, uint8_t * addr) : object{file, addr} {
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
			return (object_cache[offset] = make_shared<object_v1>(this, &data[offset]));
		} else if (version == 'O') {
			uint32_t sign = *reinterpret_cast<uint32_t*>(&data[offset]);
			if (sign != 0x5244484ful)
				throw runtime_error("TODO " STR(__LINE__));
			version = data[offset+OFFSET_V2_OBJECT_HEADER_VERSION];
			if (version != 2)
				throw runtime_error("TODO " STR(__LINE__));
			return (object_cache[offset] = make_shared<object_v2>(this, &data[offset]));
		}

		throw runtime_error("TODO " STR(__LINE__));
	}


	virtual auto make_btree(uint64_t offset) -> shared_ptr<btree> {
		throw runtime_error("TODO " STR(__LINE__));
	}

	virtual auto make_local_heap(uint64_t offset) -> shared_ptr<local_heap> {
		throw runtime_error("TODO " STR(__LINE__));
	}

	virtual auto make_global_heap(uint64_t offset) -> shared_ptr<global_heap> {
		throw runtime_error("TODO " STR(__LINE__));
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

auto file_object::base_addr() -> uint8_t *
{
	return nullptr;
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


struct h5obj;

struct _h5obj {

	_h5obj() = default;

	virtual ~_h5obj() { }

	_h5obj(_h5obj const &) = delete;
	_h5obj & operator=(_h5obj const &) = delete;

	template<typename T, typename ... ARGS>
	T * read(ARGS ... args) const;

	template<typename T>
	T read_attribute(string const & name) const;

	h5obj operator[](string name) const;

	template<typename T> struct _attr;

	virtual vector<size_t> const & shape() const {
		throw runtime_error("No shape() implemented");
	}

	virtual size_t const & shape(int i) const {
		throw runtime_error("No shape() implemented");
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
	shared_ptr<file_impl> yeach;

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
		yeach = _for_each0<2,4,8>::create(data, version, superblock_offset, size_of_offset, size_of_length);
	}

	virtual ~_h5file() = default;

};


class h5obj {
	shared_ptr<_h5obj> _ptr;

public:

	h5obj(h5obj const & x) = default;
	h5obj & operator=(h5obj const &) = default;
	h5obj(shared_ptr<_h5obj> const & x) : _ptr{x} { }

	h5obj(string const & filename) {
		_ptr = make_shared<_h5file>(filename);
	}

	virtual ~h5obj() = default;


//	template<typename T, typename ... ARGS>
//	T * read(ARGS ... args) const {
//		return _ptr->read<T>(args...);
//	}
//
//	template<typename T>
//	T read_attribute(string const & name) const {
//		return _ptr->read_attribute<T>(name);
//	}
//
//	h5obj operator[](string const & name) const {
//		return _ptr->operator [](name);
//	}
//
//	vector<size_t> const & shape() const {
//		return _ptr->shape();
//	}
//
//	size_t const & shape(int i) const {
//		return _ptr->shape(i);
//	}

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

} // hdf5ng

#endif /* SRC_HDF5_NG_HXX_ */
