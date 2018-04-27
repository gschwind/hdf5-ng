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

#include <map>
#include <string>
#include <iostream>
#include <stdexcept>
#include <memory>
#include <typeinfo>
#include <vector>
#include <list>

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


/*****************************************************
 * HIGH LEVEL API
 *****************************************************/

struct file_impl;

struct file_object {
	file_impl * file;
	uint8_t *  memory_addr;
	uint64_t   file_offset;
	uint64_t   size;

	virtual ~file_object() { }

	uint8_t * base_addr() {
		return memory_addr-file_offset;
	}

};

struct superblock : public file_object {

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
	UNDEF
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
	enum object_type_e type; // store the guessed real type.

	// dataset message
	vector<uint64_t> shape;
	vector<uint64_t> shape_max;
	vector<uint64_t> permutation; // never been implemented.

	dataset_layout_e dset_layout;
	uint64_t data_offset;

	// link info message
	uint64_t maximum_creation_index;
	uint64_t fractal_heap;
	uint64_t btree_v2_address;
	uint64_t create_order_btree_address;

	// Datatype
	uint64_t type_size;

	// fillvalue
	vector<uint8_t> fillvalue_data;

	// Link Messages
	list<void> link_list;

	// DataStorage
	// TODO

	// Data layout
	// TODO

	// Group info message
	// TODO

	// Filter pipe line
	// Only used for chunked dataset.

	// Attrubute message
	map<string, vector<uint8_t>> attributes;

	// Comments
	// TODO

	// object modification time



	virtual ~object() = default;
	virtual int version() = 0;

};

struct local_heap : public file_object {

	virtual ~local_heap() = default;
	virtual int version() = 0;

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

template<int I>
struct get_type_for_size;

template<>
struct get_type_for_size<2> {
	using type = uint16_t;
};

template<>
struct get_type_for_size<4> {
	using type = uint32_t;
};

template<>
struct get_type_for_size<8> {
	using type = uint32_t;
};


template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
struct _impl {
using offset_type = typename get_type_for_size<SIZE_OF_OFFSET>::type;
using length_type = typename get_type_for_size<SIZE_OF_LENGTH>::type;

struct symbol_table_entry {
	offset_type link_name_offset;
	offset_type object_header_address;
	uint32_t cache_type;
	uint32_t reserved_0;
	uint8_t scratch_pad_space[16];
} __attribute__((packed));

struct superblock_v0_part0 {
	uint8_t signature[8];
	uint8_t superblock_version;
	uint8_t file_free_space_storage_version;
	uint8_t root_group_symbol_table_entry_version;
	uint8_t reserved_0;
	uint8_t shared_header_message_format_version;
	uint8_t size_of_offsets;
	uint8_t size_of_length;
	uint8_t reserved_1;
	uint16_t group_leaf_node_K;
	uint16_t group_internal_node_K;
	uint32_t file_consistency_flags;
} __attribute__((packed));


struct superblock_v0_part1
{
	offset_type base_address;
	offset_type free_space_info_address;
	offset_type end_of_file_address;
	offset_type driver_information_address;
	symbol_table_entry root_group_symbol_table_entry;
} __attribute__((packed));

struct superblock_v0_raw :
		public superblock_v0_part0,
		public superblock_v0_part1
{

} __attribute__((packed));

using superblock_v1_part0 = superblock_v0_part0;

struct superblock_v1_part1
{
	uint16_t indexed_storage_internal_node_K;
	uint16_t reserved_2;
} __attribute__((packed));

using superblock_v1_part2 = superblock_v0_part1;

struct superblock_v1_raw :
		public superblock_v1_part0,
		public superblock_v1_part1,
		public superblock_v1_part2
{

} __attribute__((packed));

struct superblock_v2_raw {
	uint8_t signature[8];
	uint8_t superblock_version;
	uint8_t size_of_offsets;
	uint8_t size_of_length;
	uint8_t file_consistency_flags;
	offset_type base_address;
	offset_type superblock_extension_address;
	offset_type end_of_file_address;
	offset_type root_group_object_header_address;
	uint32_t superblock_checksum;
} __attribute__((packed));

using superblock_v3_raw = superblock_v2_raw;

struct b_tree_v1_part0 {
	uint8_t signature[4];
	uint8_t node_type;
	uint8_t node_level;
	uint16_t entries_used;
	offset_type left_sibling_address;
	offset_type right_sibling_address;
	// TODO
} __attribute__((packed));

struct b_tree_v2_header {
	uint8_t signature[4];
	uint8_t version;
	uint8_t type;
	uint32_t node_size;
	uint16_t record_size;
	uint16_t depth;
	uint8_t split_percent;
	uint8_t merge_percent;
	offset_type root_node_address;
	uint16_t number_of_records_in_root_node;
	length_type total_number_of_record_in_b_tree;
	uint32_t checksum;
} __attribute__((packed));

struct b_tree_v2_node {
	uint8_t signature[4];
	uint8_t version;
	uint8_t type;
} __attribute__((packed));


struct object_header_v1 {
	uint8_t version;
	uint8_t reserved_0;
	uint16_t total_number_of_header_message;
	uint32_t reference_count;
	uint32_t header_size;
} __attribute__((packed));

struct message_header_v1 {
	uint16_t type;
	uint16_t size_of_message;
	uint8_t flags;
	uint8_t reserved[3];
} __attribute__((packed));

struct object_header_v2 {
	uint32_t signature;
	uint8_t version;
	uint8_t flags;
	// Optional fields
} __attribute__((packed));

struct message_header_v2 {
	uint8_t type;
	uint16_t size_of_message_data;
	uint8_t flags;
	// optional fields
};

struct local_heap_raw {
	uint8_t signature[4];
	uint8_t version;
	uint8_t reserved[3];
	length_type data_segment_size;
	length_type offset_to_head_free_list;
	offset_type data_segment_address;
};


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

	superblock_v0_raw * _data() {
		return reinterpret_cast<superblock_v0_raw*>(memory_addr);
	}

	superblock_v0(uint8_t * data) { }

	virtual int version() {
		return _data()->superblock_version;
	}

	virtual int offset_size() {
		return _data()->size_of_offsets;
	}

	virtual int length_size() {
		return _data()->size_of_length;
	}

	virtual int group_leaf_node_K() {
		return _data()->group_leaf_node_K;
	}

	virtual int group_internal_node_K() {
		return _data()->group_internal_node_K;
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual int object_version() {
		return -1;
	}

	virtual uint64_t base_address() {
		return _data()->base_address;
	}

	virtual uint64_t file_free_space_info_address() {
		return _data()->base_address;
	}
	virtual uint64_t end_of_file_address() {
		return _data()->end_of_file_address;
	}

	virtual uint64_t driver_information_address() {
		return _data()->driver_information_address;
	}

	virtual uint64_t root_node_object_address() {
		return _data()->root_group_symbol_table_entry.object_header_address;
	}

};

struct superblock_v1 : public superblock {
	superblock_v1(uint8_t * data) { this->memory_addr = data; }

	superblock_v1_raw * _data() {
		return reinterpret_cast<superblock_v1_raw*>(memory_addr);
	}

	virtual int version() {
		return _data()->superblock_version;
	}

	virtual int offset_size() {
		return _data()->size_of_offsets;
	}

	virtual int length_size() {
		return _data()->size_of_length;
	}

	virtual int group_leaf_node_K() {
		return _data()->group_leaf_node_K;
	}

	virtual int group_internal_node_K() {
		return _data()->group_internal_node_K;
	}

	virtual int indexed_storage_internal_node_K() {
		return -1;
	}

	virtual uint64_t base_address() {
		return _data()->base_address;
	}

	virtual uint64_t file_free_space_info_address() {
		return _data()->base_address;
	}
	virtual uint64_t end_of_file_address() {
		return _data()->end_of_file_address;
	}

	virtual uint64_t driver_information_address() {
		return _data()->driver_information_address;
	}

	virtual uint64_t root_node_object_address() {
		return _data()->root_group_symbol_table_entry.object_header_address;
	}

};

struct superblock_v2 : public superblock {
	superblock_v2(uint8_t * data) { this->memory_addr = data; }

	superblock_v2_raw * _data() {
		return reinterpret_cast<superblock_v2_raw*>(memory_addr);
	}

	virtual int version() {
		return _data()->superblock_version;
	}

	virtual int offset_size() {
		return _data()->size_of_offsets;
	}

	virtual int length_size() {
		return _data()->size_of_length;
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

	virtual uint64_t base_address() {
		return _data()->base_address;
	}

	virtual uint64_t file_free_space_info_address() {
		return _data()->base_address;
	}
	virtual uint64_t end_of_file_address() {
		return _data()->end_of_file_address;
	}

	virtual uint64_t driver_information_address() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual uint64_t root_node_object_address() {
		return _data()->root_group_object_header_address;
	}
};

struct superblock_v3 : public superblock {
	superblock_v3(uint8_t * data) {
		memory_addr = data;
		size = sizeof(superblock_v3_raw);
	}

	superblock_v3_raw * _data() {
		return reinterpret_cast<superblock_v3_raw*>(memory_addr);
	}

	virtual int version() {
		return _data()->superblock_version;
	}

	virtual int offset_size() {
		return _data()->size_of_offsets;
	}

	virtual int length_size() {
		return _data()->size_of_length;
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

	virtual uint64_t base_address() {
		return _data()->base_address;
	}

	virtual uint64_t file_free_space_info_address() {
		return _data()->base_address;
	}
	virtual uint64_t end_of_file_address() {
		return _data()->end_of_file_address;
	}

	virtual uint64_t driver_information_address() {
		throw runtime_error("TODO" STR(__LINE__));
	}

	virtual uint64_t root_node_object_address() {
		return _data()->root_group_object_header_address;
	}
};

struct group_btree_v1 : public file_object {

	group_btree_v1(uint8_t * addr) {
		this->memory_addr = addr;
	}

	group_btree_v1(group_btree_v1 const &) = default;

	group_btree_v1 & operator=(group_btree_v1 const & x) = default;

	b_tree_v1_part0 * _data() {
		return reinterpret_cast<b_tree_v1_part0*>(memory_addr);
	}

	/* K+1 keys */
	length_type get_key(int i) {
		return *reinterpret_cast<length_type*>(&memory_addr[sizeof(b_tree_v1_part0)+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)]);
	}

	/* K key */
	offset_type get_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[sizeof(b_tree_v1_part0)+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)+SIZE_OF_LENGTH]);
	}

	uint64_t get_depth() {
		return _data()->node_level;
	}

};

template<typename record_type>
struct group_btree_v2_node;


struct group_btree_v2 : public file_object {

	map<uint64_t, shared_ptr<group_btree_v2_node>> _node_tree_cache;

	group_btree_v2(uint8_t * addr) {
		this->memory_addr = addr;
	}

	group_btree_v2(group_btree_v2 const &) = default;

	group_btree_v2 & operator=(group_btree_v2 const & x) = default;

	b_tree_v2_header * _data() {
		return reinterpret_cast<b_tree_v2_header*>(memory_addr);
	}

	/* K+1 keys */
	length_type get_key(int i) {
		return *reinterpret_cast<length_type*>(&memory_addr[sizeof(b_tree_v1_part0)+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)]);
	}

	/* K key */
	offset_type get_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[sizeof(b_tree_v1_part0)+i*(SIZE_OF_OFFSET+SIZE_OF_LENGTH)+SIZE_OF_LENGTH]);
	}

	uint64_t get_depth() {
		return _data()->depth;
	}

	uint64_t record_size() {
		return _data()->record_size;
	}

	uint64_t node_size() {
		return _data()->node_size;
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
	group_btree_v2_node * get_btree_node(uint64_t offset, uint64_t record_count, uint64_t depth) {
		auto x = _node_tree_cache.find(offset);
		if(x != _node_tree_cache.end())
			return x->second.get();
		return (_node_tree_cache[offset] = make_shared<group_btree_v2_node>(this, &base_addr()+offset, record_count, depth)).get();
	}

};

// Layout: Version 2 B-tree, Type 1 Record Layout - Indirectly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type1 {
	offset_type address;
	length_type length;
	length_type id;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 2 Record Layout - Indirectly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type2 {
	offset_type address;
	length_type length;
	uint32_t filter_mask;
	length_type memory_size;
	length_type id;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 3 Record Layout - Directly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type3 {
	offset_type address;
	length_type length;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 4 Record Layout - Directly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type4 {
	offset_type address;
	length_type length;
	uint32_t filter_mask;
	length_type memory_size;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 5 Record Layout - Link Name for Indexed Group
struct btree_v2_record_type5 {
	uint32_t hash;
	uint8_t id[7];
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 6 Record Layout - Creation Order for Indexed Group
struct btree_v2_record_type6 {
	uint64_t creation_order;
	uint8_t id[7];
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 7 Record Layout - Shared Object Header Messages (Sub-type 0 - Message in Heap)
struct btree_v2_record_type7_sub_type0 {
	uint8_t message_location;
	uint32_t hash;
	uint32_t reference_count;
	uint64_t heap_id;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 7 Record Layout - Shared Object Header Messages (Sub-type 1 - Message in Object Header)
struct btree_v2_record_type7_sub_type1 {
	uint8_t message_location;
	uint32_t hash;
	uint8_t reserved0;
	uint8_t message_type;
	uint16_t object_header_index;
	uint64_t object_header_addr;
} __attribute__((packed));


// Layout: Version 2 B-tree, Type 8 Record Layout - Attribute Name for Indexed Attributes
struct btree_v2_record_type8 {
	uint64_t heap_id;
	uint8_t message_flags;
	uint32_t creation_order;
	uint32_t hash_name;
} __attribute__((packed));


// Layout: Version 2 B-tree, Type 9 Record Layout - Creation Order for Indexed Attributes
struct btree_v2_record_type9 {
	uint64_t heap_id;
	uint8_t message_flags;
	uint32_t creation_order;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 10 Record Layout - Non-filtered Dataset Chunks
struct btree_v2_record_type10 {
	uint64_t addr;
	// variable data;
} __attribute__((packed));

// Layout: Version 2 B-tree, Type 11 Record Layout - Filtered Dataset Chunks
struct btree_v2_record_type11 {
	uint64_t addr;
	// variable data;
} __attribute__((packed));

template<typename record_type>
struct group_btree_v2_node {
	group_btree_v2 * root;
	uint8_t * memory_addr;
	uint64_t record_count;
	uint64_t depth;

	uint64_t size_of_number_of_child_node;
	uint64_t size_of_total_number_of_child_node;

	uint64_t (*read_number_of_child_node)(uint8_t * addr);
	uint64_t (*read_total_number_of_child_node)(uint8_t * addr);


	b_tree_v2_node * _data() {
		return reinterpret_cast<b_tree_v2_node*>(memory_addr);
	}

	group_btree_v2_node(group_btree_v2 * root, uint8_t * memory_addr, uint64_t record_count, uint64_t depth) :
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

		switch(size_of_number_of_child_node) {
		case 1:
			read_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint8_t*>(addr); };
			break;
		case 2:
			read_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint16_t*>(addr); };
			break;
		case 4:
			read_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint32_t*>(addr); };
			break;
		case 8:
			read_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint64_t*>(addr); };
			break;
		default:
			read_number_of_child_node = nullptr;
		}

		switch(size_of_total_number_of_child_node) {
		case 1:
			read_total_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint8_t*>(addr); };
			break;
		case 2:
			read_total_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint16_t*>(addr); };
			break;
		case 4:
			read_total_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint32_t*>(addr); };
			break;
		case 8:
			read_total_number_of_child_node = [](uint8_t * addr) -> uint64_t { return *reinterpret_cast<uint64_t*>(addr); };
			break;
		default:
			read_total_number_of_child_node = nullptr;
		}

	}

	/* K+1 keys */
	uint8_t * get_record(int i) {
		return memory_addr[sizeof(b_tree_v2_node)+i*(root->record_size())];
	}

	/* K key */
	offset_type get_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[sizeof(b_tree_v2_node)+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)]);
	}

	uint64_t get_number_of_child_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[sizeof(b_tree_v2_node)+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)+root->record_size()]);
	}

	uint64_t get_total_number_of_child_node(int i) {
		return *reinterpret_cast<offset_type*>(&memory_addr[sizeof(b_tree_v2_node)+i*(root->record_size()+size_of_number_of_child_node+size_of_total_number_of_child_node)+root->record_size()+size_of_number_of_child_node]);
	}

};

struct local_heap : public file_object {
	uint8_t * get_data(uint64_t offset) {
		return memory_addr[offset];
	}
};

struct object_v1 : public file_object {
	uint64_t K;
	shared_ptr<local_heap> lheap;
	offset_type btree_root;

	uint8_t * parse_message(uint8_t * current_message) {
		//uint8_t * current_message = &memory_addr[sizeof(object_header_v1)];
		auto message_header = reinterpret_cast<message_header_v1*>(current_message);

		switch(message_header->type) {
		//TODO: dispatch type
		}

		// next message
		return current_message + sizeof(message_header_v1) + message_header->size;
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

	virtual auto make_superblock(uint64_t offset) -> shared_ptr<superblock> {
		auto x = superblock_cache.find(offset);
		if (x != superblock_cache.end())
			return dynamic_pointer_cast<superblock>(x->second);

		shared_ptr<superblock> ret;
		switch(version) {
		case 0:
			ret = make_shared<superblock_v0>(&data[offset]);
			break;
		case 1:
			ret = make_shared<superblock_v1>(&data[offset]);
			break;
		case 2:
			ret = make_shared<superblock_v2>(&data[offset]);
			break;
		case 3:
			ret = make_shared<superblock_v3>(&data[offset]);
			break;
		}
		if(not ret)
			throw runtime_error("TODO" STR(__LINE__));
		superblock_cache[offset] = ret;
		return ret;
	}

	shared_ptr<superblock> get_superblock() {
		return make_superblock(superblock_offset);
	}

	virtual auto make_object(uint64_t offset) -> shared_ptr<object> {
		auto x = object_cache.find(offset);
		if (x != object_cache.end())
			return dynamic_pointer_cast<object>(x->second);

		int version = data[offset+OFFSET_V1_OBJECT_HEADER_VERSION];
		if (version == 1) {
			// TODO v1
		} else if (version == 'O') {
			uint32_t sign = reinterpret_cast<uint32_t*>(&data[offset]);
			if (sign != 0x5244484ful)
				throw runtime_error("TODO " STR(__LINE__));
			version = data[offset+OFFSET_V2_OBJECT_HEADER_VERSION];
			if (version != 2)
				throw runtime_error("TODO " STR(__LINE__));
			// TODO v2
		}
		return ret;
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

};



};

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

	_h5obj();
	_h5obj(H5O_type_t const & type, hid_t id);

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
	_h5dset(_h5dset const &) = delete;
	_h5dset & operator=(_h5dset const &) = delete;

	_h5dset(hid_t id, hid_t parent, string const & dsetname);

	virtual ~_h5dset();

	virtual vector<size_t> const & shape() const;
	virtual size_t const & shape(int i) const;

	template<typename T, typename ... ARGS>
	T * read(ARGS ... args) const;

	template<typename T>
	vector<T> read_attribute(string const & name) const;
};

struct _h5group : public _h5obj {
	_h5group(_h5group const &) = default;
	_h5group & operator=(_h5group const &) = default;
	_h5group(hid_t id, hid_t parent, string const & name);
	virtual ~_h5group();
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

	virtual ~_h5file();
};


class h5obj {
	shared_ptr<_h5obj> _ptr;

public:

	h5obj(h5obj const & x) = default;
	h5obj & operator=(h5obj const &) = default;
	h5obj(shared_ptr<_h5obj> const & x) : _ptr{x} { }

	h5obj(string const & filename);
	~h5obj();

	template<typename T, typename ... ARGS>
	T * read(ARGS ... args) const {
		return _ptr->read<T>(args...);
	}

	template<typename T>
	T read_attribute(string const & name) const {
		return _ptr->read_attribute<T>(name);
	}

	h5obj operator[](string const & name) const {
		return _ptr->operator [](name);
	}

	vector<size_t> const & shape() const {
		return _ptr->shape();
	}

	size_t const & shape(int i) const {
		return _ptr->shape(i);
	}

};

template<typename T>
struct _h5obj::_attr {
	static T read(_h5obj const & obj, string const & name) {
		hid_t attr = H5Aopen(obj.id, name.c_str(), H5P_DEFAULT);
		if(attr < 0)
			throw runtime_error("invalid attribute");
		hid_t aspc = H5Aget_space(attr);
		hsize_t size = H5Aget_storage_size(attr);
		/* TODO: check for type matching */
		if ((size%sizeof(T)) != 0)
			throw runtime_error("invalid attribute type or size");
		vector<T> ret{size/sizeof(T), vector<char>::allocator_type()};
		H5Aread(attr, h5type<T>::type(), &ret[0]);
		return ret[0];
	}
};

} // hdf5ng

#endif /* SRC_HDF5_NG_HXX_ */
