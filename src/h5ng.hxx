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
#include <bitset>
#include <iostream>
#include <iomanip>

#include <cstring>
#include <cstdio>

#include "exception.hxx"

namespace h5ng {

using namespace std;

class h5obj;

struct chunk_desc_t {
	uint32_t size_of_chunk;
	uint32_t filters;
	uint64_t address;
	chunk_desc_t (uint32_t size_of_chunk, uint32_t filters, uint64_t address) : size_of_chunk{size_of_chunk}, filters{filters}, address{address} { }
	chunk_desc_t(chunk_desc_t const &) = default;
	chunk_desc_t & operator=(chunk_desc_t const &) = default;
};

// Must be large enough to handle all offset and length type of any impl.
using max_offset_type = uint64_t;
using max_length_type = uint64_t;

struct object_message_handler_t {
	uint16_t type;   //< actual type
	uint16_t size;   //< size of message in the current message block, not the actual size, may be shared or padded
	bitset<8> flags; //< flags of current messages
	uint8_t * data;  //< pointer to message data

	friend ostream & operator<<(ostream & o,  object_message_handler_t const & msg);

};


struct object_symbol_table_t {
	max_offset_type local_heap;
	max_offset_type group_btree_v1_root;
};

// Attributes info
struct object_attribute_info_t {
	uint16_t maximum_creation_index;
	max_offset_type fractal_heap_address;
	max_offset_type attribute_name_btree_address;
	max_offset_type attribute_creation_order_btree_address;
};


struct object_dataspace_t {
	uint8_t rank;
	vector<uint64_t> shape;
	vector<uint64_t> max_shape;
	vector<uint64_t> permutation; // Never implement in official lib.

	friend ostream & operator<<(ostream & o,  object_dataspace_t const & dataspace);

};

struct object_datalayout_t {

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
	max_offset_type   compact_data_address;
	uint32_t          compact_data_size;
	vector<uint32_t>  compact_shape;               //< same as dataspace.shape

	// CONTIGUOUS LAYOUT
	uint8_t           contiguous_dimensionality;   //< same as dataspace.rank+1
	max_offset_type   contiguous_data_address;
	vector<uint32_t>  contiguous_shape;            //< same as dataspace.shape
	max_length_type   contiguous_data_size;

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
			max_offset_type       data_address;
		} chunk_btree_v1;

		// Single Chunk data
		struct {
			max_length_type   size_of_filtered_chunk;
			uint32_t          filters;
			max_offset_type   data_address;
		} chunk_single_chunk;

		struct {
			max_offset_type   data_address;
		} chunk_implicit;

		// Fixed Array data
		struct {
			uint8_t           page_bits;
			max_offset_type   data_address;
		} chunk_fixed_array;

		// Extensible Array data
		struct {
			uint8_t           max_bits;
			uint8_t           index_elements;
			uint8_t           min_pointers;
			uint8_t           min_elements;
			uint16_t          page_bits;
			max_offset_type   data_address;
		} chunk_extensible_array;

		// B-Tree V2 data
		struct {
			uint32_t          node_size;
			uint8_t           split_percent;
			uint8_t           merge_percent;
			max_offset_type   data_address;
		} chunk_btree_v2;

	};

	friend ostream & operator<<(ostream & o, object_datalayout_t const & datalayout);

};

struct object_datatype_t {
	uint8_t version;
	uint8_t xclass;
	bitset<32> flags;
	uint32_t size_of_elements;

	enum : uint8_t {
		CLASS_FIXED_POINT            = 0u,
		CLASS_FLOATING_POINT         = 1u,
		CLASS_TIME                   = 2u,
		CLASS_STRING                 = 3u,
		CLASS_BITFIELD               = 4u,
		CLASS_OPAQUE                 = 5u,
		CLASS_COMPOUND               = 6u,
		CLASS_REFERENCE              = 7u,
		CLASS_ENUMERATED             = 8u,
		CLASS_VARIABLE_LEGNTH        = 9u,
		CLASS_ARRAY                  = 10u
	};

	friend ostream & operator<<(ostream & o, object_datatype_t const & datatype);

};


struct object_link_info_t {
	bitset<8> flags;
	uint64_t maximum_creation_index;
	max_offset_type fractal_head_address;
	max_offset_type name_index_b_tree_address;
	max_offset_type creation_order_index_address;

	friend ostream & operator<<(ostream & o, object_link_info_t const & link_info);

};

struct object_data_storage_filter_pipeline_t {

	struct filter {
		uint16_t id;
		string name;
		bitset<16> flags;
		vector<uint32_t> params;

		filter(uint16_t id, string name, bitset<16> flags, vector<uint32_t> const & params) :
			id{id}, name{name}, flags{flags}, params{params}
		{

		}

		friend ostream & operator<< (ostream & o, filter const & f);

	};

	vector<filter> filters;

	friend ostream & operator<< (ostream & o, object_data_storage_filter_pipeline_t const & f);

};

struct object_fill_value_t {
	vector<uint8_t> value;

	friend ostream & operator<< (ostream & o, object_fill_value_t const & f);

};

struct object_data_storage_fill_value_t {
	bitset<8> flags;
	vector<uint8_t> value;

	bool has_fillvalue() const
	{
		return flags.test(5);
	}

	friend ostream & operator<< (ostream & o, object_data_storage_fill_value_t const & f);

};


struct object_link_t {
	uint8_t type;
	string name;

	max_offset_type offset;
	string soft_link_value;

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
