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

#include <map>
#include <string>
#include <iostream>
#include <stdexcept>
#include <memory>
#include <typeinfo>

#define STR(x) #x

namespace hdf5ng {

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

struct file_object {
	uint8_t * data;
	uint64_t offset;
	uint64_t size;

	virtual ~file_object() { }

};

struct superblock : public file_object {

	virtual ~superblock() = default;

	virtual int version() = 0;
	virtual int offset_size() = 0;
	virtual int length_size() = 0;
	virtual int group_leaf_node_K() = 0;
	virtual int group_internal_node_K() = 0;
	virtual int indexed_storage_internal_node_K() = 0;
	virtual uint64_t base_address() = 0;
	virtual uint64_t file_free_space_info_address() = 0;
	virtual uint64_t end_of_file_address() = 0;
	virtual uint64_t driver_information_address() = 0;
	virtual uint64_t root_node_object_address() = 0;

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

//struct map<uint64_t, weak_ptr<file_object>> cache;

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

struct b_tree_v2_part0 {
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



struct superblock_v0 : public superblock {

	superblock_v0_raw * _data() {
		return reinterpret_cast<superblock_v0_raw*>(data);
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
	superblock_v1(uint8_t * data) { this->data = data; }

	superblock_v1_raw * _data() {
		return reinterpret_cast<superblock_v1_raw*>(data);
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
	superblock_v2(uint8_t * data) { this->data = data; }

	superblock_v2_raw * _data() {
		return reinterpret_cast<superblock_v2_raw*>(data);
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
	superblock_v3(uint8_t * data) { this->data = data; }

	superblock_v3_raw * _data() {
		return reinterpret_cast<superblock_v3_raw*>(data);
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

static shared_ptr<superblock> make_superblock(uint8_t * data, int version) {
	switch(version) {
	case 0:
		return make_shared<superblock_v0>(data);
	case 1:
		return make_shared<superblock_v1>(data);
	case 2:
		return make_shared<superblock_v2>(data);
	case 3:
		return make_shared<superblock_v3>(data);
	}

	throw runtime_error("TODO" STR(__LINE__));
}

};

struct superblock;

template<int I, int J>
struct xsuperblock;

template<int ... ARGS>
struct _for_each1;

template<int J, int I, int ... ARGS>
struct _for_each1<J, I, ARGS...> {
	static shared_ptr<superblock> create(uint8_t * data, int version, int size_of_length) {
		if (I == size_of_length) {
			return _impl<J, I>::make_superblock(data, version);
		} else {
			return _for_each1<J, ARGS...>::create(data, version, size_of_length);
		}
	}
};

template<int J>
struct _for_each1<J> {
	static shared_ptr<superblock> create(uint8_t * data, int version, int size_of_length) {
		throw runtime_error("TODO" STR(__LINE__));
	}
};

template<int ... ARGS>
struct _for_each0;

template<int J, int ... ARGS>
struct _for_each0<J, ARGS...> {
	static shared_ptr<superblock> create(uint8_t * data, int version, int size_of_offset, int size_of_length) {
		if (J == size_of_offset) {
			return _for_each1<J,2,4,8>::create(data, version, size_of_length);
		} else {
			return _for_each0<ARGS...>::create(data, version, size_of_offset, size_of_length);
		}
	}
};

template<>
struct _for_each0<> {
	static shared_ptr<superblock> create(uint8_t * data, int version, int size_of_offset, int size_of_length) {
		throw runtime_error("TODO" STR(__LINE__));
	}
};



struct file {
	string filename;
	int fd;
	uint8_t * data;
	uint64_t file_size;

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



	file(string const & filename) : filename{filename} {
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
		 * 2, 4, 8, 16 or 32. our implementation is limited to 8 bytes, uint64_t
		 */
		shared_ptr<superblock> yeach = _for_each0<2,4,8>::create(&data[superblock_offset], version, size_of_offset, size_of_length);

	}

};

} // hdf5ng

#endif /* SRC_HDF5_NG_HXX_ */
