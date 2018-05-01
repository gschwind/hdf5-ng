/*
 * h5ng-spec.hxx
 *
 *  Created on: 28 avr. 2018
 *      Author: benoit.gschwind
 */

#ifndef SRC_H5NG_SPEC_HXX_
#define SRC_H5NG_SPEC_HXX_

#include <cstdint>
#include <type_traits>

namespace h5ng {


using namespace std;

struct type_spec {

	struct none;

	// select if T derivate from type_spec, get the size diferently
	template<bool B, typename T>
	struct _sizeof {
		enum : uint64_t { value = sizeof(T) };
	};

	template<typename T>
	struct _sizeof<true, T> {
		enum : uint64_t { value = T::size };
	};

	template<bool B, typename T>
	struct _return {
		using type = T;
	};

	template<typename T>
	struct _return<true, T> {
		using type = uint8_t;
	};

	template<typename T>
	struct last {
		enum : uint64_t { size = T::offset + _sizeof<is_base_of<type_spec, typename T::type>::value, typename T::type>::value };
	};

	template<typename T, typename PREV>
	struct spec {
		enum : bool { prev_is_typespec = std::is_base_of<type_spec, typename PREV::type>::value };
		using type = T;
		enum : uint64_t { offset = PREV::offset + _sizeof<prev_is_typespec, typename PREV::type>::value };

		using return_type = typename _return<is_base_of<type_spec, T>::value, T>::type;
		static return_type & get(uint8_t * addr) {
			return *reinterpret_cast<return_type*>(addr + offset);
		}

	};

	template<typename T>
	struct spec<T, none> {
		using type = T;
		enum : uint64_t { offset = 0ul };

		using return_type = typename _return<std::is_base_of<type_spec, T>::value, T>::type;
		static return_type & get(uint8_t * addr) {
			return *reinterpret_cast<return_type*>(addr + offset);
		}


	};

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
	using type = uint64_t;
};


template<int SIZE_OF_OFFSET, int SIZE_OF_LENGTH>
struct spec_defs {

using offset_type = typename get_type_for_size<SIZE_OF_OFFSET>::type;
using length_type = typename get_type_for_size<SIZE_OF_LENGTH>::type;

struct group_symbol_table_spec : public type_spec {
	using signature                    = spec<uint8_t[4], none>;
	using version                      = spec<uint8_t, signature>;
	using reserved_0                   = spec<uint8_t, version>;
	using number_of_symbols            = spec<uint16_t, reserved_0>;

	enum : uint64_t { size = last<number_of_symbols>::size };

};

struct group_symbol_table_entry_spec : public type_spec {

	using link_name_offset            = spec<offset_type, none>;
	using object_header_address       = spec<offset_type, link_name_offset>;
	using cache_type                  = spec<uint32_t, object_header_address>;
	using reserved_0                  = spec<uint32_t, cache_type>;
	using scratch_pad_space           = spec<uint8_t[16], reserved_0>;

	enum : uint64_t { size = last<scratch_pad_space>::size };

};

// Layout: Object Header Scratch-pad Format
struct object_header_scratch_pad_spec : public type_spec {

	using b_tree_address            = spec<offset_type, none>;
	using local_heap_address        = spec<offset_type, b_tree_address>;

	enum : uint64_t { size = last<local_heap_address>::size };

};

struct superblock_v0_spec : public type_spec {
	using signature                                = spec<uint8_t[8], none>;
	using superblock_version                       = spec<uint8_t, signature>;
	using file_free_space_storage_version          = spec<uint8_t, superblock_version>;
	using root_group_symbol_table_entry_version    = spec<uint8_t, file_free_space_storage_version>;
	using reserved_0                               = spec<uint8_t, root_group_symbol_table_entry_version>;
	using shared_header_message_format_version     = spec<uint8_t, reserved_0>;
	using size_of_offsets                          = spec<uint8_t, shared_header_message_format_version>;
	using size_of_length                           = spec<uint8_t, size_of_offsets>;
	using reserved_1                               = spec<uint8_t, size_of_length>;
	using group_leaf_node_K                        = spec<uint16_t, reserved_1>;
	using group_internal_node_K                    = spec<uint16_t, group_leaf_node_K>;
	using file_consistency_flags                   = spec<uint32_t, group_internal_node_K>;
	using base_address                             = spec<offset_type, file_consistency_flags>;
	using free_space_info_address                  = spec<offset_type, base_address>;
	using end_of_file_address                      = spec<offset_type, free_space_info_address>;
	using driver_information_address               = spec<offset_type, end_of_file_address>;
	using root_group_symbol_table_entry            = spec<group_symbol_table_entry_spec, driver_information_address>;

	enum : uint64_t { size = last<root_group_symbol_table_entry>::size };

};

struct superblock_v1_spec : public type_spec {
	using signature                                = spec<uint8_t[8], none>;
	using superblock_version                       = spec<uint8_t, signature>;
	using file_free_space_storage_version          = spec<uint8_t, superblock_version>;
	using root_group_symbol_table_entry_version    = spec<uint8_t, file_free_space_storage_version>;
	using reserved_0                               = spec<uint8_t, root_group_symbol_table_entry_version>;
	using shared_header_message_format_version     = spec<uint8_t, reserved_0>;
	using size_of_offsets                          = spec<uint8_t, shared_header_message_format_version>;
	using size_of_length                           = spec<uint8_t, size_of_offsets>;
	using reserved_1                               = spec<uint8_t, size_of_length>;
	using group_leaf_node_K                        = spec<uint16_t, reserved_1>;
	using group_internal_node_K                    = spec<uint16_t, group_leaf_node_K>;
	using file_consistency_flags                   = spec<uint32_t, group_internal_node_K>;
	using indexed_storage_internal_node_K          = spec<uint16_t, file_consistency_flags>;
	using reserved_2                               = spec<uint16_t, indexed_storage_internal_node_K>;
	using base_address                             = spec<offset_type, reserved_2>;
	using free_space_info_address                  = spec<offset_type, base_address>;
	using end_of_file_address                      = spec<offset_type, free_space_info_address>;
	using driver_information_address               = spec<offset_type, end_of_file_address>;
	using root_group_symbol_table_entry            = spec<group_symbol_table_entry_spec, driver_information_address>;

	enum : uint64_t { size = last<root_group_symbol_table_entry>::size };

};

struct superblock_v2_spec : public type_spec {
	using signature                                = spec<uint8_t[8], none>;
	using superblock_version                       = spec<uint8_t, signature>;
	using size_of_offsets                          = spec<uint8_t, superblock_version>;
	using size_of_length                           = spec<uint8_t, size_of_offsets>;
	using file_consistency_flags                   = spec<uint8_t, size_of_length>;
	using base_address                             = spec<offset_type, file_consistency_flags>;
	using superblock_extension_address             = spec<offset_type, base_address>;
	using end_of_file_address                      = spec<offset_type, superblock_extension_address>;
	using root_group_object_header_address         = spec<offset_type, end_of_file_address>;
	using superblock_checksum                      = spec<uint32_t, root_group_object_header_address>;

	enum : uint64_t { size = last<superblock_checksum>::size };

};

struct superblock_v3_spec : public superblock_v2_spec { };

struct local_heap_spec : public type_spec {
	using signature                 = spec<uint8_t[4],   none>;
	using version                   = spec<uint8_t,      signature>;
	using reserved0                 = spec<uint8_t[3],   version>;
	using data_segment_size         = spec<length_type,  reserved0>;
	using offset_to_head_free_list  = spec<length_type,  data_segment_size>;
	using data_segment_address      = spec<offset_type,  offset_to_head_free_list>;

	enum : uint64_t { size = last<data_segment_address>::size };

};

struct b_tree_v1_hdr_spec : public type_spec {
	using signature                 = spec<uint8_t[4],    none>;
	using node_type                 = spec<uint8_t,       signature>;
	using node_level                = spec<uint8_t,       node_type>;
	using entries_used              = spec<uint16_t,      node_level>;
	using left_sibling_address      = spec<offset_type,   entries_used>;
	using right_sibling_address     = spec<offset_type,   left_sibling_address>;

	enum : uint64_t { size = last<right_sibling_address>::size };

};

struct b_tree_v2_hdr_spec : public type_spec {
	using signature                            = spec<uint8_t[4],   none>;
	using version                              = spec<uint8_t,      signature>;
	using type                                 = spec<uint8_t,      version>;
	using node_size                            = spec<uint32_t,     type>;
	using record_size                          = spec<uint16_t,     node_size>;
	using depth                                = spec<uint16_t,     record_size>;
	using split_percent                        = spec<uint8_t,      depth>;
	using merge_percent                        = spec<uint8_t,      split_percent>;
	using root_node_address                    = spec<offset_type,  merge_percent>;
	using number_of_records_in_root_node       = spec<uint16_t,     root_node_address>;
	using total_number_of_record_in_b_tree     = spec<length_type,  number_of_records_in_root_node>;
	using checksum                             = spec<uint32_t,     total_number_of_record_in_b_tree>;

	enum : uint64_t { size = last<checksum>::size };

};

struct b_tree_v2_node_spec : public type_spec {
	using signature                     = spec<uint8_t[4],   none>;
	using version                       = spec<uint8_t,      signature>;
	using type                          = spec<uint8_t,      version>;

	// variable size;
} __attribute__((packed));

struct object_header_v1_spec : public type_spec {
	using version                             = spec<uint8_t,  none>;
	using reserved_0                          = spec<uint8_t,  version>;
	using total_number_of_header_message      = spec<uint16_t, reserved_0>;
	using reference_count                     = spec<uint32_t, total_number_of_header_message>;
	using header_size                         = spec<uint32_t, reference_count>;

	enum : uint64_t { size = last<header_size>::size };
};

struct message_header_v1_spec : public type_spec {
	using type                           = spec<uint16_t,    none>;
	using size_of_message                = spec<uint16_t,    type>;
	using flags                          = spec<uint8_t,     size_of_message>;
	using reserved                       = spec<uint8_t[3],  flags>;

	enum : uint64_t { size = last<reserved>::size };
};

struct object_header_v2_spec : public type_spec {
	using signature               = spec<uint8_t[4], none>;
	using version                 = spec<uint8_t, signature>;
	using flags                   = spec<uint8_t, version>;
	// Optional fields
	enum : uint64_t { size = last<flags>::size };
};

struct message_header_v2_spec : public type_spec {
	using type                       = spec<uint8_t,  none>;
	using size_of_message_data       = spec<uint16_t, type>;
	using flags                      = spec<uint8_t,  size_of_message_data>;
	// optional fields
	enum : uint64_t { size = last<flags>::size };
};

// Layout: Dataspace Message - Version 1
struct message_dataspace_spec : public type_spec {
	using version             = spec<uint8_t,     none>;
	using rank                = spec<uint8_t,     version>;
	using flags               = spec<uint8_t,     rank>;
	using reserved_v1         = spec<uint8_t[5],  flags>;
	using reserved_v2         = spec<uint8_t,     flags>;
	// optional fields
	enum : uint64_t { size_v1 = last<reserved_v1>::size };
	enum : uint64_t { size_v2 = last<reserved_v2>::size };
};

// Layout: Dataspace Message - Version 1&2
struct message_datatype_spec : public type_spec {
	using class_and_version   = spec<uint8_t,     none>;
	using class_bit_fields    = spec<uint8_t[3],  class_and_version>;
	using size_of_elements    = spec<uint32_t,    class_bit_fields>;
	// optional fields
	enum : uint64_t { size = last<size_of_elements>::size };
};

// Layout: Fill Value Message (Old)
// This is never writen by hdf5 library above 1.6
struct message_fillvalue_old_spec : public type_spec {
	using size_of_fillvalue   = spec<uint32_t,     none>; // should match datatype size_of_elements
	// optional fields
	enum : uint64_t { size = last<size_of_fillvalue>::size };
};

// Layout: Symbol Table Message #0x0011
struct message_symbole_table_spec : public type_spec {
	using b_tree_v1_address        = spec<offset_type,  none>;
	using local_heap_address       = spec<offset_type, b_tree_v1_address>;
	// optional fields
	enum : uint64_t { size = last<local_heap_address>::size };
};

// Layout: Version 2 B-tree, Type 1 Record Layout - Indirectly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type1 : public type_spec {
	using address              = spec<offset_type,   none>;
	using length               = spec<length_type,   address>;
	using id                   = spec<length_type,   length>;

	enum : uint64_t { size = last<id>::size };

};

// Layout: Version 2 B-tree, Type 2 Record Layout - Indirectly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type2 : public type_spec {
	using address              = spec<offset_type,   none>;
	using length               = spec<length_type,   address>;
	using filter_mask          = spec<uint32_t,      length>;
	using memory_size          = spec<length_type,   filter_mask>;
	using id                   = spec<length_type,   memory_size>;

	enum : uint64_t { size = last<id>::size };

};

// Layout: Version 2 B-tree, Type 3 Record Layout - Directly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type3 : public type_spec {
	using address              = spec<offset_type,   none>;
	using length               = spec<length_type,   address>;

	enum : uint64_t { size = last<length>::size };

};

// Layout: Version 2 B-tree, Type 4 Record Layout - Directly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
struct btree_v2_record_type4 : public type_spec {
	using address              = spec<offset_type,   none>;
	using length               = spec<length_type,   address>;
	using filter_mask          = spec<uint32_t,      length>;
	using memory_size          = spec<length_type,   filter_mask>;

	enum : uint64_t { size = last<memory_size>::size };

};

// Layout: Version 2 B-tree, Type 5 Record Layout - Link Name for Indexed Group
struct btree_v2_record_type5 : public type_spec {
	using hash              = spec<uint32_t,   none>;
	using id                = spec<uint8_t[7], hash>;

	enum : uint64_t { size = last<id>::size };

};

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




}; // spec_defs

} // namespace h5ng




#endif /* SRC_H5NG_SPEC_HXX_ */
