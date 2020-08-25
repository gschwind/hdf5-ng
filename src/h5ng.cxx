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


ostream & operator<<(ostream & o,  object_message_handler_t const & msg)
{
	return o << "<object_message_handler_t type=0x" << std::hex << std::setw(4) << std::setfill('0') << msg.type
			<< ", size=" << msg.size
			<< ", flags=0b" << msg.flags
			<< ", data=" << reinterpret_cast<void*>(msg.data) << ">";
}


ostream & operator<<(ostream & o,  object_dataspace_t const & dataspace)
{
	o << "dataspace.rank = " << static_cast<int>(dataspace.rank) << endl;
	o << "dataspace.shape = " << dataspace.shape << endl;
	o << "dataspace.max_shape = " << dataspace.max_shape << endl;
	o << "dataspace.permutation = " << dataspace.permutation << endl;

	return o;
}


ostream & operator<<(ostream & o, object_datalayout_t const & datalayout)
{
	using tns = object_datalayout_t;

	o << "datalayout.version = " << static_cast<int>(datalayout.version) << endl;

	switch(datalayout.layout_class) {
	case tns::LAYOUT_COMPACT: {
		o << "datalayout.layout_class = COMPACT" << endl;
		o << "datalayout.compact_dimensionality = " << static_cast<int>(datalayout.compact_dimensionality) << endl;
		o << "datalayout.compact_data_address = " << datalayout.compact_data_address << endl;
		o << "datalayout.compact_data_size = " << datalayout.compact_data_size << endl;
		o << "datalayout.compact_shape = " << datalayout.compact_shape << endl;
		 break;
	}
	case tns::LAYOUT_CONTIGUOUS: {
		o << "datalayout.layout_class = CONTIGUOUS" << endl;
		o << "datalayout.contiguous_dimensionality = " << static_cast<int>(datalayout.contiguous_dimensionality) << endl;
		o << "datalayout.contiguous_data_address = " << datalayout.contiguous_data_address << endl;
		o << "datalayout.contiguous_shape = " << datalayout.contiguous_shape << endl;
		o << "datalayout.contiguous_data_size = " << datalayout.contiguous_data_size << endl;
		break;
	}
	case tns::LAYOUT_CHUNKED: {
		o << "datalayout.layout_class = CHUNKED" << endl;
		o << "datalayout.chunk_flags = 0b" << make_bitset(datalayout.chunk_flags) << endl;
		o << "datalayout.chunk_dimensionality = " << static_cast<int>(datalayout.chunk_dimensionality) << endl;
		o << "datalayout.chunk_shape = " << datalayout.chunk_shape << endl;
		o << "datalayout.chunk_size_of_element = " << datalayout.chunk_size_of_element << endl;

		switch (datalayout.chunk_indexing_type) {
			case tns::CHUNK_INDEXING_BTREE_V1:
				o << "datalayout.chunk_indexing_type = BTREE_V1" << endl;
				o << "datalayout.chunk_btree_v1.data_address = " << datalayout.chunk_btree_v1.data_address << endl;
				break;
			case tns::CHUNK_INDEXING_SINGLE_CHUNK:
				o << "datalayout.chunk_indexing_type = SINGLE_CHUNK" << endl;
				o << "datalayout.chunk_single_chunk.size_of_filtered_chunk = " << datalayout.chunk_single_chunk.size_of_filtered_chunk << endl;
				o << "datalayout.chunk_single_chunk.filters = " << datalayout.chunk_single_chunk.filters << endl;
				o << "datalayout.chunk_single_chunk.data_address = " << datalayout.chunk_single_chunk.data_address << endl;
				break;
			case tns::CHUNK_INDEXING_IMPLICIT:
				o << "datalayout.chunk_indexing_type = IMPLICIT" << endl;
				o << "datalayout.chunk_implicit.data_address = " << datalayout.chunk_implicit.data_address << endl;
				break;
			case tns::CHUNK_INDEXING_FIXED_ARRAY:
				o << "datalayout.chunk_indexing_type = FIXED_ARRAY" << endl;
				o << "datalayout.chunk_fixed_array.page_bits = " << datalayout.chunk_fixed_array.page_bits << endl;
				o << "datalayout.chunk_fixed_array.data_address = " << datalayout.chunk_fixed_array.data_address << endl;
				break;
			case tns::CHUNK_INDEXING_EXTENSIBLE_ARRAY:
				o << "datalayout.chunk_indexing_type = EXTENSIBLE_ARRAY" << endl;
				o << "datalayout.chunk_extensible_array.max_bits = " << datalayout.chunk_extensible_array.max_bits << endl;
				o << "datalayout.chunk_extensible_array.index_elements = " << datalayout.chunk_extensible_array.index_elements << endl;
				o << "datalayout.chunk_extensible_array.min_elements = " << datalayout.chunk_extensible_array.min_elements << endl;
				o << "datalayout.chunk_extensible_array.min_pointers = " << datalayout.chunk_extensible_array.min_pointers << endl;
				o << "datalayout.chunk_extensible_array.page_bits = " << datalayout.chunk_extensible_array.page_bits << endl;
				break;
			case tns::CHUNK_INDEXING_BTREE_V2:
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
	case tns::LAYOUT_VIRTUAL: {
		o << "datalayout.layout_class = VIRTUAL" << endl;
		break;
	}
	default:
		o << "datalayout.layout_class = UNSUPPORTED" << endl;
		break;
	}

	return o;
}

ostream & operator<<(ostream & o, object_datatype_t const & datatype)
{
	using tns = object_datatype_t;

	o << "datatype.version = " << static_cast<int>(datatype.version) << endl;

	o << "datatype.class = ";
	switch (datatype.xclass) {
	case tns::CLASS_FIXED_POINT: o << "FIXED_POINT"; break;
	case tns::CLASS_FLOATING_POINT: o << "FLOATING_POINT"; break;
	case tns::CLASS_TIME: o << "TIME"; break;
	case tns::CLASS_STRING: o << "STRING"; break;
	case tns::CLASS_BITFIELD: o << "BITFIELD"; break;
	case tns::CLASS_OPAQUE: o << "OPAQUE"; break;
	case tns::CLASS_COMPOUND: o << "COMPOUND"; break;
	case tns::CLASS_REFERENCE: o << "REFERENCE"; break;
	case tns::CLASS_ENUMERATED: o << "ENUMERATED"; break;
	case tns::CLASS_VARIABLE_LEGNTH: o << "VARIABLE_LEGNTH"; break;
	case tns::CLASS_ARRAY: o << "ARRAY"; break;
	default: o << "UNKNOWN";
	}
	o << endl;

	o << "datatype.flags = 0b" << datatype.flags << endl;
	o << "datatype.size_of_elements = " << datatype.size_of_elements << endl;
	return o;
}


ostream & operator<<(ostream & o, object_link_info_t const & link_info)
{
	o << "link_info.flags = " << link_info.flags << endl;
	o << "link_info.maximum_creation_index = " << link_info.maximum_creation_index << endl;
	o << "link_info.fractal_head_address = " << link_info.fractal_head_address << endl;
	o << "link_info.name_index_b_tree_address = " << link_info.name_index_b_tree_address << endl;
	o << "link_info.creation_order_index_address = " << link_info.creation_order_index_address << endl;
	return o;
}

ostream & operator<< (ostream & o, object_data_storage_filter_pipeline_t::filter const & f)
{
	return o << "<filter id=" << f.id <<", name=" << f.name << ", flags=" << f.flags << ", client data=" << f.params << ">";
}

ostream & operator<< (ostream & o, object_data_storage_filter_pipeline_t const & f)
{
	for (int i = 0; i < f.filters.size(); ++i) {
		o << "data_storage_filter_pipeline.filter["<<i<<"] = " << f.filters[i] << endl;
	}
	return o;
}


ostream & operator<< (ostream & o, object_fill_value_t const & f)
{
	o << "fillvalue_old.size = " << f.value.size() << endl;
	return o << "fillvalue_old.value = " << vector_to_hex_string(f.value) << endl;
}


ostream & operator<< (ostream & o, object_data_storage_fill_value_t const & f)
{
	o << "data_storage_fillvalue.flags = 0b" << f.flags << endl;
	o << "data_storage_fillvalue.size = " << f.value.size() << endl;
	if (f.has_fillvalue()) {
		return o << "data_storage_fillvalue.value = " << vector_to_hex_string(f.value) << endl;
	} else {
		return o << "data_storage_fillvalue.value = undef" << endl;
	}
}

ostream & operator<<(ostream & o, object_group_info_t const & group_info) {
		o << "group_info.version = " << static_cast<int>(group_info.version) << endl;
		o << "group_info.flags = 0b" << group_info.flags << endl;
		o << "group_info.maximum_compact_value = " << group_info.maximum_compact_value << endl;
		o << "group_info.minimum_dense_value = " << group_info.minimum_dense_value << endl;
		o << "group_info.estimated_number_of_entry = " << group_info.estimated_number_of_entry << endl;
		o << "group_info.estimated_link_name_length_entry = " << group_info.estimated_link_name_length_entry << endl;
		return o;
}

} // h5ng


