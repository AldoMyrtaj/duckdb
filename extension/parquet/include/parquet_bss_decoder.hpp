#pragma once
#include "decode_utils.hpp"

namespace duckdb {
class BssDecoder {
public:
	BssDecoder(const uint8_t *buffer, uint32_t buffer_len, uint32_t page_rows_awaileable) : buffer_((char *)buffer, buffer_len) {
		num_values_in_buffer = page_rows_awaileable;
	};

	template <typename T>
	void GetBatch(char *values_target_ptr, uint32_t batch_size, idx_t result_offset) {
		constexpr size_t num_streams = sizeof(T);
		for(int i = 0; i < batch_size; ++i) {
			uint8_t gathered_byte_data[num_streams];
			for(size_t b=0; b < num_streams; ++b) {
				uint64_t byte_index = b * num_values_in_buffer + result_offset + i;
				gathered_byte_data[b] = buffer_.getValue<uint8_t>(byte_index);
				values_target_ptr[i*num_streams+b] = buffer_.getValue<uint8_t>(byte_index);
			}
		}
	}

private:
	ByteBuffer buffer_;
	int num_values_in_buffer{0U};
};
} // namespace duckdb
