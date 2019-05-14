#include "zlib_utils.h"
#include "gs_error.h"

UTIL_TRACER_DECLARE(ZLIB_UTILS);

ZlibUtils::ZlibUtils() :
isFirstTime_(true), compressionErrorCount_(0) {
	deflateStream_.zalloc = Z_NULL;
	deflateStream_.zfree = Z_NULL;
	deflateStream_.opaque = Z_NULL;

	inflateStream_.zalloc = Z_NULL;
	inflateStream_.zfree = Z_NULL;

	static const char* currentVersion = ZLIB_VERSION;
	if (currentVersion[0] != zlibVersion()[0]) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INCOMPATIBLE_ZLIB_VERSION,
				"the zlib library version (zlib_version) is incompatible"
				<< " with the version assumed, " << currentVersion
				<< ", but the library version, " << zlibVersion());
	}
}

ZlibUtils::~ZlibUtils() {}

uint64_t ZlibUtils::getCompressionErrorCount() const {
	return compressionErrorCount_;
}

void ZlibUtils::compressData(
		const uint8_t* src, uint32_t srcSize,
		uint8_t* dest, uint32_t &destSize)
{
	int32_t flush = Z_FINISH;

	deflateStream_.zalloc = Z_NULL;
	deflateStream_.zfree = Z_NULL;
	deflateStream_.opaque = Z_NULL;

	int32_t ret = deflateInit(&deflateStream_, COMPRESS_LEVEL);
	if (ret != Z_OK) {
		if (ret == Z_MEM_ERROR) {
			if (isFirstTime_) {
				GS_TRACE_WARNING(ZLIB_UTILS, GS_TRACE_CM_COMPRESSION_FAILED,
						"error occured in deflateInit.");
				isFirstTime_ = false;
			}
			destSize = srcSize;
			compressionErrorCount_++;
			return;
		}
		GS_THROW_USER_ERROR(GS_ERROR_CM_COMPRESSION_FAILED,
							  "deflateInit failed.");
	}

	deflateStream_.avail_in = srcSize;
	deflateStream_.avail_out = destSize;
	deflateStream_.next_in = (Bytef*)src;
	deflateStream_.next_out = (Bytef*)dest;

	do {
		ret = deflate(&deflateStream_, flush);
	} while (ret == Z_OK);

	if (ret != Z_STREAM_END) {
		if (isFirstTime_) {
			GS_TRACE_ERROR(ZLIB_UTILS, GS_TRACE_CM_COMPRESSION_FAILED,
						   "error occured in deflate.");
			isFirstTime_ = false;
		}
		destSize = srcSize;
		compressionErrorCount_++;
		return;
	} else {
		destSize = static_cast<uint32_t>(deflateStream_.total_out);
		if (srcSize < destSize) {
			destSize = srcSize;
		}
	}

	ret = deflateEnd(&deflateStream_);
	if (ret != Z_OK) {
		if (isFirstTime_) {
			GS_TRACE_ERROR(ZLIB_UTILS, GS_TRACE_CM_COMPRESSION_FAILED,
						   "error occured in deflateEnd.");
		}
		destSize = srcSize;
		compressionErrorCount_++;
		return;
	}
}

void ZlibUtils::uncompressData(
		const uint8_t* src, uint32_t srcSize,
		uint8_t* dest, uint32_t &destSize)
{
	int32_t flush = Z_FINISH;

	inflateStream_.zalloc = Z_NULL;
	inflateStream_.zfree = Z_NULL;
	inflateStream_.opaque = Z_NULL;

	if (inflateInit(&inflateStream_) != Z_OK) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_UNCOMPRESSION_FAILED,
							  "inflateInit failed.");
	}

	inflateStream_.avail_in = srcSize;
	inflateStream_.avail_out = destSize;
	inflateStream_.next_in = (Bytef*)src;
	inflateStream_.next_out = (Bytef*)dest;

	int ret = Z_OK;
	do {
		ret = inflate(&inflateStream_, flush);
	} while(ret == Z_OK);

	if (ret != Z_STREAM_END) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_UNCOMPRESSION_FAILED,
							  " There was not enough memory "
							  << ", or there was not enough room in the output "
							  "buffer. "
							  << " srcSize = " << srcSize
							  << ", desSize = " << destSize << ", ret = " << ret);
	}

	destSize = static_cast<uint32_t>(inflateStream_.total_out);

	if (inflateEnd(&inflateStream_) != Z_OK) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_UNCOMPRESSION_FAILED,
							  "inflateEnd failed.");
	}
}

