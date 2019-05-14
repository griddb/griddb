#ifndef ZLIB_UTILS_H_
#define ZLIB_UTILS_H_

#include "util/type.h"
#include "zlib.h"

class ZlibUtils {
  public:
	ZlibUtils();
	~ZlibUtils();

	uint64_t getCompressionErrorCount() const;

	void compressData(
		const uint8_t* src, uint32_t srcSize,
		uint8_t* dest, uint32_t &destSize);

	void uncompressData(
		const uint8_t* src, uint32_t srcSize,
		uint8_t* dest, uint32_t &destSize);

  private:
	static const int32_t COMPRESS_LEVEL = Z_BEST_SPEED;

	ZlibUtils(const ZlibUtils&);
	ZlibUtils& operator=(const ZlibUtils&);
	bool isFirstTime_;
	uint64_t compressionErrorCount_;
	z_stream deflateStream_;
	z_stream inflateStream_;
};

#endif
