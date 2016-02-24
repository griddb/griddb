/*
    Copyright (c) 2012 TOSHIBA CORPORATION.
    
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*
    Copyright (c) 2011 Minor Gordon
    All rights reserved

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
    * Neither the name of the Yield project nor the
    names of its contributors may be used to endorse or promote products
    derived from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL Minor Gordon BE LIABLE FOR ANY DIRECT,
    INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
    THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*!
	@file
    @brief Definition of Utility of operation of codes, bits and binary strings
*/
#ifndef UTIL_CODE_H_
#define UTIL_CODE_H_

#include "util/type.h"
#include <set>
#include <limits>
#include <stdlib.h>
#include <string.h>

namespace util {

/*!
	@brief Creates a random number.
*/



class Random {
public:
	

	Random() {}

	explicit Random(int64_t seed) {
		srand(static_cast<int32_t>(seed));
	}

	~Random() {}

	int32_t nextInt32() {
		return (rand() << 30 | rand() << 15 | rand());
	}

	
	
	
	int32_t nextInt32(int32_t count) {
		if (count <= 0) {
			return 0;
		}

		
		return (nextInt32() & 0x7fffffff) % count;
	}

	int64_t nextInt64() {
		return (static_cast<int64_t>(nextInt32()) << 32 | nextInt32());
	}

	bool nextBool() {
		return !!(rand() & 0x1);
	}
};

/*!
	@brief Converts strings and values of numbers or booleans.
*/



template<typename T>
struct LexicalConverter {
	bool operator()(const char8_t *src, T &dest);
	T operator()(const char8_t *src);

	bool operator()(const u8string &src, T &dest);
	T operator()(const u8string &src);
};

template<typename T>
struct FormattedValue {
public:
	inline FormattedValue(const T &value) : value_(value) {}
	void operator()(std::ostream &stream) const;

private:
	const T &value_;
};

struct ValueFormatter {
	template<typename T>
	inline FormattedValue<T> operator()(const T &value) const {
		return value;
	}
};

template<typename T>
inline std::ostream& operator<<(
		std::ostream &stream, const FormattedValue<T> &value) {
	value(stream);
	return stream;
}

class CodeConverter;

/*!
	@brief Types of encodings of strings.
*/



class Code {
	friend class CodeConverter;
public:



	const static Code CHAR;




	const static Code ISO88591;




	const static Code UTF8;

#ifdef _WIN32



	const static Code WCHAR_T;
#endif

private:
#ifdef _WIN32




	Code(unsigned int codePage) :
			codePage_(codePage) {
	}
#else




	Code(const char *iconvCode) : iconvCode_(iconvCode) {
	}
#endif

private:
#ifdef _WIN32




	operator unsigned int() const {
		return codePage_;
	}
#else




	operator const char* () const {
		return iconvCode_;
	}
#endif

private:
#ifdef _WIN32
	unsigned int codePage_;
#else
	const char *iconvCode_;
#endif
};

/*!
	@brief Utilities to convert encodings of a string.
*/



class CodeConverter {
public:





	CodeConverter(Code fromCode, Code toCode);





	~CodeConverter();
















	size_t operator()(const char **inBuf, size_t *inBytesLeft, char **outBuf,
			size_t *outBytesLeft);







	bool operator()(const std::string &inBuf, std::string &outBuf);

#ifdef _WIN32






	bool operator()(const std::string &inBuf, std::wstring &outBuf);







	bool operator()(const std::wstring &inBuf, std::string &outBuf);
#endif

private:
	CodeConverter(const CodeConverter&);
	CodeConverter& operator=(const CodeConverter&);

#ifndef _WIN32
	size_t convertToChars(const char **inBuf, size_t *inBytesLeft,
			char **outBuf, size_t *outBytesLeft);

	template<class OutStringType>
	bool convertToString(const char *inBuf, size_t inBytesLeft,
			OutStringType &outBuf);

	bool reset();
#endif 
private:
#ifdef _WIN32
	Code fromCode_;
	Code toCode_;
#else
	void *descriptor_;
#endif
};







/*!
	@brief Creates CRC32.
*/



class CRC32 {
public:

	static uint32_t update(uint32_t lastCRC, const void *buf, size_t length);

	static uint32_t calculate(const void *buf, size_t length);

private:
	static const uint32_t* getTable();

	CRC32();
	~CRC32();
};

/*!
	@brief Creates CRC16.
*/



class CRC16 {
public:

	

	static uint16_t calculate(const void *buf, size_t length);

private:
	static const uint16_t* getTable();

	CRC16();
	~CRC16();
};


uint32_t fletcher32(const void *buf, size_t len);


uint32_t countNumOfBits(uint32_t bits);


uint32_t population(uint32_t bits);


uint32_t nlz(uint32_t bits);



uint32_t bitsize(uint32_t bits);



uint32_t ilog2(uint32_t bits);

/*!
	@brief Gets logarithm of base of e^2.
*/



template<uint32_t n>
struct ILog2 {
	enum {
		VALUE = ILog2<n / 2>::VALUE + 1
	};
};

template<>
struct ILog2<0> {
	enum {
		VALUE = -1
	};
};



uint32_t getFirst1Position(uint32_t bits);


uint32_t nextPowerBitsOf2(uint32_t x);


uint32_t nextPowerOf2(uint32_t x);

/*!
	@brief Finds a bit 1 in the array of bits.
*/





template<typename V>
class BitNumSet {
public:
	BitNumSet(const std::set<V> &orgSet);
	~BitNumSet();
	bool find(const V value);

private:
	typedef uint32_t Block;
	static const size_t TABLE_BITS = 7;
	static const size_t BLOCK_BITS = ILog2<sizeof(Block)>::VALUE;		

	static const size_t TABLE_SIZE = static_cast<size_t>(1) << TABLE_BITS;
	static const V TABLE_MASK = static_cast<V>(
			(static_cast<size_t>(1) << TABLE_BITS) - 1 );
	static const V BLOCK_MASK = static_cast<V>(
			(static_cast<size_t>(1) << BLOCK_BITS) - 1 );

	Block blockList_[TABLE_SIZE];
	bool conflict_;
	std::set<V> valueSet_;
};

/*!
	@brief Returns alignment size of each types.
*/




template<typename T>
struct AlignmentOf {
	struct ComplexType {
		char first_;
		T second_;
	};
	enum Value {
		VALUE = static_cast<size_t>( offsetof(ComplexType, second_) )
	};
};






















int32_t varIntDecode64(const uint8_t *p, uint64_t &v);
int32_t varIntDecode32(const uint8_t *p, uint32_t &v);
int32_t varIntEncode64(uint8_t *p, uint64_t v);
int32_t varIntEncode32(uint8_t *p, uint32_t v);







/*!
	@brief Encodes and decodes  BASE64.
*/



class Base64Converter {
public:
	
	
	
	
	static std::ostream& encode(std::ostream &os, std::istream &is);

	
	
	
	
	
	
	static size_t encode(char *obuf, const char *ibuf, size_t iblen);

	
	
	
	
	static std::ostream& decode(std::ostream &os, std::istream &is);

	
	
	
	
	
	static size_t decode(char *obuf, const char *ibuf, size_t iblen);

public:
	
	
	
	
	
	static char* encodeUnit(char o[4], const char *i, size_t ilen);

	
	
	
	
	static size_t decodeUnit(char o[3], const char i[4]);

private:
	Base64Converter();
	Base64Converter(const Base64Converter&);
	inline Base64Converter& operator=(const Base64Converter&);
};

/*!
	@brief Encodes and decodes  strings using escape characters.
*/



class EscapeConverter {
public:
	
	
	
	
	
	static std::ostream& encode(std::ostream &os, std::istream &is,
			char e = '\\');

	
	
	
	
	
	
	
	static size_t encode(char *obuf, const char *ibuf, size_t iblen,
			char e = '\\');

	
	
	
	
	
	static std::ostream& decode(std::ostream &os, std::istream &is,
			char e = '\\');

	
	
	
	
	
	
	static size_t decode(char *obuf, const char *ibuf, size_t iblen,
			char e = '\\');

private:
	EscapeConverter();
	EscapeConverter(const EscapeConverter&);
	EscapeConverter& operator=(const EscapeConverter&);
};

/*!
	@brief Encodes and decodes  a hex byte array.
*/



class HexConverter {
public:
	
	
	
	
	
	static std::ostream& encode(std::ostream &os, std::istream &is,
			bool cap = true);

	
	
	
	
	
	
	
	static size_t encode(char *obuf, const char *ibuf, size_t iblen,
			bool cap = true);

	
	
	
	
	static std::ostream& decode(std::ostream &os, std::istream &is);

	
	
	
	
	
	static size_t decode(char *obuf, const char *ibuf, size_t iblen);

	
	
	
	
	
	static char* encode(char o[2], char c, bool cap);

	
	
	
	static char decode(const char o[2]);

private:
	HexConverter();
	HexConverter(const HexConverter&);
	HexConverter& operator=(const HexConverter&);
};

/*!
	@brief Encodes and decodes  a URL.
*/



class URLConverter {
public:
	
	
	
	
	static std::ostream& encode(std::ostream &os, std::istream &is);

	
	
	
	
	
	
	static size_t encode(char *obuf, const char *ibuf, size_t iblen);

	
	
	
	
	static std::ostream& decode(std::ostream &os, std::istream &is);

	
	
	
	
	
	static size_t decode(char *obuf, const char *ibuf, size_t iblen);

private:
	URLConverter();
	URLConverter(const URLConverter&);
	inline URLConverter& operator=(const URLConverter&);

	static const uint8_t URL_TABLE[];
};




























/*!
	@brief Encodes and decodes  a byte stream.
*/



template<typename S>
class BasicByteStream {
public:
	BasicByteStream() : error_(false) {}

	
	
	

	
	
	size_t readAll(void *buf, size_t length, bool force);

	
	void readAll(void *buf, size_t length);

	
	
	

	
	
	size_t writeAll(const void *buf, size_t length, bool force);

	
	void writeAll(const void *buf, size_t length);

	inline void ensureEOF();

	inline void flush() {}

	inline bool isError() const { return error_; };

	void setError() { error_ = true; };

protected:
	template<bool Enabled=true>
	struct NotSupportedError {
		static void UTIL_NORETURN(raise()) {
			UTIL_STATIC_ASSERT(Enabled);
		}
	};

private:
	inline S& impl() { return *static_cast<S*>(this); }

	bool error_;
};

/*!
	@brief Returns size of an encoded or decoded byte stream.
*/




class SizeCountStream : public BasicByteStream<SizeCountStream> {
public:
	explicit SizeCountStream(size_t &size) : size_(size) {}

	inline size_t write(const void*, size_t length) {
		size_ += length;
		return length;
	}

private:
	size_t &size_;
};

/*!
	@brief Arrays in the stream.
*/



class ArrayInStream : public BasicByteStream<ArrayInStream> {
public:
	ArrayInStream(const void *buffer, size_t size);

	size_t read(void *buf, size_t length);

	void readAll(void *buf, size_t length);

	inline size_t getRemaining() const { return remaining(); }		

	inline size_t remaining() const { return end_ - next_; }

	inline size_t position() const { return next_ - begin_; }

	void position(size_t pos);

	inline void clear() { position(0); }

private:
	const uint8_t *begin_;
	const uint8_t *end_;
	const uint8_t *next_;
};

/*!
	@brief Read and write formatter of a string as a pair of a size header and an array.
*/











template<bool Check = true, typename StrSize = uint32_t>
class BasicStingFormatter {
public:
	template<typename S, typename E, typename T, typename A>
	void get(S &s, std::basic_string<E, T, A> &str);

	template<typename S, typename E, typename T, typename A>
	void put(S &s, const std::basic_string<E, T, A> &str);

	template<typename S, typename E>
	void put(S &s, const E *str);

private:
	
	struct StrSizeChecker {
		static inline bool check(const uint8_t*) { return true; }
		static inline bool check(const uint16_t*) { return true; }
		static inline bool check(const uint32_t*) { return true; }
		static inline bool check(const uint64_t*) { return true; }
		static inline bool check(const char *s) { return *s >= 0; }
		static inline bool check(const int16_t *s) { return *s >= 0; }
		static inline bool check(const int32_t *s) { return *s >= 0; }
		static inline bool check(const int64_t *s) { return *s >= 0; }
	};

	
	
	
	
	
	struct InstanceForGet {};
	struct InstanceForPut {};
	template<typename Src, typename Dest, typename Instance>
	static inline Dest filterStringSize(Src size, Dest maxSize, const Instance&);
};

/*!
	@brief I/O stream of a byte array of basic types or string type in binary.
*/




template< typename S, typename F = BasicStingFormatter<> >
class ByteStream {
public:
	typedef ByteStream<S, F> ThisType;

	inline explicit ByteStream(const S &baseStream);

	inline ByteStream(const S &baseStream, const F &formatterObj);

	
	template<typename V>
	inline ThisType& operator>>(V &value);

	
	template<typename V>
	inline ThisType& operator<<(const V &value);

	
	template<typename V, typename Size>
	inline ThisType& operator>>(const std::pair<V*, Size> &valArray);

	
	template<typename V, typename Size>
	inline ThisType& operator<<(const std::pair<const V*, Size> &valArray);
	template<typename V, typename Size>
	inline ThisType& operator<<(const std::pair<V*, Size> &valArray);

	
	template<typename E>
	inline ThisType& operator<<(const E *str);

	inline size_t readAll(void *buf, size_t length, bool force) {
		return base_.readAll(buf, length, force);
	}
	inline void readAll(void *buf, size_t length) {
		base_.readAll(buf, length);
	}

	inline size_t writeAll(const void *buf, size_t length, bool force) {
		return base_.writeAll(buf, length, force);
	}
	inline void writeAll(const void *buf, size_t length) {
		base_.writeAll(buf, length);
	}

	
	
	inline void ensureEOF() { base_.ensureEOF(); }

	
	
	
	inline void flush() { base_.flush(); }

	inline S& base() { return base_; };

	inline const S& base() const { return base_; };

	inline F& formatter() { return formatter_; };

	inline const F& formatter() const { return formatter_; };

private:
	template<typename V>
	inline ThisType& get(V &value, TrueType);

	template<typename V>
	inline ThisType& put(const V &value, TrueType);

	template<typename E, typename T, typename A>
	inline ThisType& get(std::basic_string<E, T, A> &str, FalseType);

	template<typename E, typename T, typename A>
	inline ThisType& put(const std::basic_string<E, T, A> &str, FalseType);

	S base_;
	F formatter_;
};

namespace detail {
template<typename T>
struct PrimitiveOf {
	typedef FalseType Result;
};
template<> struct PrimitiveOf<char> { typedef TrueType Result; };
template<> struct PrimitiveOf<int8_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<int16_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<int32_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<int64_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<uint8_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<uint16_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<uint32_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<uint64_t> { typedef TrueType Result; };
template<> struct PrimitiveOf<float> { typedef TrueType Result; };
template<> struct PrimitiveOf<double> { typedef TrueType Result; };
struct StreamErrors {
	static void UTIL_NORETURN(throwUnexpectedEnd());
	static void UTIL_NORETURN(throwPositionOutOfRange());
	static void UTIL_NORETURN(throwUnexpectedRemaining());
};
}

typedef ByteStream<util::ArrayInStream> ArrayByteInStream;











namespace detail {

template<typename T>
struct GeneralParser {
	bool operator()(const std::string &src, T &dest) {
		for (;;) {
			util::NormalIStringStream iss(src);
			iss.peek();
			if (iss.eof()) {
				break;
			}

			iss.unsetf(std::ios::skipws);
			iss >> dest;

			
			if (iss.bad()) {
				break;
			}
			if (!iss.eof()) {
				break;
			}

			return true;
		}

		dest = T();
		return false;
	}
};

template<typename T>
struct GeneralFormatter {
	void operator()(const T &src, std::ostream &dest) {
		LocaleUtils::CLocaleScope localeScope(dest);
		dest << src;
	}
};

struct BoolParser {
	bool operator()(const std::string &src, bool &dest) {
		return this->operator()(src.c_str(), dest);
	}

	bool operator()(const char8_t *src, bool &dest) {
		if (strcmp(src, "true") == 0) {
			dest = true;
			return true;
		}
		else if (strcmp(src, "false") == 0) {
			dest = false;
			return true;
		}

		dest = bool();
		return false;
	}
};

struct BoolFormatter {
	void operator()(bool src, std::ostream &dest) {
		LocaleUtils::CLocaleScope localeScope(dest);
		if (src) {
			dest << "true";
		}
		else {
			dest << "false";
		}
	}
};

template<typename T, typename B>
struct SmallNumberParser {
	bool operator()(const std::string &src, T &dest) {
		do {
			B base;
			if (!GeneralParser<B>()(src, base)) {
				break;
			}

			if (base < static_cast<B>(std::numeric_limits<T>::min()) ||
					base > static_cast<B>(std::numeric_limits<T>::max())) {
				break;
			}

			dest = static_cast<T>(base);
			return true;
		}
		while (false);

		dest = T();
		return false;
	}
};

template<typename T, typename B>
struct SmallNumberFormatter {
	void operator()(T src, std::ostream &dest) {
		GeneralFormatter<B>()(src, dest);
	}
};

template<typename T>
struct FloatingNumberFormatter {
	void operator()(const T &src, std::ostream &dest);
};

template< typename T, typename P = GeneralParser<T> >
struct ValueParserTraits {
	typedef P Parser;
};

template<> struct ValueParserTraits<bool> {
	typedef BoolParser Parser; };
template<> struct ValueParserTraits<int8_t> {
	typedef SmallNumberParser<int8_t, int32_t> Parser; };
template<> struct ValueParserTraits<uint8_t> {
	typedef SmallNumberParser<uint8_t, uint32_t> Parser; };
template<> struct ValueParserTraits<int16_t> {
	typedef SmallNumberParser<int16_t, int32_t> Parser; };
template<> struct ValueParserTraits<uint16_t> {
	typedef SmallNumberParser<uint16_t, uint32_t> Parser; };

template< typename T, typename F = GeneralFormatter<T> >
struct ValueFormatterTraits {
	typedef F Formatter;
};

template<> struct ValueFormatterTraits<bool> {
	typedef BoolFormatter Formatter; };
template<> struct ValueFormatterTraits<int8_t> {
	typedef SmallNumberFormatter<int8_t, int32_t> Formatter; };
template<> struct ValueFormatterTraits<uint8_t> {
	typedef SmallNumberFormatter<uint8_t, uint32_t> Formatter; };
template<> struct ValueFormatterTraits<float> {
	typedef FloatingNumberFormatter<float> Formatter; };
template<> struct ValueFormatterTraits<double> {
	typedef FloatingNumberFormatter<double> Formatter; };

}	

template<typename T>
inline bool LexicalConverter<T>::operator()(const char8_t *src, T &dest) {
	typedef typename detail::ValueParserTraits<T>::Parser Parser;
	return Parser()(src, dest);
}

template<typename T>
inline T LexicalConverter<T>::operator()(const char8_t *src) {
	T dest;
	if (!this->operator()(src, dest)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_PARAMETER);
	}

	return dest;
}

template<typename T>
inline bool LexicalConverter<T>::operator()(const u8string &src, T &dest) {
	typedef typename detail::ValueParserTraits<T>::Parser Parser;
	return Parser()(src, dest);
}

template<typename T>
inline T LexicalConverter<T>::operator()(const u8string &src) {
	T dest;
	if (!this->operator()(src, dest)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_PARAMETER);
	}

	return dest;
}

template<>
struct LexicalConverter<u8string> {
	template<typename S>
	void operator()(const S &src, std::ostream &dest) {
		typedef typename detail::ValueFormatterTraits<S>::Formatter Formatter;
		Formatter()(src, dest);
	}

	template<typename S>
	u8string operator()(const S &src) {
		NormalOStringStream oss;
		this->operator()(src, oss);
		return oss.str();
	}
};

template<typename T>
inline void FormattedValue<T>::operator()(std::ostream &stream) const {
	typedef typename detail::ValueFormatterTraits<T>::Formatter Formatter;
	Formatter()(value_, stream);
}






inline uint32_t CRC32::update(uint32_t lastCRC, const void *buf, size_t length) {
	const uint32_t* const table = getTable();

	uint32_t c = lastCRC ^ 0xffffffffL;
	for (size_t n = 0; n < length; n++) {
		c = table[(c ^ static_cast<const uint8_t*>(buf)[n]) & 0xff] ^ (c >> 8);
	}
	return c ^ 0xffffffffL;
}

inline uint32_t CRC32::calculate(const void *buf, size_t length) {
	return update(0, buf, length);
}




inline uint16_t CRC16::calculate(const void *buf, size_t length) {
	const uint16_t* const table = getTable();

	uint16_t c = 0xffff;
	for (size_t n = 0; n < length; n++) {
		c = static_cast<uint16_t>(table[
			(c >> 8 ^ static_cast<const uint8_t*>(buf)[n]) & 0xff] ^ (c << 8));
	}
	return c;
}



inline uint32_t fletcher32(const void *buf, size_t len) {
	len /= 2; 
	const uint16_t *data = static_cast<const uint16_t *>(buf);
    uint32_t sum1 = 0xffff, sum2 = 0xffff;

    while (len) {
            size_t tlen = len > 360 ? 360 : len;
            len -= tlen;
            do {
                    sum1 += *data++;
                    sum2 += sum1;
            } while (--tlen);
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }
    /* Second reduction step to reduce sums to 16 bits */
    sum1 = (sum1 & 0xffff) + (sum1 >> 16);
    sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    return sum2 << 16 | sum1;
}


inline uint32_t countNumOfBits(uint32_t bits) {
	bits = (bits & 0x55555555) + (bits >> 1 & 0x55555555);
	bits = (bits & 0x33333333) + (bits >> 2 & 0x33333333);
	bits = (bits & 0x0f0f0f0f) + (bits >> 4 & 0x0f0f0f0f);
	bits = (bits & 0x00ff00ff) + (bits >> 8 & 0x00ff00ff);
	return ((bits & 0x0000ffff) + (bits >> 16 & 0x0000ffff));
}

inline uint32_t population(uint32_t bits) {
	bits = bits - ((bits >> 1) & 0x55555555);
	bits = (bits & 0x33333333) + ((bits >> 2) & 0x33333333);
	bits = (bits + (bits >> 4)) & 0x0f0f0f0f;
	bits = bits + (bits >> 8);
	bits = bits + (bits >> 16);
	return (bits & 0x0000003f);
}

inline uint32_t nlz(uint32_t bits) {
	bits = bits | (bits >> 1);
	bits = bits | (bits >> 2);
	bits = bits | (bits >> 4);
	bits = bits | (bits >> 8);
	bits = bits | (bits >> 16);
	return population(~bits);
}

/*
	@note cf.(Public Domain)http://www.hackersdelight.org/
*/


inline uint32_t bitsize(uint32_t bits) {
	bits = bits ^ (bits >> 31);
	return (33 - nlz(bits));
}

/*
	@note cf.(Public Domain)http://www.hackersdelight.org/
*/


inline uint32_t ilog2(uint32_t bits) {
	bits = bits | (bits >> 1);
	bits = bits | (bits >> 2);
	bits = bits | (bits >> 4);
	bits = bits | (bits >> 8);
	bits = bits | (bits >> 16);
	return population(bits) - 1;
}


inline uint32_t getFirst1Position(uint32_t bits) {
	
	
	
	return countNumOfBits(
		( bits & static_cast<uint32_t>(-static_cast<int32_t>(bits)) ) - 1);
}

/*
	@note cf.(Public Domain)http://www.hackersdelight.org/
*/


inline uint32_t nextPowerBitsOf2(uint32_t x) {
	return ( 32 - nlz(x - 1) );
}

inline uint32_t nextPowerOf2(uint32_t x) {
	return ( 1 << nextPowerBitsOf2(x) );
}





template<typename V>
BitNumSet<V>::BitNumSet(const std::set<V> &orgSet) :
		conflict_(false), valueSet_(orgSet) {

	std::fill(&blockList_[0], &blockList_[TABLE_SIZE], 0);
	for ( typename std::set<V>::iterator it = valueSet_.begin();
			it != valueSet_.end(); ++it ) {
		const V value = *it;
		if (( value >> (BLOCK_BITS + TABLE_BITS) ) != 0) {
			conflict_ |= true;
		}

		Block &block = blockList_[(value >> BLOCK_BITS) & TABLE_MASK];
		if (( block & (1 << (value & BLOCK_MASK)) ) != 0) {
			conflict_ |= true;
		}
		else {
			block |= (1 << (value & BLOCK_MASK));
		}
	}
}

template<typename V>
BitNumSet<V>::~BitNumSet() {
}

template<typename V>
inline bool BitNumSet<V>::find(const V value) {
	if (( blockList_[(value >> BLOCK_BITS) & TABLE_MASK] &
			(1 << (value & BLOCK_MASK)) ) != 0) {
		if (conflict_) {
			return ( valueSet_.find(value) != valueSet_.end() );
		}
		else {
			return true;
		}
	}

	return false;
}






/*
	@note cf.(Public Domain)http://www.sqlite.org
*/
UTIL_FORCEINLINE int32_t varIntDecode64(const uint8_t *p, uint64_t &v) {
	uint32_t a, b, s;
	a = *p;
	++p;
	
	if ( !(a & 0x80) ) {
		v = a;
		return 1;
	}
	b = *p;
	++p;
	
	if ( !(b & 0x80) ) {
		a &= 0x7f;
		a = a<<7;
		a |= b;
		v = a;
		return 2;
	}
	a = a << 14;
	a |= *p;
	++p;
	
	if ( !(a&0x80) ) {
		a &= (0x7f << 14) | (0x7f);
		b &= 0x7f;
		b = b << 7;
		a |= b;
		v = a;
		return 3;
	}
	
	a &= (0x7f << 14) | (0x7f);
	b = b << 14;
	b |= *p;
	++p;
	
	if ( !(b & 0x80) ) {
		b &= (0x7f << 14) | (0x7f);
		
		
		a = a << 7;
		a |= b;
		v = a;
		return 4;
	}
	
	
	
	
	
	b &= (0x7f << 14) | (0x7f);
	s = a;
	
	a = a<<14;
	a |= *p;
	++p;
	
	if ( !(a & 0x80) ) {
		
		
		
		b = b << 7;
		a |= b;
		s = s >> 18;
		v = static_cast<uint64_t>(s) << 32 | a;
		return 5;
	}
	
	s = s << 7;
	s |= b;
	
	b = b << 14;
	b |= *p;
	++p;
	
	if ( !(b&0x80) ) {
		
		
		a &= (0x7f << 14) | (0x7f);
		a = a << 7;
		a |= b;
		s = s >> 18;
		v = static_cast<uint64_t>(s) << 32 | a;
		return 6;
	}
	a = a << 14;
	a |= *p;
	++p;
	
	if ( !(a & 0x80) ) {
		a &= (0x7f << 28) | (0x7f << 14) | (0x7f);
		b &= (0x7f << 14) | (0x7f);
		b = b << 7;
		a |= b;
		s = s >> 11;
		v = static_cast<uint64_t>(s) << 32 | a;
		return 7;
	}
	
	a &= (0x7f << 14) | (0x7f);
	b = b << 14;
	b |= *p;
	++p;
	
	if ( !(b & 0x80) ) {
		b &= (0x7f << 28) | (0x7f << 14) | (0x7f);
		
		
		a = a << 7;
		a |= b;
		s = s >> 4;
		v = static_cast<uint64_t>(s) << 32 | a;
		return 8;
	}
	a = a << 15;
	a |= *p;
	++p;
	
	
	
	b &= (0x7f << 14) | (0x7f);
	b = b << 8;
	a |= b;
	s = s << 4;
	
	b = p[-5];	
	b &= 0x7f;
	b = b >> 3;
	s |= b;
	v = static_cast<uint64_t>(s) << 32 | a;
	return 9;
}

/*
	@note cf.(Public Domain)http://www.sqlite.org
*/
UTIL_FORCEINLINE int32_t varIntDecode32(const uint8_t *p, uint32_t &v) {
	uint32_t a, b;
	a = *p;
	++p;
	
	if ( !(a & 0x80) ) {
		v = a;
		return 1;
	}
	b = *p;
	++p;
	
	if ( !(b & 0x80) ) {
		a &= 0x7f;
		a = a<<7;
		a |= b;
		v = a;
		return 2;
	}
	a = a << 14;
	a |= *p;
	++p;
	
	if ( !(a&0x80) ) {
		a &= (0x7f << 14) | (0x7f);
		b &= 0x7f;
		b = b << 7;
		a |= b;
		v = a;
		return 3;
	}
	
	a &= (0x7f << 14) | (0x7f);
	b = b << 14;
	b |= *p;
	++p;

	
	if ( !(b & 0x80) ) {
		b &= (0x7f << 14) | (0x7f);
		
		
		a = a << 7;
		a |= b;
		v = a;
		return 4;
	}

	uint32_t s;
	
	
	
	
	
	b &= (0x7f << 14) | (0x7f);
	s = a;
	
	a = a<<14;
	a |= *p;
	++p;
	
	
	
	
	b = b << 7;
	a |= b;
	s = s >> 18;
	v = static_cast<uint32_t>(static_cast<uint64_t>(s) << 32 | a);
	return 5;
}

/*
	@note cf.(Public Domain)http://www.sqlite.org
*/
UTIL_FORCEINLINE int32_t varIntDecode32_partial(const uint8_t *p, uint32_t &v) {

	uint32_t a, b;
	b = *(p + 1);
	a = *p << 14;
	a |= *(p + 2);
	p += 2;

	
	if ( !(a&0x80) ) {
		a &= (0x7f << 14) | (0x7f);
		b &= 0x7f;
		b = b << 7;
		a |= b;
		v = a;
		return 3;
	}
	
	a &= (0x7f << 14) | (0x7f);
	b = b << 14;
	b |= *p;
	++p;

	
	if ( !(b & 0x80) ) {
		b &= (0x7f << 14) | (0x7f);
		
		
		a = a << 7;
		a |= b;
		v = a;
		return 4;
	}
	uint32_t s;
	
	
	
	
	
	b &= (0x7f << 14) | (0x7f);
	s = a;
	
	a = a<<14;
	a |= *p;
	++p;
	
	
	
	
	b = b << 7;
	a |= b;
	s = s >> 18;
	v = static_cast<uint32_t>(static_cast<uint64_t>(s) << 32 | a);
	return 5;
}


/*
	@note cf.(Public Domain)http://www.sqlite.org
*/
UTIL_FORCEINLINE int32_t varIntEncode64(uint8_t *p, uint64_t v) {
	int32_t i, j, n;
	uint8_t buf[10];
	if ( v & (static_cast<uint64_t>(0xff000000) << 32) ) {
		p[8] = static_cast<uint8_t>(v);
		v >>= 8;
		for (i = 7; i >= 0; --i) {
			p[i] = static_cast<uint8_t>((v & 0x7f) | 0x80);
			v >>= 7;
		}
		return 9;
	}
	n = 0;
	do {
		buf[n++] = static_cast<uint8_t>((v & 0x7f) | 0x80);
		v >>= 7;
	} while( v != 0 );
	buf[0] &= 0x7f;
	for (i = 0, j = n - 1; j >= 0; j--, i++) {
		p[i] = buf[j];
	}
	return n;
}

/*
	@note cf.(Public Domain)http://www.sqlite.org
*/
UTIL_FORCEINLINE int32_t varIntEncode32(uint8_t *p, uint32_t v) {
	int32_t i, j, n;
	uint8_t buf[5];
	n = 0;
	do {
		buf[n++] = static_cast<uint8_t>((v & 0x7f) | 0x80);
		v >>= 7;
	} while( v != 0 );
	buf[0] &= 0x7f;
	for (i = 0, j = n - 1; j >= 0; j--, i++) {
		p[i] = buf[j];
	}
	return n;
}



/*
	@note cf.(Public Domain)http://www.sqlite.org
*/

UTIL_FORCEINLINE uint32_t varIntDecode32_limited_fast_short(const uint8_t *p, uint32_t &v) {
	uint32_t a;
	a = *p;
	if ( !(a & 0x80) ) {
		v = a;
		return 1;
	}
	a &= 0x7f;
	a = a<<7;
	v = (a |= *(++p));
	return 2;
}

/*
	@note cf.(Public Domain)http://www.sqlite.org
*/

UTIL_FORCEINLINE uint32_t varIntDecode32_fast_short(const uint8_t *p, uint32_t &v) {
	if ( !(*p & 0x80) ) {
		v = *p;
		return 1;
	}
	uint32_t a = *p;
	p++;
	if ( !(*p & 0x80) ) {
		a &= 0x7f;
		a = a<<7;
		v = (a |= *p);
		return 2;
	}
	
	return varIntDecode32_partial(p - 1, v);
}

/*
	@note cf.(Public Domain)http://www.sqlite.org
*/

UTIL_FORCEINLINE int32_t varIntEncode32_limited_fast_short(uint8_t *p, uint32_t v) {
	if( (v & ~0x7f)==0 ){
		p[0] = static_cast<uint8_t>(v);
		return 1;
	}
	
	p[0] = static_cast<uint8_t>((v >> 7) | 0x80);
	p[1] = static_cast<uint8_t>(v & 0x7f);
	return 2;
}


UTIL_FORCEINLINE int32_t varIntEncode32_fast_short(uint8_t *p, uint32_t v) {
	if( (v & ~0x7f)==0 ){
		p[0] = static_cast<uint8_t>(v);
		return 1;
	}
	if( (v & ~0x3fff)==0 ){
		p[0] = static_cast<uint8_t>((v >> 7) | 0x80);
		p[1] = static_cast<uint8_t>(v & 0x7f);
		return 2;
	}
	
	return varIntEncode64(p, v);
}











template<typename S>
size_t BasicByteStream<S>::readAll(void *buf, size_t length, bool force) {

	uint8_t *pos = static_cast<uint8_t*>(buf);
	for (size_t remaining = length; remaining > 0;) {
		const size_t last = impl().read(pos, remaining);
		if (last == 0) {
			if (force) {
				setError();
				detail::StreamErrors::throwUnexpectedEnd();
			}
			return (length - remaining);
		}

		remaining -= last;
		pos += last;
	}

	return length;
}

template<typename S>
void BasicByteStream<S>::readAll(void *buf, size_t length) {
	impl().readAll(buf, length);
}

template<typename S>
size_t BasicByteStream<S>::writeAll(const void *buf, size_t length, bool force) {

	const uint8_t *pos = static_cast<const uint8_t*>(buf);
	for (size_t remaining = length; remaining > 0;) {
		const size_t last = impl().write(pos, remaining);
		if (last == 0) {
			if (force) {
				setError();
				detail::StreamErrors::throwUnexpectedEnd();
			}
			return (length - remaining);
		}

		remaining -= last;
		pos += last;
	}

	return length;
}

template<typename S>
void BasicByteStream<S>::writeAll(const void *buf, size_t length) {
	impl().writeAll(buf, length);
}

template<typename S>
inline void BasicByteStream<S>::ensureEOF() {
	uint8_t buf;
	if (impl().read( &buf, sizeof(buf) ) > 0) {
		setError();
		detail::StreamErrors::throwUnexpectedRemaining();
	}
}





inline ArrayInStream::ArrayInStream(const void *buffer, size_t size) :
		begin_( static_cast<const uint8_t*>(buffer) ),
		end_(begin_ + size),
		next_(begin_) {
}

inline size_t ArrayInStream::read(void *buf, size_t length) {
	const size_t readSize = std::min<size_t>(end_ - next_, length);
	memcpy(buf, next_, readSize);

	next_ += readSize;
	return readSize;
}

inline void ArrayInStream::readAll(void *buf, size_t length) {
	const uint8_t *newNext = next_ + length;
	if (newNext > end_) {
		detail::StreamErrors::throwUnexpectedEnd();
	}
	memcpy(buf, next_, length);
	next_ = newNext;
}

inline void ArrayInStream::position(size_t pos) {
	const uint8_t *const newNext = begin_ + pos;
	if (newNext > end_) {
		detail::StreamErrors::throwPositionOutOfRange();
	}
	next_ = newNext;
}





template<bool Check, typename StrSize>
template<typename S, typename E, typename T, typename A>
void BasicStingFormatter<Check, StrSize>::get(
		S &s, std::basic_string<E, T, A> &str) {
	typedef typename std::basic_string<E, T, A>::size_type DestStrSize;

	DestStrSize size;
	{
		StrSize orgSize;
		s >> orgSize;
		if (!StrSizeChecker::check(&orgSize)) {
			UTIL_THROW_UTIL_ERROR(CODE_DECODE_FAILED,
					"Invalid string size");
		}

		try {
			size = this->filterStringSize(
					orgSize, str.max_size(), InstanceForGet());
		}
		catch (std::exception &e) {
			UTIL_RETHROW_UTIL_ERROR(CODE_DECODE_FAILED, e, "");
		}
	}
	str.reserve(size);

	
	
	const size_t LOCAL_BUF_SIZE = 8192;
	E buf[LOCAL_BUF_SIZE + 1];
	for (size_t r = static_cast<size_t>(size); r > 0;) {
		const size_t readLength = std::min(r, LOCAL_BUF_SIZE);

		s >> std::make_pair(buf, readLength);
		if (Check) {
			buf[readLength] = typename T::char_type();
			if (T::length(buf) != readLength) {
				UTIL_THROW_UTIL_ERROR(CODE_DECODE_FAILED,
						"Invalid null character found");
			}
		}

		str.append(buf, readLength);
		r -= readLength;
	}
}

template<bool Check , typename StrSize>
template<typename S, typename E, typename T, typename A>
void BasicStingFormatter<Check, StrSize>::put(
		S &s, const std::basic_string<E, T, A> &str) {
	const StrSize size = this->filterStringSize(
		str.size(), std::numeric_limits<StrSize>::max(), InstanceForPut() );

	StrSizeChecker::check(&size);
	s << size;
	s << std::make_pair( str.c_str(), static_cast<size_t>(size) );
}

template<bool Check , typename StrSize>
template<typename S, typename E>
void BasicStingFormatter<Check, StrSize>::put(S &s, const E *str) {
	const StrSize size = this->filterStringSize(
		std::char_traits<E>::length(str),
		std::numeric_limits<StrSize>::max(), InstanceForPut() );

	StrSizeChecker::check(&size);
	s << size;
	s << std::make_pair( str, static_cast<size_t>(size) );
}

template<bool Check , typename StrSize>
template<typename Src, typename Dest, typename Instance>
inline Dest BasicStingFormatter<Check, StrSize>::filterStringSize(
		Src size, Dest maxSize, const Instance&) {
	UTIL_STATIC_ASSERT(sizeof(Src) <= sizeof(size_t));

	const Dest destSize = static_cast<Dest>(size);
	if (sizeof(Dest) <= sizeof(Src)) {
		if (!StrSizeChecker::check(&destSize) || destSize > maxSize) {
			UTIL_THROW_UTIL_ERROR(CODE_SIZE_LIMIT_EXCEEDED,
					"Too large string size (value=" << destSize <<
					", limit=" << maxSize << ")");
		}
	}

	return destSize;
}





template<typename S, typename F>
inline ByteStream<S, F>::ByteStream(const S &baseStream) :
		base_(baseStream), formatter_() {
}

template<typename S, typename F>
inline ByteStream<S, F>::ByteStream(
		const S &baseStream, const F &formatterObj) :
		base_(baseStream), formatter_(formatterObj) {
}

template<typename S, typename F>
template<typename V>
inline ByteStream<S, F>& ByteStream<S, F>::operator>>(
		V &value ) {
	return get(value, typename detail::PrimitiveOf<V>::Result());
}

template<typename S, typename F>
template<typename V>
inline ByteStream<S, F>& ByteStream<S, F>::operator<<(
		const V &value ) {
	return put(value, typename detail::PrimitiveOf<V>::Result());
}

template<typename S, typename F>
template<typename V, typename Size>
inline ByteStream<S, F>& ByteStream<S, F>::operator>>(
		const std::pair<V*, Size> &valArray ) {
	UTIL_STATIC_ASSERT(detail::PrimitiveOf<V>::Result::VALUE);
	base_.readAll(valArray.first, sizeof(V) * valArray.second);
	return *this;
}

template<typename S, typename F>
template<typename V, typename Size>
inline ByteStream<S, F>& ByteStream<S, F>::operator<<(
		const std::pair<const V*, Size> &valArray ) {
	UTIL_STATIC_ASSERT(detail::PrimitiveOf<V>::Result::VALUE);
	base_.writeAll(valArray.first, sizeof(V) * valArray.second);
	return *this;
}

template<typename S, typename F>
template<typename V, typename Size>
inline ByteStream<S, F>& ByteStream<S, F>::operator<<(
		const std::pair<V*, Size> &valArray ) {
	return ( *this <<
		std::pair<const V*, Size>(valArray.first, valArray.second) );
}

template<typename S, typename F>
template<typename E>
inline ByteStream<S, F>& ByteStream<S, F>::operator<<(
		const E *str ) {
	formatter_.put(*this, str);
	return *this;
}

template<typename S, typename F>
template<typename V>
inline ByteStream<S, F>& ByteStream<S, F>::get(V &value, TrueType) {
	base_.readAll( &value, sizeof(V) );
	return *this;
}

template<typename S, typename F>
template<typename V>
inline ByteStream<S, F>& ByteStream<S, F>::put(const V &value, TrueType) {
	base_.writeAll( &value, sizeof(V) );
	return *this;
}

template<typename S, typename F>
template<typename E, typename T, typename A>
inline ByteStream<S, F>& ByteStream<S, F>::get(
		std::basic_string<E, T, A> &str, FalseType) {
	formatter_.get(*this, str);
	return *this;
}

template<typename S, typename F>
template<typename E, typename T, typename A>
inline ByteStream<S, F>& ByteStream<S, F>::put(
		const std::basic_string<E, T, A> &str, FalseType) {
	formatter_.put(*this, str);
	return *this;
}

} 

#endif
