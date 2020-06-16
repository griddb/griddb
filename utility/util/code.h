/*
    Copyright (c) 2017 TOSHIBA Digital Solutions Corporation
    
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
#include <algorithm> 
#include <stdlib.h>
#include <string.h>


namespace util {

class TimeZone;
class DateTime;


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


struct TinyLexicalIntConverter {
	TinyLexicalIntConverter();

	bool format(char8_t *&it, char8_t *end, uint32_t value) const;
	bool parse(const char8_t *&it, const char8_t *end, uint32_t &value) const;

	size_t minWidth_;
	size_t maxWidth_;
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
struct StrictLexicalConverter {
public:
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

typedef uint32_t CodePoint;

struct UTF8Utils {
	typedef CodePoint ValueType;

	enum {
		MAX_ENCODED_LENGTH = 4,
		ILLEGAL_CHARACTER = 0xfffd
	};

	static bool skipRaw(const char8_t **it, const char8_t *end);
	static bool decodeRaw(
			const char8_t **it, const char8_t *end, CodePoint *c);
	static bool decodeBackRaw(
			const char8_t **it, const char8_t *begin, CodePoint *c);
	static bool encode(char8_t **it, const char8_t *end, CodePoint c);
	static bool encodeRaw(char8_t **it, const char8_t *end, CodePoint c);
	static bool isIllegal(CodePoint c);
	static bool isIllegal(uint64_t c);
};

template<typename T, typename It, typename C>
struct SequenceReader {
public:
	typedef T ValueType;
	typedef It IteratorType;
	typedef C ValueCoderType;

	typedef typename ValueCoderType::ValueType CodeType;

	explicit SequenceReader(const std::pair<IteratorType, size_t> &src);
	SequenceReader(IteratorType begin, IteratorType end);

	void next();
	void prev();

	void step(size_t count);
	void back(size_t count);

	void toBegin();
	void toEnd();

	bool atBegin() const;
	bool atEnd() const;

	const ValueType& get() const;

	bool hasNext() const;
	bool hasPrev() const;

	size_t getNext() const;
	size_t getPrev() const;

	bool nextCode();
	bool prevCode();

	bool nextCode(CodeType *code);
	bool prevCode(CodeType *code);

private:
	IteratorType it_;
	IteratorType begin_;
	IteratorType end_;
};

template<typename T, typename It, typename C>
struct SequenceWriter {
public:
	typedef T ValueType;
	typedef It IteratorType;
	typedef C ValueCoderType;

	typedef typename ValueCoderType::ValueType CodeType;

	explicit SequenceWriter(IteratorType inserter);

	void appendCode(const CodeType &code);
	void append(const ValueType &value);
	void append(const ValueType *buf, size_t size);

private:
	IteratorType inserter_;
};

struct SequenceUtils {
public:
	template<typename T> struct Ref {
	public:
		explicit Ref(const T &value) :
				value_(value),
				ref_(&value_) {
		}

		Ref(const Ref<T> &another) :
				value_(another.value_),
				ref_(&value_) {
		}

		Ref& operator=(const Ref<T> &another) const {
			value_ = another.value_;
			return *this;
		}

		T& operator*() const {
			return *ref_;
		}

	private:
		T value_;
		T *ref_;
	};

	template<typename CharT, typename Traits, typename Alloc>
	static Ref< SequenceReader<char8_t, const char8_t*, UTF8Utils> > toReader(
			const std::basic_string<CharT, Traits, Alloc> &str) {
		return toReader(str.c_str(), str.c_str() + str.size());
	}

	static Ref< SequenceReader<char8_t, const char8_t*, UTF8Utils> > toReader(
			const char8_t *str) {
		return toReader(str, str + strlen(str));
	}

	static Ref< SequenceReader<char8_t, const char8_t*, UTF8Utils> > toReader(
			const char8_t *begin, const char8_t *end) {
		return toRef(SequenceReader<char8_t, const char8_t*, UTF8Utils>(
				begin, end));
	}

	template<typename It>
	static Ref< SequenceWriter<char8_t, It, UTF8Utils> > toWriter(It inserter) {
		return toRef(SequenceWriter<char8_t, It, UTF8Utils>(inserter));
	}


	template<typename W>
	static void appendValue(W &writer, uint32_t value, uint32_t minWidth = 0);

private:
	template<typename T>
	static Ref<T> toRef(const T &value) {
		return Ref<T>(value);
	}
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
uint32_t nlz(uint64_t bits);

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
	typedef FalseType Locator;

	BasicByteStream() : error_(false) {}


	size_t readAll(void *buf, size_t length, bool force);

	void readAll(void *buf, size_t length);

	template<typename A> void readAll(size_t length, A &action);


	size_t writeAll(const void *buf, size_t length, bool force);

	void writeAll(const void *buf, size_t length);

	inline void ensureEOF();

	inline void flush() {}

	inline bool isError() const { return error_; };

	void setError() { error_ = true; };

	Locator locator() { return Locator(); }

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
	class Locator;

	ArrayInStream(const void *buffer, size_t size);

	size_t read(void *buf, size_t length);

	void readAll(void *buf, size_t length);

	template<typename A> void readAll(size_t length, A &action);

	inline size_t getRemaining() const { return remaining(); }		

	inline size_t remaining() const { return end_ - next_; }

	inline size_t position() const { return next_ - begin_; }

	void position(size_t pos);

	inline void clear() { position(0); }

	Locator locator();

private:
	const uint8_t *begin_;
	const uint8_t *end_;
	const uint8_t *next_;
};

class ArrayInStream::Locator {
public:
	Locator();
	explicit Locator(ArrayInStream &stream);

	void locate() const;

private:
	ArrayInStream *stream_;
	size_t position_;
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
	typedef typename S::Locator Locator;

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
	template<typename A> inline void readAll(size_t length, A &action) {
		base_.readAll(length, action);
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

	Locator locator() { return base_.locator(); }

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
struct NameCoderImpl {
	typedef std::pair<const char8_t*, int32_t> Entry;

	struct EntryPred {
		bool operator()(const Entry &entry1, const Entry &entry2) const;
	};

	static void initialize(
			const char8_t **nameList, Entry *entryList, size_t count);

	static const char8_t* findName(
			const char8_t *const *nameList, size_t count, int32_t id,
			const char8_t *defaultName);

	static const Entry* findEntry(
			const Entry *entryList, size_t count, const char8_t *name);

	static const char8_t* removePrefix(
			const char8_t *name, size_t prefixWordCount);
};
} 

template<typename T>
struct NameCoderEntry {
	typedef T Id;
	const char8_t *name_;
	Id id_;
};

template<typename T, size_t Count>
class NameCoder {
public:
	typedef NameCoderEntry<T> Entry;

	NameCoder(const Entry (&entryList)[Count], size_t prefixWordCount);

	const char8_t* operator()(T id, const char8_t *defaultName = NULL) const;
	bool operator()(const char8_t *name, T &id) const;

	size_t getSize() const { return Count; }

private:
	typedef detail::NameCoderImpl Impl;
	typedef Impl::Entry BaseEntry;

	const char8_t *nameList_[Count];
	BaseEntry entryList_[Count];
};

#define UTIL_NAME_CODER_ENTRY_CUSTOM(name, id) { name, id }
#define UTIL_NAME_CODER_ENTRY(id) UTIL_NAME_CODER_ENTRY_CUSTOM(#id, id)
#define UTIL_NAME_CODER_NON_NAME_ENTRY(id) \
		UTIL_NAME_CODER_ENTRY_CUSTOM(NULL, id)

class GeneralNameCoder {
public:
	typedef int32_t Id;

	GeneralNameCoder();

	template<typename C>
	GeneralNameCoder(const C *baseCoder);

	const char8_t* operator()(Id id, const char8_t *defaultName = NULL) const;
	bool operator()(const char8_t *name, Id &id) const;

	size_t getSize() const;

private:
	typedef const char8_t* (*CoderFunc)(
			const void*, bool, const char8_t*, Id&);

	template<typename C>
	static const char8_t* coderFunc(
			const void *coder, bool nameResolving, const char8_t *name, Id &id);

	static const char8_t* emptyCoderFunc(
			const void *coder, bool nameResolving, const char8_t *name, Id &id);

	const void *coder_;
	CoderFunc coderFunc_;
	size_t size_;
};



#define UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR_BASE \
	UtilObjectCoder_AllocConstructorTag

#define UTIL_OBJECT_CODER_PARTIAL_OBJECT_BASE \
	UtilObjectCoder_PartialObjectTag

#define UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR \
	struct UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR_BASE {}

#define UTIL_OBJECT_CODER_PARTIAL_OBJECT \
	struct UTIL_OBJECT_CODER_PARTIAL_OBJECT_BASE {}

template<typename T, typename BaseAllocator>
class StdAllocator;

struct EnumCoder {
	typedef int32_t IntegerType;

	template<typename T> struct EncoderFunc {
		typedef const char8_t* (*Type)(T);
	};

	template<typename T> struct DecoderFunc {
		typedef bool (*Type)(const char8_t*, T&);
	};

	template<typename T, bool WithDefault, typename C> struct Base;
	template<typename T, bool D, typename C = typename EncoderFunc<T>::Type>
	struct Encoder;
	template<typename T, bool D, typename C = typename DecoderFunc<T>::Type>
	struct Decoder;

	template<typename T>
	Encoder<T, false> operator()(
			const T &value,
			typename EncoderFunc<T>::Type func = NULL,
			typename DecoderFunc<T>::Type = NULL) const;
	template<typename T, typename C>
	Encoder<T, false, const C*> operator()(const T &value, const C &coder) const;

	template<typename T>
	Encoder<T, true> operator()(
			const T &value, T defaultValue,
			typename EncoderFunc<T>::Type func = NULL,
			typename DecoderFunc<T>::Type = NULL) const;
	template<typename T, typename C>
	Encoder<T, true, const C*> operator()(
			const T &value, T defaultValue, const C &coder) const;

	template<typename T>
	Decoder<T, false> operator()(
			T &value,
			typename EncoderFunc<T>::Type = NULL,
			typename DecoderFunc<T>::Type func = NULL) const;
	template<typename T, typename C>
	Decoder<T, false, const C*> operator()(T &value, const C &coder) const;

	template<typename T>
	Decoder<T, true> operator()(
			T &value, T defaultValue,
			typename EncoderFunc<T>::Type = NULL,
			typename DecoderFunc<T>::Type func = NULL) const;
	template<typename T, typename C>
	Decoder<T, true, const C*> operator()(
			T &value, T defaultValue, const C &coder) const;
};

template<typename T, bool D, typename C>
struct EnumCoder::Base {
public:
	static const bool WITH_DEFAULT = D;

	const char8_t* getName() const;
	C withName(const char8_t *name) const;

	const T& getDefaultValue() const;

	const C& coder() const;

protected:
	explicit Base(T defaultValue);

private:
	const char8_t *name_;
	T defaultValue_;
};

template<typename T, bool D, typename C>
struct EnumCoder::Encoder : public Base< T, D, Encoder<T, D, C> > {
public:
	typedef Base< T, D, Encoder<T, D, C> > BaseType;
	typedef EnumCoder::IntegerType IntegerType;

	Encoder(const T &value, T defaultValue, C coder);

	IntegerType toInteger() const;
	const char8_t* toString() const;

	bool isDefault() const;

private:
	const T &value_;
	C coder_;
};

template<typename T, bool D, typename C>
struct EnumCoder::Decoder : public Base< T, D, Decoder<T, D, C> > {
public:
	typedef Base< T, D, Decoder<T, D, C> > BaseType;
	typedef EnumCoder::IntegerType IntegerType;

	Decoder(T &value, T defaultValue, C coder);

	void accept(IntegerType src) const;
	bool accept(const char8_t *src) const;

	void setDefault() const;

private:
	T &value_;
	C coder_;
};

template<typename T, typename D>
struct ObjectCoderOptional {
public:
	ObjectCoderOptional(T &value, const D &defaultValue, const char8_t *name);

	const ObjectCoderOptional& ref() const;

	T& getValue() const;
	const D& getDefaultValue() const;
	const char8_t* getName() const;

private:
	T &value_;
	const D &defaultValue_;
	const char8_t *name_;
};

struct ObjectCoderAttribute {
	explicit ObjectCoderAttribute(const char8_t *name = NULL);

	template<typename T>
	static ObjectCoderAttribute create(const char8_t *name, const T&);

	template<typename T, bool D, typename C>
	static ObjectCoderAttribute create(
			const char8_t*, const EnumCoder::Encoder<T, D, C> &coder);
	template<typename T, bool D, typename C>
	static ObjectCoderAttribute create(
			const char8_t*, const EnumCoder::Decoder<T, D, C> &coder);

	template<typename T, typename D>
	static ObjectCoderAttribute create(
			const char8_t*, const ObjectCoderOptional<T, D> &optional);

	const char8_t *name_;
};

template<typename C, typename Alloc, typename Base = FalseType>
class ObjectCoderBase {
public:
	typedef Alloc Allocator;
	typedef Base BaseCoder;
	typedef ObjectCoderAttribute Attribute;
	typedef ObjectCoderBase BaseType;
	typedef FalseType DefaultsEncodable;
	typedef TrueType OptionalDecodable;

	ObjectCoderBase(Allocator &alloc, const BaseCoder &baseCoder);

	Allocator& getAllocator() const;

	template<typename S, typename T>
	void encode(S &stream, const T &value) const;

	template<typename S, typename T>
	void decode(S &stream, T &value) const;

	template<typename Org, typename S, typename T, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const T &value, const Attribute &attr,
			const Traits&) const;

	template<typename Org, typename S, typename T, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, T &value, const Attribute &attr,
			const Traits&) const;

	template<typename S, typename T, typename D>
	void encodeOptional(
			S &stream, const ObjectCoderOptional<T, D> &value) const;
	template<typename S, typename T, typename D>
	void decodeOptional(
			S &stream, const ObjectCoderOptional<T, D> &value) const;

	template<typename T>
	T create() const;

	template<typename T>
	void remove(T &value) const;

protected:
	~ObjectCoderBase() {}

private:
	const C& coder() const;

	Allocator &alloc_;
	const BaseCoder &baseCoder_;
};

class ObjectCoder :
		public ObjectCoderBase< ObjectCoder, std::allocator<void> > {
public:
	struct Impl;
	struct Encoding {};
	struct Decoding {};
	struct EmptyValue { inline const EmptyValue& ref() const { return *this; } };
	template<typename S> class BaseScope;
	template<typename T, typename Char = typename T::value_type>
	class StringBuilder;
	template<typename Alloc> class AllocCoder;
	template<typename Alloc = Allocator> class NonOptionalCoder;
	class DefaultsCoder;

	enum Type {
		TYPE_NULL,
		TYPE_BOOL,
		TYPE_NUMERIC,
		TYPE_STRING,
		TYPE_BINARY,
		TYPE_LIST,
		TYPE_OBJECT,
		TYPE_OPTIONAL,
		TYPE_OPTIONAL_OBJECT,
		TYPE_ENUM
	};

	enum NumericPrecision {
		PRECISION_NORMAL,
		PRECISION_EXACT
	};

	ObjectCoder();

	template<typename Alloc>
	static AllocCoder<Alloc> withAllocator(Alloc &alloc);

	static NonOptionalCoder<> withoutOptional();
	template<typename Alloc>
	static NonOptionalCoder<Alloc> withoutOptional(Alloc &alloc);

	static DefaultsCoder withDefaults();

	template<typename T, typename D>
	static ObjectCoderOptional<T, D> createOptional(
			T &value, const D &defaultValue, const char8_t *name);

	static Allocator& getDefaultAllocator();

private:
	static Allocator defaultAlloc_;
};

struct ObjectCoder::Impl {
public:
	struct OptionalObjectCoding {};

	template<typename S> class IsCodableStream;
	template<typename T> struct RefHolder;
	template<typename T> struct GeneralHolder;
	template<typename B, bool ForInput> struct StreamHolder;

	template<typename C, typename S, typename T>
	static void encodeGeneral(C &coder, S &stream, const T &value);

	template<typename C, typename S, typename T>
	static void decodeGeneral(C &coder, S &stream, T &value);

	template<
			typename C, typename Other, typename S, typename T,
			typename Traits>
	static void encodeGeneralBy(
			C &coder, Other &otherCoder, S &stream, const T &value,
			const Attribute &attr, const Traits&);

	template<typename C, typename S, typename T, typename Traits>
	static void encodeGeneralBy(
			C &coder, const FalseType&, S &stream, const T &value,
			const Attribute &attr, const Traits&);

	template<
			typename C, typename Other, typename S, typename T,
			typename Traits>
	static void decodeGeneralBy(
			C &coder, Other &otherCoder, S &stream, T &value,
			const Attribute &attr, const Traits&);

	template<typename C, typename S, typename T, typename Traits>
	static void decodeGeneralBy(
			C &coder, const FalseType&, S &stream, T &value,
			const Attribute &attr, const Traits&);

	template<typename S, typename T>
	static void encodeNumeric(
			S &stream, const T &value, const Attribute &attr,
			NumericPrecision presicion);
	template<typename S, typename T>
	static T decodeNumeric(
			S &stream, const Attribute &attr, NumericPrecision presicion);

	template<typename C, typename S, typename T, typename D>
	static void encodeOptional(
			C &coder, S &stream, const ObjectCoderOptional<T, D> &value);
	template<typename C, typename S, typename T, typename D>
	static void decodeOptional(
			C &coder, S &stream, const ObjectCoderOptional<T, D> &value);

	template<typename C, typename T>
	static T createGeneral(C &coder);

	template<typename C, typename T>
	static void removeGeneral(C &coder, T &value);

	template<typename T>
	static void checkTooManyArguments(const T &value);

	template<typename T>
	static size_t toSize(const T &value);

	template<typename T>
	static T toUInt(size_t value);

	static const char8_t* nameBegin(const Attribute &attr);
	static const char8_t* nameEnd(const Attribute &attr);

private:
	template<Type> struct TypeTag {};
	template<typename Alloc, typename T> struct UniqueAllocValue;

	Impl();

	template<typename C, typename S, typename T, typename Traits>
	static void encodeWrapped(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TrueType&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeWrapped(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const FalseType&);
	template<typename C, typename T, typename Traits>
	static void encodeWrapped(
			C &coder, std::ostream &stream, const T &value,
			const Attribute &attr, const Traits&, const FalseType&);

	template<typename C, typename S, typename T, typename Traits>
	static void decodeWrapped(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TrueType&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeWrapped(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const FalseType&);

	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_BOOL>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_NUMERIC>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_ENUM>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_STRING>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_LIST>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OBJECT>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OPTIONAL>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OPTIONAL_OBJECT>&);
	template<typename C, typename S, typename T, typename Traits>
	static void encodeInternal(
			C &coder, S &stream, const T &value, const Attribute &attr,
			const Traits&, const FalseType&);

	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_BOOL>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_NUMERIC>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_ENUM>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_STRING>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_LIST>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OBJECT>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OPTIONAL>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const TypeTag<TYPE_OPTIONAL_OBJECT>&);
	template<typename C, typename S, typename T, typename Traits>
	static void decodeInternal(
			C &coder, S &stream, T &value, const Attribute &attr,
			const Traits&, const FalseType&);

	template<typename C, typename Alloc, typename T>
	static T* createInternal(
			C &coder, const Alloc &alloc, T**, const TrueType&);
	template<typename C, typename Alloc, typename T>
	static T createInternal(
			C &coder, const Alloc &alloc, T*, const FalseType&);
	template<typename C, typename Alloc, typename T>
	static T* createInternal(
			C &coder, const Alloc &alloc, const TrueType&, const TrueType&);
	template<typename C, typename Alloc, typename T>
	static T* createInternal(
			C &coder, const Alloc &alloc, const TrueType&, const FalseType&);
	template<typename C, typename Alloc, typename T>
	static T createInternal(
			C &coder, const Alloc &alloc, const FalseType&, const TrueType&);
	template<typename C, typename Alloc, typename T>
	static T createInternal(
			C &coder, const Alloc &alloc, const FalseType&, const FalseType&);

	template<typename Alloc, typename T>
	static void removeInternal(const Alloc &alloc, T *&value, const TrueType&);
	template<typename Alloc, typename T>
	static void removeInternal(const Alloc &alloc, T &value, const FalseType&);

	template<typename C, typename T>
	static typename C::Allocator::template rebind<T>::other
	getAllocatorInternal(
			C &coder,
			typename C::Allocator::template rebind<int>::other::value_type*);

	template<typename C, typename T>
	static StdAllocator<T, typename C::Allocator>
	getAllocatorInternal(C &coder, void*);

	template<typename S>
	static TrueType detectStream(typename EnableIf<
			!IsSame<typename S::ObjectScope, void>::VALUE &&
			!IsSame<typename S::ListScope, void>::VALUE, int>::Type*) {
		return TrueType();
	}

	template<typename S>
	static FalseType detectStream(void*) {
		return FalseType();
	}

	template<typename T, typename Traits>
	static TrueType detectDigits(typename EnableIf<
			(std::numeric_limits<T>::digits > 0), int>::Type*) {
		UTIL_STATIC_ASSERT((IsSame<Traits, int>::VALUE));
		return TrueType();
	}

	template<typename T, typename Traits>
	static FalseType detectDigits(void*) {
		return FalseType();
	}

	template<typename T>
	static TrueType detectIterable(typename EnableIf<
			!IsSame<typename T::allocator_type, void>::VALUE &&
			!IsSame<typename T::const_iterator, void>::VALUE &&
			!IsSame<typename T::value_type, void>::VALUE, int>::Type*) {
		return TrueType();
	}

	template<typename T>
	static FalseType detectIterable(void*) {
		return FalseType();
	}

	template<typename T>
	static TrueType detectString(typename EnableIf<
			!IsSame<typename T::traits_type, void>::VALUE, int>::Type*) {
		return TrueType();
	}

	template<typename T>
	static FalseType detectString(void*) {
		return FalseType();
	}

	static TypeTag<TYPE_BOOL> detectType(
			const bool&, const TrueType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_BOOL>();
	}

	template<typename T>
	static TypeTag<TYPE_NUMERIC> detectType(
			const T&, const TrueType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_NUMERIC>();
	}

	template<typename T, bool D, typename C>
	static TypeTag<TYPE_ENUM> detectType(
			const EnumCoder::Encoder<T, D, C>&,
			const FalseType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_ENUM>();
	}

	template<typename T, bool D, typename C>
	static TypeTag<TYPE_ENUM> detectType(
			const EnumCoder::Decoder<T, D, C>&,
			const FalseType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_ENUM>();
	}

	template<typename T>
	static TypeTag<TYPE_STRING> detectType(
			const T&, const FalseType&, const TrueType&, const TrueType&) {
		return TypeTag<TYPE_STRING>();
	}

	template<typename T>
	static TypeTag<TYPE_LIST> detectType(
			const T&, const FalseType&, const TrueType&, const FalseType&) {
		return TypeTag<TYPE_LIST>();
	}

	template<typename T>
	static TypeTag<TYPE_OBJECT> detectType(
			const T&, const FalseType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_OBJECT>();
	}

	template<typename T, typename D>
	static TypeTag<TYPE_OPTIONAL> detectType(
			const ObjectCoderOptional<T, D>&,
			const FalseType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_OPTIONAL>();
	}

	template<typename T>
	static TypeTag<TYPE_OPTIONAL_OBJECT> detectType(
			T* const&, const FalseType&, const FalseType&, const FalseType&) {
		return TypeTag<TYPE_OPTIONAL_OBJECT>();
	}

	static FalseType detectType(
			const EmptyValue&,
			const FalseType&, const FalseType&, const FalseType&) {
		return FalseType();
	}

	template<typename T>
	static TrueType detectPointer(T**) {
		return TrueType();
	}

	template<typename T>
	static FalseType detectPointer(T*) {
		return FalseType();
	}

	template<typename T>
	static TrueType detectAllocConstructor(
			typename EnableIf<!IsSame<
					typename T::UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR_BASE,
					void>::VALUE, int>::Type*) {
		return TrueType();
	}

	template<typename T>
	static FalseType detectAllocConstructor(void*) {
		return FalseType();
	}

	template<typename T>
	static TrueType detectPartialObject(
			typename EnableIf<!IsSame<
					typename T::UTIL_OBJECT_CODER_PARTIAL_OBJECT_BASE,
					void>::VALUE, int>::Type*) {
		return TrueType();
	}

	template<typename T>
	static FalseType detectPartialObject(void*) {
		return FalseType();
	}

	template<typename R1, typename R2>
	static TrueType detectOr(const R1&, const R2&) {
		return TrueType();
	}

	static FalseType detectOr(const FalseType&, const FalseType&) {
		return FalseType();
	}

	static int filterTraitsForMembers(const OptionalObjectCoding&) {
		return int();
	}

	template<typename Traits>
	static Traits filterTraitsForMembers(const Traits&) {
		return Traits();
	}

	template<typename D, typename S>
	static D convertUInt(const S &src, const TrueType&);

	template<typename D, typename S>
	static D convertUInt(const S &src, const FalseType&);

	static void errorOutOfRange();
	static void errorUnexpectedType();
};

template<typename S>
class ObjectCoder::Impl::IsCodableStream {
private:
	typedef int TrueResult;
	struct FalseResult { TrueResult i, j; };

	template<typename T>
	static TrueResult check(
			T*,
			typename T::ObjectScope* = NULL,
			typename T::ValueScope* = NULL,
			typename T::ListScope* = NULL);
	static FalseResult check(...);

public:
	typedef typename util::BoolType<
			sizeof(check(static_cast<S*>(NULL))) ==
			sizeof(TrueResult)>::Result Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename T>
struct ObjectCoder::Impl::RefHolder {
	typedef T Type;

	explicit RefHolder(Type &base) : ref_(base) {
	}

	Type& operator()() const { return ref_; }

	Type &ref_;
};

template<typename T>
struct ObjectCoder::Impl::GeneralHolder {
	typedef T Type;

	template<typename B> explicit GeneralHolder(B &base) :
			value_(base), valueRef_(value_) {
	}

	GeneralHolder(const GeneralHolder &another) :
			value_(another.value_), valueRef_(value_) {
	}

	GeneralHolder& operator=(const GeneralHolder &another) {
		value_ = another.value_;
		return *this;
	}

	T& operator()() const { return valueRef_; }

	T value_;
	T &valueRef_;
};

template<typename S> class ObjectInStream;
template<typename S> class ObjectOutStream;
class ObjectTextOutStream;

template<typename B, bool ForInput>
struct ObjectCoder::Impl::StreamHolder {
	enum {
		STREAM_CODABLE = IsCodableStream<B>::VALUE,
		STREAM_TEXTUAL = BaseOf<std::ios_base, B>::Result::VALUE
	};

	typedef typename Conditional<
			!STREAM_CODABLE, ObjectInStream<B>,
			void>::Type InWrapper;

	typedef typename Conditional<
			STREAM_TEXTUAL, ObjectTextOutStream, typename Conditional<
			!STREAM_CODABLE, ObjectOutStream<B>,
			void>::Type>::Type OutWrapper;

	typedef typename Conditional<
			ForInput, InWrapper, OutWrapper>::Type Wrapper;

	typedef typename Conditional<
			IsSame<Wrapper, void>::VALUE,
			RefHolder<B>, GeneralHolder<Wrapper> >::Type BaseHolder;

	typedef typename BaseHolder::Type Stream;

	explicit StreamHolder(B &base) : base_(base) {}

	Stream& operator()() const { return base_(); }

	BaseHolder base_;
};

template<typename Alloc, typename T>
struct ObjectCoder::Impl::UniqueAllocValue {
public:
	UniqueAllocValue(Alloc &alloc, T *value);
	~UniqueAllocValue();

	void reset();
	T* get() throw();
	T* release() throw();

private:
	Alloc &alloc_;
	T *value_;
};

template<typename S>
class ObjectCoder::BaseScope {
public:
	typedef S Stream;

	BaseScope(Stream &base, const Attribute&);
	BaseScope(Stream &base, Type, const Attribute&);
	BaseScope(Stream &base, size_t, const Attribute&);

	Stream& stream();

private:
	Stream &base_;
};

template<typename T, typename Char>
class ObjectCoder::StringBuilder {
public:
	explicit StringBuilder(T &dest);

	void operator()(size_t) {}
	void operator()(const void *data, size_t size);

private:
	typedef Char CharType;
	T &dest_;
};

template<typename Alloc>
class ObjectCoder::AllocCoder :
		public ObjectCoderBase<AllocCoder<Alloc>, Alloc> {
public:
	typedef Alloc Allocator;
	typedef ObjectCoderBase<AllocCoder, Alloc> BaseType;

	explicit AllocCoder(Allocator &alloc);
};

template<typename Alloc>
class ObjectCoder::NonOptionalCoder :
		public ObjectCoderBase<NonOptionalCoder<Alloc>, Alloc> {
public:
	typedef FalseType OptionalDecodable;
	typedef ObjectCoderBase<NonOptionalCoder<Alloc>, Alloc> BaseType;

	explicit NonOptionalCoder(Allocator &alloc);
};

class ObjectCoder::DefaultsCoder :
		public ObjectCoderBase<DefaultsCoder, Allocator> {
public:
	typedef TrueType DefaultsEncodable;
	typedef ObjectCoderBase<DefaultsCoder, Allocator> BaseType;

	DefaultsCoder();
};

template<typename S>
class ObjectInStream {
public:
	typedef typename S::Locator Locator;
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;
	typedef ObjectCoder::NumericPrecision NumericPrecision;
	typedef ObjectCoder::BaseScope<ObjectInStream> ObjectScope;

	class Scope;

	typedef Scope ValueScope;
	typedef Scope ListScope;

	explicit ObjectInStream(S &base);

	void reserve(size_t size);

	bool peekType(Type &type, const Attribute&);

	bool readBool(const Attribute&);
	template<typename T> T readNumeric(
			const Attribute&,
			NumericPrecision = ObjectCoder::PRECISION_NORMAL);
	template<typename T> void readNumericGeneral(
			T &value, const Attribute&, NumericPrecision);
	template<typename T> void readEnum(T &value, const Attribute&);
	template<typename A> void readString(A &action, const Attribute&);
	template<typename A> void readBinary(A &action, const Attribute&);

	Locator locator();

private:
	S &base_;
};

template<typename S>
class ObjectInStream<S>::Scope :
		public ObjectCoder::BaseScope< ObjectInStream<S> > {
public:
	typedef ObjectCoder::BaseScope< ObjectInStream<S> > BaseType;

	Scope(ObjectInStream &base, size_t &size, const Attribute&);
	Scope(ObjectInStream &base, Type &type, const Attribute&);
};

template<typename S>
class ObjectOutStream {
public:
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;
	typedef ObjectCoder::NumericPrecision NumericPrecision;
	typedef ObjectCoder::BaseScope<ObjectOutStream> ObjectScope;

	class Scope;

	typedef Scope ValueScope;
	typedef Scope ListScope;

	explicit ObjectOutStream(S &base);

	bool isTyped() const { return false; }
	void reserve(size_t size);

	void writeBool(bool value, const Attribute&);
	template<typename T> void writeNumeric(
			const T &value, const Attribute&,
			NumericPrecision = ObjectCoder::PRECISION_NORMAL);
	template<typename T> void writeNumericGeneral(
			const T &value, const Attribute&, NumericPrecision);
	template<typename T> void writeEnum(const T &value, const Attribute&);
	void writeString(const char8_t *data, size_t size, const Attribute&);
	void writeBinary(const void *data, size_t size, const Attribute&);

private:
	S &base_;
};

template<typename S>
class ObjectOutStream<S>::Scope :
		public ObjectCoder::BaseScope< ObjectOutStream<S> > {
public:
	typedef ObjectCoder::BaseScope< ObjectOutStream<S> > BaseType;

	Scope(ObjectOutStream &base, size_t size, const Attribute&);
	Scope(ObjectOutStream &base, Type type, const Attribute&);
};

struct ObjectFormatter {
public:
	template<typename T, typename C = ObjectCoder> struct Base;

	template<typename T>
	Base<T> operator()(const T &value) const;

	template<typename T, typename C>
	Base<T, C> operator()(const T &value, C &coder) const;

private:
	static ObjectCoder defaultCoder_;
};

template<typename T, typename C>
struct ObjectFormatter::Base {
public:
	Base(const T &value, C &coder);
	void format(std::ostream &stream) const;

private:
	const T &value_;
	C &coder_;
};

template<typename T, typename C>
std::ostream& operator<<(
		std::ostream &s, const ObjectFormatter::Base<T, C> &formatterBase);

class ObjectTextOutStream {
public:
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;
	typedef ObjectCoder::NumericPrecision NumericPrecision;

	class Scope;
	class ValueScope;

	typedef Scope ListScope;
	typedef Scope ObjectScope;

	explicit ObjectTextOutStream(std::ostream &base);

	void setSingleLine(bool singleLine);
	void setNullVisible(bool nullVisible);

	bool isTyped() const { return true; }
	void reserve(size_t size);

	void writeBool(bool value, const Attribute &attr);
	template<typename T>
	void writeNumeric(
			const T &value, const Attribute &attr,
			NumericPrecision = ObjectCoder::PRECISION_NORMAL);
	template<typename T>
	void writeNumericGeneral(
			const T &value, const Attribute &attr, NumericPrecision);
	template<typename T> void writeEnum(const T &value, const Attribute &attr);
	void writeString(const char8_t *data, size_t size, const Attribute &attr);
	void writeBinary(const void *data, size_t size, const Attribute&);

private:
	void writeType(Type value, const Attribute &attr);

	void writeHead(const Attribute &attr, bool withSeparator);
	void writeTail();

	void push(const Attribute &attr, bool listing);
	void pop();

	std::ostream &base_;
	size_t nestLevel_;
	size_t index_;
	size_t indentSize_;
	Attribute lastAttr_;
	bool listing_;
	bool singleLine_;
	bool nullVisible_;
};

class ObjectTextOutStream::Scope {
public:
	typedef ObjectTextOutStream Stream;

	Scope(Stream &base, const Attribute &attr);
	Scope(Stream &base, size_t, const Attribute &attr);
	~Scope();

	Stream& stream();

private:
	Stream base_;
};

class ObjectTextOutStream::ValueScope :
		public ObjectCoder::BaseScope<ObjectTextOutStream> {
public:
	typedef ObjectCoder::BaseScope<ObjectTextOutStream> BaseType;

	ValueScope(ObjectTextOutStream &base, Type type, const Attribute &attr);
	~ValueScope();

private:
	const Attribute orgAttr_;
};

class AbstractNumericEncoder {
public:
	template<typename B> class Wrapper;

	virtual ~AbstractNumericEncoder();

	virtual bool isInteger() const = 0;
	virtual std::pair<const void*, size_t> getBinary() const = 0;
	virtual int64_t getInt64() const = 0;
	virtual double getDouble() const = 0;
};

template<typename B>
class AbstractNumericEncoder::Wrapper : public AbstractNumericEncoder {
public:
	typedef B Base;

	explicit Wrapper(const Base &base);
	virtual ~Wrapper();

	virtual bool isInteger() const;
	virtual std::pair<const void*, size_t> getBinary() const;
	virtual int64_t getInt64() const;
	virtual double getDouble() const;

private:
	const Base &base_;
};

class AbstractNumericDecoder {
public:
	template<typename B> class Wrapper;

	virtual ~AbstractNumericDecoder();

	virtual bool isInteger() const = 0;
	virtual std::pair<void*, size_t> getBinary() const = 0;
	virtual void accept(int64_t value) const = 0;
	virtual void accept(double value) const = 0;
};

template<typename B>
class AbstractNumericDecoder::Wrapper : public AbstractNumericDecoder {
public:
	typedef B Base;

	explicit Wrapper(Base &base);
	virtual ~Wrapper();

	virtual bool isInteger() const;
	virtual std::pair<void*, size_t> getBinary() const;
	virtual void accept(int64_t value) const;
	virtual void accept(double value) const;

private:
	Base &base_;
};

class AbstractEnumEncoder {
public:
	typedef EnumCoder::IntegerType IntegerType;

	template<typename B> class Wrapper;

	virtual ~AbstractEnumEncoder();

	virtual IntegerType toInteger() const = 0;
	virtual const char8_t* toString() const = 0;
};

template<typename B>
class AbstractEnumEncoder::Wrapper : public AbstractEnumEncoder {
public:
	typedef B Base;

	explicit Wrapper(Base &base);
	virtual ~Wrapper();

	virtual IntegerType toInteger() const;
	virtual const char8_t* toString() const;

private:
	Base &base_;
};

class AbstractEnumDecoder {
public:
	typedef EnumCoder::IntegerType IntegerType;

	template<typename B> class Wrapper;

	virtual ~AbstractEnumDecoder();

	virtual void accept(IntegerType value) const = 0;
	virtual bool accept(const char8_t *value) const = 0;
};

template<typename B>
class AbstractEnumDecoder::Wrapper : public AbstractEnumDecoder {
public:
	typedef B Base;

	explicit Wrapper(Base &base);
	virtual ~Wrapper();

	virtual void accept(IntegerType value) const;
	virtual bool accept(const char8_t *value) const;

private:
	Base &base_;
};

class AbstractObjectInStream;
class AbstractObjectOutStream;

class AbstractObjectStream {
public:
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;

	template<typename Stream> class Scope;

private:
	friend class AbstractObjectInStream;
	friend class AbstractObjectOutStream;

	typedef uint64_t StreamStorage[8];

	static void* getRawAddress(void *ptr);
	static const void* getRawAddress(const void *ptr);
};

template<typename Stream>
class AbstractObjectStream::Scope {
public:
	typedef typename Stream::TypeArg TypeArg;
	typedef typename Stream::SizeArg SizeArg;

	Scope(Stream &base, const Attribute &attr);
	Scope(Stream &base, TypeArg type, const Attribute &attr);
	Scope(Stream &base, SizeArg size, const Attribute &attr);
	~Scope();

	Scope(const Scope &another);
	Scope& operator=(const Scope &another);

	Stream& stream();
	const Stream& stream() const;

private:
	StreamStorage storage_;
};

class AbstractObjectInStream {
public:
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;
	typedef ObjectCoder::NumericPrecision NumericPrecision;

	typedef AbstractObjectStream::Scope<AbstractObjectInStream> Scope;

	class Action;
	class Locator;
	class BaseLocator;

	typedef Scope ObjectScope;
	typedef Scope ValueScope;
	typedef Scope ListScope;

	typedef Type &TypeArg;
	typedef size_t &SizeArg;

	template<typename B, typename S = ObjectCoder::EmptyValue> class Wrapper;
	template<typename B> class ActionWrapper;
	template<typename B> class LocatorWrapper;

	virtual ~AbstractObjectInStream();

	virtual void reserve(size_t size) = 0;

	virtual bool peekType(Type &type, const Attribute &attr) = 0;

	virtual bool readBool(const Attribute &attr) = 0;

	template<typename T> T readNumeric(
			const Attribute &attr,
			NumericPrecision precision = ObjectCoder::PRECISION_NORMAL);
	virtual void readNumericGeneral(
			const AbstractNumericDecoder &value, const Attribute &attr,
			NumericPrecision precision) = 0;

	template<typename T> void readEnum(T &value, const Attribute &attr);
	virtual void readEnumGeneral(
			const AbstractEnumDecoder &value, const Attribute &attr) = 0;

	template<typename A> void readString(A &action, const Attribute &attr);
	virtual void readStringGeneral(Action &action, const Attribute &attr) = 0;

	template<typename A> void readBinary(A &action, const Attribute &attr);
	virtual void readBinaryGeneral(Action &action, const Attribute &attr) = 0;

	Locator locator();

protected:
	friend class AbstractObjectStream::Scope<AbstractObjectInStream>;

	typedef AbstractObjectStream::StreamStorage StreamStorage;
	typedef uint64_t LocatorStorage[4];

	virtual void duplicate(StreamStorage &storage) const = 0;

	virtual void pushObject(
			StreamStorage &storage, const Attribute &attr) = 0;
	virtual void pushValue(
			StreamStorage &storage, Type &type, const Attribute &attr) = 0;
	virtual void pushList(
			StreamStorage &storage, size_t &size, const Attribute &attr) = 0;

	virtual BaseLocator* createLocator(LocatorStorage &storage) = 0;

private:
	static void* getRawAddress(void *ptr) {
		return AbstractObjectStream::getRawAddress(ptr);
	}
};

class AbstractObjectInStream::Action {
public:
	virtual ~Action();
	virtual void operator()(size_t size) = 0;
	virtual void operator()(const void *data, size_t size) = 0;
};

class AbstractObjectInStream::Locator {
public:
	Locator();
	~Locator();

	explicit Locator(AbstractObjectInStream &stream);

	Locator(const Locator &another);
	Locator& operator=(const Locator &another);

	void locate() const;

protected:
	explicit Locator(BaseLocator *base);

private:
	void clear();

	BaseLocator *base_;
	LocatorStorage storage_;
};

class AbstractObjectInStream::BaseLocator {
public:
	virtual ~BaseLocator();
	virtual BaseLocator* duplicate(LocatorStorage &storage) const = 0;
	virtual void locate() const = 0;
};

template<typename B, typename S>
class AbstractObjectInStream::Wrapper : public AbstractObjectInStream {
public:
	typedef B Base;
	typedef S BaseScope;

	explicit Wrapper(Base &base);
	virtual ~Wrapper();

	template<typename Other>
	Wrapper(Other &base, const Attribute &attr);
	template<typename Other>
	Wrapper(Other &base, Type &type, const Attribute &attr);
	template<typename Other>
	Wrapper(Other &base, size_t &size, const Attribute &attr);

	virtual void reserve(size_t size);

	virtual bool peekType(Type &type, const Attribute &attr);

	virtual bool readBool(const Attribute &attr);
	virtual void readNumericGeneral(
			const AbstractNumericDecoder &value, const Attribute &attr,
			NumericPrecision precision);
	virtual void readEnumGeneral(
			const AbstractEnumDecoder &value, const Attribute &attr);

	virtual void readStringGeneral(Action &action, const Attribute &attr);
	virtual void readBinaryGeneral(Action &action, const Attribute &attr);

protected:
	virtual void duplicate(StreamStorage &storage) const;

	virtual void pushObject(
			StreamStorage &storage, const Attribute &attr);
	virtual void pushValue(
			StreamStorage &storage, Type &type, const Attribute &attr);
	virtual void pushList(
			StreamStorage &storage, size_t &size, const Attribute &attr);

	virtual BaseLocator* createLocator(LocatorStorage &storage);

private:
	BaseScope scope_;
	Base &base_;
};

template<typename B>
class AbstractObjectInStream::ActionWrapper : public Action {
public:
	typedef B Base;

	explicit ActionWrapper(Base &base);
	virtual ~ActionWrapper();

	virtual void operator()(size_t size);
	virtual void operator()(const void *data, size_t size);

private:
	Base &base_;
};

template<typename B>
class AbstractObjectInStream::LocatorWrapper : public BaseLocator {
public:
	typedef B BaseStream;
	typedef typename BaseStream::Locator Base;

	explicit LocatorWrapper(BaseStream &stream);
	virtual ~LocatorWrapper();

	virtual BaseLocator* duplicate(LocatorStorage &storage) const;
	virtual void locate() const;

private:
	Base base_;
};

class AbstractObjectOutStream {
public:
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoder::Type Type;
	typedef ObjectCoder::NumericPrecision NumericPrecision;

	typedef AbstractObjectStream::Scope<AbstractObjectOutStream> Scope;

	class NumericEncoder;
	class EnumEncoder;

	typedef Scope ObjectScope;
	typedef Scope ValueScope;
	typedef Scope ListScope;

	typedef Type TypeArg;
	typedef size_t SizeArg;

	template<typename B, typename S = ObjectCoder::EmptyValue> class Wrapper;

	virtual ~AbstractObjectOutStream();

	virtual bool isTyped() const = 0;
	virtual void reserve(size_t size) = 0;

	virtual void writeBool(bool value, const Attribute &attr) = 0;

	template<typename T>
	void writeNumeric(
			const T &value, const Attribute &attr,
			NumericPrecision precision = ObjectCoder::PRECISION_NORMAL);
	virtual void writeNumericGeneral(
			const AbstractNumericEncoder &value, const Attribute &attr,
			NumericPrecision precision) = 0;

	template<typename T> void writeEnum(const T &value, const Attribute &attr);
	virtual void writeEnumGeneral(
			const AbstractEnumEncoder &value, const Attribute &attr) = 0;

	virtual void writeString(
			const char8_t *data, size_t size, const Attribute &attr) = 0;
	virtual void writeBinary(
			const void *data, size_t size, const Attribute &attr) = 0;

protected:
	friend class AbstractObjectStream::Scope<AbstractObjectOutStream>;

	typedef AbstractObjectStream::StreamStorage StreamStorage;

	virtual void duplicate(StreamStorage &storage) const = 0;

	virtual void pushObject(
			StreamStorage &storage, const Attribute &attr) = 0;
	virtual void pushValue(
			StreamStorage &storage, Type type, const Attribute &attr) = 0;
	virtual void pushList(
			StreamStorage &storage, size_t size, const Attribute &attr) = 0;

private:
	static void* getRawAddress(void *ptr) {
		return AbstractObjectStream::getRawAddress(ptr);
	}
};

template<typename B, typename S>
class AbstractObjectOutStream::Wrapper : public AbstractObjectOutStream {
public:
	typedef B Base;
	typedef S BaseScope;

	explicit Wrapper(Base &base);
	virtual ~Wrapper();

	template<typename Other>
	Wrapper(Other &base, const Attribute &attr);
	template<typename Other>
	Wrapper(Other &base, Type type, const Attribute &attr);
	template<typename Other>
	Wrapper(Other &base, size_t size, const Attribute &attr);

	virtual bool isTyped() const;
	virtual void reserve(size_t size);

	virtual void writeBool(bool value, const Attribute &attr);
	virtual void writeNumericGeneral(
			const AbstractNumericEncoder &value, const Attribute &attr,
			NumericPrecision precision);
	virtual void writeEnumGeneral(
			const AbstractEnumEncoder &value, const Attribute &attr);

	virtual void writeString(
			const char8_t *data, size_t size, const Attribute &attr);
	virtual void writeBinary(
			const void *data, size_t size, const Attribute &attr);

protected:
	virtual void duplicate(StreamStorage &storage) const;

	virtual void pushObject(
			StreamStorage &storage, const Attribute &attr);
	virtual void pushValue(
			StreamStorage &storage, Type type, const Attribute &attr);
	virtual void pushList(
			StreamStorage &storage, size_t size, const Attribute &attr);

private:
	BaseScope scope_;
	Base &base_;
};

class AbstractObjectCoder {
public:
	template<typename Base> class Wrapper;
	template<typename Base> Wrapper<Base> operator()(const Base &base) const;

	template<bool ForInput> struct SelectorBase;

	typedef SelectorBase<true> In;
	typedef SelectorBase<false> Out;
};

template<typename Base>
class AbstractObjectCoder::Wrapper : public ObjectCoderBase<
		AbstractObjectCoder::Wrapper<Base>, typename Base::Allocator, Base> {
public:
	typedef Base BaseCoder;
	typedef typename Base::Allocator Allocator;
	typedef ObjectCoder::Attribute Attribute;
	typedef ObjectCoderBase<
			Wrapper<Base>, typename Base::Allocator, Base> BaseType;

	explicit Wrapper(const BaseCoder &base);

	template<typename S, typename T>
	void encode(S &stream, const T &value) const;

	template<typename S, typename T>
	void decode(S &stream, T &value) const;
};

template<bool ForInput>
struct AbstractObjectCoder::SelectorBase {
	typedef typename Conditional<
			ForInput,
			AbstractObjectInStream,
			AbstractObjectOutStream>::Type BaseStream;

	template<typename B>
	struct WrapperHolder {
		typedef BaseStream Type;

		typedef typename ObjectCoder::Impl::StreamHolder<
				B, ForInput> GeneralHolder;
		typedef typename GeneralHolder::Stream GeneralStream;

		typedef typename Type::template Wrapper<GeneralStream> StreamWrapper;
		typedef typename ObjectCoder::Impl::GeneralHolder<
				StreamWrapper> BaseHolder;

		explicit WrapperHolder(B &base) : general_(base), base_(general_()) {
		}

		WrapperHolder(const WrapperHolder &another) :
				general_(another.general_), base_(general_()) {
		}

		WrapperHolder& operator=(const WrapperHolder &another) {
			general_ = another.general_;
			base_ = BaseHolder(general_());
			return *this;
		}

		Type& operator()() const { return base_(); }

		GeneralHolder general_;
		BaseHolder base_;
	};

	template<typename B, typename Other>
	struct Holder {
		typedef typename Conditional<
				BaseOf<BaseStream, B>::Result::VALUE ||
				(BaseOf<Other, B>::Result::VALUE &&
						!IsSame<Other, void>::VALUE),
				ObjectCoder::Impl::RefHolder<B>,
				WrapperHolder<B> >::Type BaseHolder;
		typedef typename BaseHolder::Type Stream;

		explicit Holder(B &base) : base_(base) {}

		Stream& operator()() const { return base_(); }

		BaseHolder base_;
	};

	template<typename Other = void>
	struct Selector {
		template<typename B> Holder<B, Other> operator()(B &base) {
			return Holder<B, Other>(base);
		}
	};
};

template<typename T>
class IsAllocCodableObject {
private:
	typedef int TrueResult;
	struct FalseResult { TrueResult i, j; };

	template<typename V>
	static TrueResult check(
			V*, typename V::UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR_BASE* = NULL);
	static FalseResult check(...);

public:
	typedef typename util::BoolType<
			sizeof(check(static_cast<T*>(NULL))) ==
			sizeof(TrueResult)>::Result Type;
	enum Value { VALUE = Type::VALUE };
};

template<typename T, bool AllocCodable = IsAllocCodableObject<T>::VALUE>
struct PartialCodableObject : public T {
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	template<typename Alloc>
	explicit PartialCodableObject(Alloc &alloc) : T(alloc) {}
	explicit PartialCodableObject(const T &base) : T(base) {}
	PartialCodableObject() {}
};

template<typename T>
struct PartialCodableObject<T, false> : public T {
	UTIL_OBJECT_CODER_PARTIAL_OBJECT;

	explicit PartialCodableObject(const T &base = T()) : T(base) {}
};

template<typename Base, typename V, typename C>
class CustomObjectCoder : public ObjectCoderBase<
		CustomObjectCoder<Base, V, C>, typename Base::Allocator, Base> {
public:
	typedef Base BaseCoder;
	typedef typename Base::Allocator Allocator;
	typedef typename Base::Attribute Attribute;
	typedef ObjectCoderBase<
			CustomObjectCoder<Base, V, C>, Allocator, Base> BaseType;

	CustomObjectCoder(const BaseCoder &base, const C &customBase) :
			BaseType(base.getAllocator(), base),
			customBase_(customBase) {
	}

	template<typename Org, typename S, typename T, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::encodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename T, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::decodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const V &value, const Attribute &attr,
			const Traits&) const {
		customBase_.encodeBy(*this, orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, V &value, const Attribute &attr,
			const Traits&) const {
		customBase_.decodeBy(*this, orgCoder, stream, value, attr, Traits());
	}

private:
	C customBase_;
};

#define UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, member) \
	do { \
		(coder).op( \
				(coder), \
				(stream), \
				(member), \
				util::ObjectCoder::Attribute::create(#member, (member)), \
				(traits)); \
	} \
	while (false)

#define UTIL_OBJECT_CODER_MEMBERS_BASE8( \
	op, coder, stream, traits, m1, m2, m3, m4, m5, m6, m7, m8) \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m1); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m2); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m3); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m4); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m5); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m6); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m7); \
	UTIL_OBJECT_CODER_MEMBERS_BASE(op, coder, stream, traits, m8)

#define UTIL_OBJECT_CODER_MEMBER_DEFAULTS4 \
	util::ObjectCoder::EmptyValue().ref(), \
	util::ObjectCoder::EmptyValue().ref(), \
	util::ObjectCoder::EmptyValue().ref(), \
	util::ObjectCoder::EmptyValue().ref()

#define UTIL_OBJECT_CODER_MEMBER_DEFAULTS16 \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS4, \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS4, \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS4, \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS4

#define UTIL_OBJECT_CODER_MEMBER_DEFAULTS32 \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS16, \
	UTIL_OBJECT_CODER_MEMBER_DEFAULTS16

#define UTIL_OBJECT_CODER_MEMBERS_OP3( \
	op, coder, stream, traits, \
	m1, m2, m3, m4, m5, m6, m7, m8, \
	m9, m10, m11, m12, m13, m14, m15, m16, \
	m17, m18, m19, m20, m21, m22, m23, m24, \
	m25, m26, m27, m28, m29, m30, m31, m32, m33, ...) \
	UTIL_OBJECT_CODER_MEMBERS_BASE8(op, coder, stream, traits, \
			m1, m2, m3, m4, m5, m6, m7, m8); \
	UTIL_OBJECT_CODER_MEMBERS_BASE8(op, coder, stream, traits, \
			m9, m10, m11, m12, m13, m14, m15, m16); \
	UTIL_OBJECT_CODER_MEMBERS_BASE8(op, coder, stream, traits, \
			m17, m18, m19, m20, m21, m22, m23, m24); \
	UTIL_OBJECT_CODER_MEMBERS_BASE8(op, coder, stream, traits, \
			m25, m26, m27, m28, m29, m30, m31, m32); \
	do { \
		util::ObjectCoder::Impl::checkTooManyArguments((m33)); \
	} \
	while (false)

#define UTIL_OBJECT_CODER_MEMBERS_OP2(tuple) \
	UTIL_OBJECT_CODER_MEMBERS_OP3 tuple

#define UTIL_OBJECT_CODER_MEMBERS_OP1(op, coder, stream, traits, ...) \
	UTIL_OBJECT_CODER_MEMBERS_OP2(( \
			op, coder, stream, traits, __VA_ARGS__, \
			UTIL_OBJECT_CODER_MEMBER_DEFAULTS32, \
			util::ObjectCoder::EmptyValue(), \
			))

#define UTIL_OBJECT_CODER_MEMBERS(...) \
	template<typename C, typename S, typename Traits> \
	void operator()( \
			C &coder, S &stream, const Traits &traits, \
			const util::ObjectCoder::Encoding&) const { \
		UTIL_OBJECT_CODER_MEMBERS_OP1( \
				encodeBy, coder, stream, traits, __VA_ARGS__); \
	} \
	template<typename C, typename S, typename Traits> \
	void operator()( \
			C &coder, S &stream, const Traits &traits, \
			const util::ObjectCoder::Decoding&) { \
		UTIL_OBJECT_CODER_MEMBERS_OP1( \
				decodeBy, coder, stream, traits, __VA_ARGS__); \
	}

#define UTIL_OBJECT_CODER_ENUM_NAME2(name, ...) #name

#define UTIL_OBJECT_CODER_ENUM_NAME1(tuple) \
	UTIL_OBJECT_CODER_ENUM_NAME2 tuple

#define UTIL_OBJECT_CODER_ENUM(...) \
	util::EnumCoder()(__VA_ARGS__).withName( \
			UTIL_OBJECT_CODER_ENUM_NAME1((__VA_ARGS__, int()))).coder()

#define UTIL_OBJECT_CODER_OPTIONAL(value, defaultValue) \
	util::ObjectCoder::createOptional((value), (defaultValue), #value).ref()




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
struct StrictParser {
	bool operator()(const std::string &src, T &dest) const;
};

template<typename T>
bool StrictParser<T>::operator()(const std::string &src, T &dest) const {
	for (;;) {
		util::NormalIStringStream iss(src);
		iss.peek();
		if (iss.eof()) {
			break;
		}

		iss.unsetf(std::ios::skipws);
		if (std::numeric_limits<T>::is_integer &&
				sizeof(T) < sizeof(int32_t)) {
			int32_t int32Dest;
			iss >> int32Dest;
			if (iss.bad() || int32Dest <
					static_cast<int32_t>(std::numeric_limits<T>::min()) ||
					int32Dest >
					static_cast<int32_t>(std::numeric_limits<T>::max()) ) {
				break;
			}
			dest = static_cast<T>(int32Dest);
		}
		else {
			iss >> dest;
		}

		if (iss.bad()) {
			break;
		}
		if (!iss.eof()) {
			break;
		}

		if (std::numeric_limits<T>::is_integer) {
			util::NormalOStringStream oss;
			oss << dest;
			if (iss.str() != oss.str()) {
				break;
			}
		}

		return true;
	}

	dest = T();
	return false;
}

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

template< typename T, typename P = StrictParser<T> >
struct StrictValueParserTraits {
	typedef P Parser;
};
template<> struct StrictValueParserTraits<bool> {
	typedef BoolParser Parser; };

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
inline bool StrictLexicalConverter<T>::operator()(const char8_t *src, T &dest) {
	typedef typename detail::StrictValueParserTraits<T>::Parser Parser;
	return Parser()(src, dest);
}

template<typename T>
inline T StrictLexicalConverter<T>::operator()(const char8_t *src) {
	T dest;
	if (!this->operator()(src, dest)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_PARAMETER);
	}

	return dest;
}

template<typename T>
inline bool StrictLexicalConverter<T>::operator()(const u8string &src, T &dest) {
	typedef typename detail::StrictValueParserTraits<T>::Parser Parser;
	return Parser()(src, dest);
}

template<typename T>
inline T StrictLexicalConverter<T>::operator()(const u8string &src) {
	T dest;
	if (!this->operator()(src, dest)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_PARAMETER);
	}

	return dest;
}

template<typename T>
inline void FormattedValue<T>::operator()(std::ostream &stream) const {
	typedef typename detail::ValueFormatterTraits<T>::Formatter Formatter;
	Formatter()(value_, stream);
}



inline bool UTF8Utils::skipRaw(const char8_t **it, const char8_t *end) {
	if (*it == end) {
		return false;
	}

	const char8_t b = *((*it)++);

	if (b < 0 && *it != end) {
		++(*it);
		if ((b & 0x20) && *it != end) {
			++(*it);
			if ((b & 0x10) && *it != end) {
				++(*it);
			}
		}
	}

	return true;
}

inline bool UTF8Utils::decodeRaw(
		const char8_t **it, const char8_t *end, CodePoint *c) {
	if (*it == end) {
		return false;
	}

	const char8_t b = *((*it)++);
	*c = static_cast<CodePoint>(b);

	if (b < 0 && *it != end) {
		*c = (*c << 6) | (*((*it)++) & 0x3f);
		if ((b & 0x20) && *it != end) {
			*c = (*c << 6) | (*((*it)++) & 0x3f);
			if ((b & 0x10) && *it != end) {
				*c = ((*c << 6) | (*((*it)++) & 0x3f)) & 0x1fffff;
			}
			else {
				*c &= 0xffff;
			}
		}
		else {
			*c &= 0x7ff;
		}
	}

	return true;
}

inline bool UTF8Utils::decodeBackRaw(
		const char8_t **it, const char8_t *begin, CodePoint *c) {
	if (*it == begin) {
		return false;
	}

	char8_t b = *(--(*it));
	*c = static_cast<CodePoint>(b);

	if (b < 0) {
		*c &= 0x7f;
		if ((b & 0xc0) == 0x80 && *it != begin) {
			*c |= ((b = *(--(*it))) & 0x3f) << 6;
			if ((b & 0xc0) == 0x80 && *it != begin) {
				*c |= ((b = *(--(*it))) & 0x3f) << 12;
				if ((b & 0xc0) == 0x80 && *it != begin) {
					*c |= (*(--(*it)) & 0x7) << 18;
				}
				else {
					*c &= 0xffff;
				}
			}
			else {
				*c &= 0x7ff;
			}
		}
	}

	return true;
}

inline bool UTF8Utils::encode(char8_t **it, const char8_t *end, CodePoint c) {
	return encodeRaw(it, end,
			isIllegal(c) ? static_cast<CodePoint>(ILLEGAL_CHARACTER) : c);
}

inline bool UTF8Utils::encodeRaw(
		char8_t **it, const char8_t *end, CodePoint c) {
	if (c > 0x7f) {
		if (c > 0x7ff) {
			if (c > 0xffff) {
				if (end - *it <= 3) {
					return false;
				}
				*((*it)++) = static_cast<char8_t>((c >> 18) | 0xf0);
				*((*it)++) = static_cast<char8_t>(((c >> 12) & 0x3f) | 0x80);
			}
			else {
				if (end - *it <= 2) {
					return false;
				}
				*((*it)++) = static_cast<char8_t>((c >> 12) | 0xe0);
			}
			*((*it)++) = static_cast<char8_t>(((c >> 6) & 0x3f) | 0x80);
		}
		else {
			if (end - *it <= 1) {
				return false;
			}
			*((*it)++) = static_cast<char8_t>((c >> 6) | 0xc0);
		}
		*((*it)++) = static_cast<char8_t>((c & 0x3f) | 0x80);
	}
	else {
		if (*it == end) {
			return false;
		}
		*((*it)++) = static_cast<char8_t>(c);
	}

	return true;
}

inline bool UTF8Utils::isIllegal(CodePoint c) {
	return isIllegal(static_cast<uint64_t>(c));
}

inline bool UTF8Utils::isIllegal(uint64_t c) {
	return (c & 0xfffff800) == 0xd800 || (c & ~0x1fffff) != 0;
}


template<typename T, typename It, typename C>
inline SequenceReader<T, It, C>::SequenceReader(
		const std::pair<IteratorType, size_t> &src) :
		it_(src.first),
		begin_(src.first),
		end_(src.first + src.second) {
}

template<typename T, typename It, typename C>
inline SequenceReader<T, It, C>::SequenceReader(
		IteratorType begin, IteratorType end) :
		it_(begin),
		begin_(begin),
		end_(end) {
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::next() {
	assert(it_ != end_);
	++it_;
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::prev() {
	assert(it_ != begin_);
	--it_;
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::step(size_t count) {
	assert(count <= getNext());
	it_ += count;
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::back(size_t count) {
	assert(count <= getPrev());
	it_ -= count;
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::toBegin() {
	it_ = begin_;
}

template<typename T, typename It, typename C>
inline void SequenceReader<T, It, C>::toEnd() {
	it_ = end_;
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::atBegin() const {
	return it_ == begin_;
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::atEnd() const {
	return it_ == end_;
}

template<typename T, typename It, typename C>
inline const typename SequenceReader<T, It, C>::ValueType&
SequenceReader<T, It, C>::get() const {
	assert(!atEnd());
	return *it_;
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::hasNext() const {
	return (it_ != end_);
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::hasPrev() const {
	return (it_ != begin_);
}

template<typename T, typename It, typename C>
inline size_t SequenceReader<T, It, C>::getNext() const {
	return end_ - it_;
}

template<typename T, typename It, typename C>
inline size_t SequenceReader<T, It, C>::getPrev() const {
	return it_ - begin_;
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::nextCode() {
	CodeType code;
	return nextCode(&code);
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::prevCode() {
	CodeType code;
	return prevCode(&code);
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::nextCode(CodeType *code) {
	return ValueCoderType::decodeRaw(&it_, end_, code);
}

template<typename T, typename It, typename C>
inline bool SequenceReader<T, It, C>::prevCode(CodeType *code) {
	return ValueCoderType::decodeBackRaw(&it_, begin_, code);
}


template<typename T, typename It, typename C>
inline SequenceWriter<T, It, C>::SequenceWriter(IteratorType inserter) :
		inserter_(inserter) {
}

template<typename T, typename It, typename C>
inline void SequenceWriter<T, It, C>::appendCode(const CodeType &code) {
	ValueType buf[ValueCoderType::MAX_ENCODED_LENGTH];
	ValueType *tail = buf;
	ValueCoderType::encodeRaw(&tail, buf + sizeof(buf), code);
	for (ValueType *it = buf; it != tail; ++it) {
		append(*it);
	}
}

template<typename T, typename It, typename C>
inline void SequenceWriter<T, It, C>::append(const ValueType &value) {
	*inserter_ = value;
	++inserter_;
}

template<typename T, typename It, typename C>
inline void SequenceWriter<T, It, C>::append(
		const ValueType *buf, size_t size) {
	const ValueType *const end = buf + size;
	for (const ValueType *it = buf; it != end; ++it) {
		append(*it);
	}
}


template<typename W>
void SequenceUtils::appendValue(
		W &writer, uint32_t value, uint32_t minWidth) {
	char8_t buf[std::numeric_limits<uint32_t>::digits10 + 1];
	char8_t *it = buf;
	char8_t *const end = it + sizeof(buf);

	TinyLexicalIntConverter converter;
	converter.minWidth_ = minWidth;
	converter.format(it, end, value);

	writer.append(buf, static_cast<size_t>(it - buf));
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

inline uint32_t nlz(uint64_t bits) {
	uint32_t high = static_cast<uint32_t>(bits >> 32);
	if (high == 0) {
		return 32 + nlz(static_cast<uint32_t>(bits));
	}
	else {
		return nlz(high);
	}
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

/*
	https://developers.google.com/protocol-buffers/docs/encoding#types
*/
inline uint32_t zigzagEncode32(int32_t n) {
	return (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(n >> 31);
}

inline int32_t zigzagDecode32(uint32_t n) {
	return static_cast<int32_t>(n >> 1) ^ (-1) * static_cast<int32_t>(n & 1);
}

inline uint64_t zigzagEncode64(int64_t n) {
	return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}

inline int64_t zigzagDecode64(uint64_t n) {
	return static_cast<int64_t>(n >> 1) ^ (-1) * static_cast<int64_t>(n & 1);
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
template<typename A>
inline void BasicByteStream<S>::readAll(size_t length, A &action) {
	uint8_t buf[8192];
	for (size_t remaining = length; remaining > 0;) {
		const size_t last = impl().read(buf, std::min(remaining, sizeof(buf)));
		if (last == 0) {
			setError();
			detail::StreamErrors::throwUnexpectedEnd();
		}

		action(static_cast<const void*>(buf), last);
		remaining -= last;
	}
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

template<typename A>
inline void ArrayInStream::readAll(size_t length, A &action) {
	const uint8_t *newNext = next_ + length;
	if (newNext > end_) {
		detail::StreamErrors::throwUnexpectedEnd();
	}
	action(static_cast<const void*>(next_), length);
	next_ = newNext;
}

inline void ArrayInStream::position(size_t pos) {
	const uint8_t *const newNext = begin_ + pos;
	if (newNext > end_) {
		detail::StreamErrors::throwPositionOutOfRange();
	}
	next_ = newNext;
}

inline ArrayInStream::Locator ArrayInStream::locator() {
	return Locator(*this);
}


inline ArrayInStream::Locator::Locator() : stream_(NULL), position_(0) {
}

inline ArrayInStream::Locator::Locator(ArrayInStream &stream) :
		stream_(&stream), position_(stream.position()) {
}

inline void ArrayInStream::Locator::locate() const {
	if (stream_ == NULL) {
		return;
	}

	stream_->position(position_);
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



template<typename T, size_t Count>
NameCoder<T, Count>::NameCoder(
		const Entry (&entryList)[Count], size_t prefixWordCount) {

	const Entry *const end = entryList + Count;
	BaseEntry *destIt = entryList_;

	for (const Entry *it = entryList; it != end; ++it, ++destIt) {
		destIt->first = Impl::removePrefix(it->name_, prefixWordCount);
		destIt->second = it->id_;
	}

	Impl::initialize(nameList_, entryList_, Count);
}

template<typename T, size_t Count>
const char8_t* NameCoder<T, Count>::operator()(
		T id, const char8_t *defaultName) const {
	return Impl::findName(nameList_, Count, id, defaultName);
}

template<typename T, size_t Count>
bool NameCoder<T, Count>::operator()(const char8_t *name, T &id) const {
	const BaseEntry *entry = Impl::findEntry(entryList_, Count, name);

	if (entry == NULL) {
		id = T();
		return false;
	}

	id = static_cast<T>(entry->second);
	return true;
}


inline GeneralNameCoder::GeneralNameCoder() :
		coder_(NULL),
		coderFunc_(&emptyCoderFunc),
		size_(0) {
}

template<typename C>
inline GeneralNameCoder::GeneralNameCoder(const C *baseCoder) :
		coder_(baseCoder),
		coderFunc_(&coderFunc<C>),
		size_(baseCoder == NULL ? 0 : baseCoder->getSize()) {
}

inline const char8_t* GeneralNameCoder::operator()(
		Id id, const char8_t *defaultName) const {
	const bool nameResolving = true;
	return coderFunc_(coder_, nameResolving, defaultName, id);
}

inline bool GeneralNameCoder::operator()(const char8_t *name, Id &id) const {
	const bool nameResolving = false;
	return coderFunc_(coder_, nameResolving, name, id) != NULL;
}

inline size_t GeneralNameCoder::getSize() const {
	return size_;
}

template<typename C>
const char8_t* GeneralNameCoder::coderFunc(
		const void *coder, bool nameResolving, const char8_t *name, Id &id) {
	if (coder == NULL) {
		return emptyCoderFunc(coder, nameResolving, name, id);
	}

	const C &typedCoder = *static_cast<const C*>(coder);
	if (nameResolving) {
		return typedCoder(id, name);
	}
	else {
		typename C::Entry::Id baseId;
		const bool found = typedCoder(name, baseId);
		id = baseId;
		return found ? "" : NULL;
	}
}




template<typename T>
inline EnumCoder::Encoder<T, false> EnumCoder::operator()(
		const T &value,
		typename EncoderFunc<T>::Type func,
		typename DecoderFunc<T>::Type) const {
	return Encoder<T, false>(value, T(), func);
}

template<typename T, typename C>
inline EnumCoder::Encoder<T, false, const C*> EnumCoder::operator()(
		const T &value, const C &coder) const {
	return Encoder<T, false, const C*>(value, T(), &coder);
}

template<typename T>
inline EnumCoder::Encoder<T, true> EnumCoder::operator()(
		const T &value, T defaultValue,
		typename EncoderFunc<T>::Type func,
		typename DecoderFunc<T>::Type) const {
	return Encoder<T, true>(value, defaultValue, func);
}

template<typename T, typename C>
inline EnumCoder::Encoder<T, true, const C*> EnumCoder::operator()(
		const T &value, T defaultValue, const C &coder) const {
	return Encoder<T, true, const C*>(value, defaultValue, &coder);
}

template<typename T>
inline EnumCoder::Decoder<T, false> EnumCoder::operator()(
		T &value,
		typename EncoderFunc<T>::Type,
		typename DecoderFunc<T>::Type func) const {
	return Decoder<T, false>(value, T(), func);
}

template<typename T, typename C>
inline EnumCoder::Decoder<T, false, const C*> EnumCoder::operator()(
		T &value, const C &coder) const {
	return Decoder<T, false, const C*>(value, T(), &coder);
}

template<typename T>
inline EnumCoder::Decoder<T, true> EnumCoder::operator()(
		T &value, T defaultValue,
		typename EncoderFunc<T>::Type,
		typename DecoderFunc<T>::Type func) const {
	return Decoder<T, true>(value, defaultValue, func);
}

template<typename T, typename C>
inline EnumCoder::Decoder<T, true, const C*> EnumCoder::operator()(
		T &value,  T defaultValue, const C &coder) const {
	return Decoder<T, true, const C*>(value, defaultValue, &coder);
}


template<typename T, bool D, typename C>
inline const char8_t* EnumCoder::Base<T, D, C>::getName() const {
	return name_;
}

template<typename T, bool D, typename C>
inline C EnumCoder::Base<T, D, C>::withName(const char8_t *name) const {
	C dest = coder();
	dest.name_ = name;
	return dest;
}

template<typename T, bool D, typename C>
inline const T& EnumCoder::Base<T, D, C>::getDefaultValue() const {
	return defaultValue_;
}

template<typename T, bool D, typename C>
inline const C& EnumCoder::Base<T, D, C>::coder() const {
	return *static_cast<const C*>(this);
}

template<typename T, bool D, typename C>
inline EnumCoder::Base<T, D, C>::Base(T defaultValue) :
		name_(NULL), defaultValue_(defaultValue) {
}


template<typename T, bool D, typename C>
inline EnumCoder::Encoder<T, D, C>::Encoder(
		const T &value, T defaultValue, C coder) :
		BaseType(defaultValue), value_(value), coder_(coder) {
}

template<typename T, bool D, typename C>
inline EnumCoder::IntegerType EnumCoder::Encoder<T, D, C>::toInteger() const {
	UTIL_STATIC_ASSERT(sizeof(IntegerType) >= sizeof(T));
	return static_cast<IntegerType>(value_);
}

template<typename T, bool D, typename C>
inline const char8_t* EnumCoder::Encoder<T, D, C>::toString() const {
	if (coder_ == NULL) {
		return NULL;
	}

	return (*coder_)(value_);
}

template<typename T, bool D, typename C>
bool EnumCoder::Encoder<T, D, C>::isDefault() const {
	return BaseType::WITH_DEFAULT && value_ == BaseType::getDefaultValue();
}


template<typename T, bool D, typename C>
inline EnumCoder::Decoder<T, D, C>::Decoder(
		T &value, T defaultValue, C coder) :
		BaseType(defaultValue), value_(value), coder_(coder) {
}

template<typename T, bool D, typename C>
inline void EnumCoder::Decoder<T, D, C>::accept(IntegerType src) const {
	UTIL_STATIC_ASSERT(sizeof(IntegerType) >= sizeof(T));
	value_ = static_cast<T>(src);
}

template<typename T, bool D, typename C>
inline bool EnumCoder::Decoder<T, D, C>::accept(const char8_t *src) const {
	if (coder_ == NULL) {
		return false;
	}

	return (*coder_)(src, value_);
}

template<typename T, bool D, typename C>
void EnumCoder::Decoder<T, D, C>::setDefault() const {
	value_ = BaseType::getDefaultValue();
}


template<typename T, typename D>
ObjectCoderOptional<T, D>::ObjectCoderOptional(
		T &value, const D &defaultValue, const char8_t *name) :
		value_(value),
		defaultValue_(defaultValue),
		name_(name) {
}

template<typename T, typename D>
const ObjectCoderOptional<T, D>& ObjectCoderOptional<T, D>::ref() const {
	return *this;
}

template<typename T, typename D>
T& ObjectCoderOptional<T, D>::getValue() const {
	return value_;
}

template<typename T, typename D>
const D& ObjectCoderOptional<T, D>::getDefaultValue() const {
	return defaultValue_;
}

template<typename T, typename D>
const char8_t* ObjectCoderOptional<T, D>::getName() const {
	return name_;
}


inline ObjectCoderAttribute::ObjectCoderAttribute(const char8_t *name) :
		name_(name) {
}

template<typename T>
inline ObjectCoderAttribute ObjectCoderAttribute::create(
		const char8_t *name, const T&) {
	return ObjectCoderAttribute(name);
}

template<typename T, bool D, typename C>
inline ObjectCoderAttribute ObjectCoderAttribute::create(
		const char8_t*, const EnumCoder::Encoder<T, D, C> &coder) {
	return ObjectCoderAttribute(coder.getName());
}

template<typename T, bool D, typename C>
inline ObjectCoderAttribute ObjectCoderAttribute::create(
		const char8_t*, const EnumCoder::Decoder<T, D, C> &coder) {
	return ObjectCoderAttribute(coder.getName());
}

template<typename T, typename D>
inline ObjectCoderAttribute ObjectCoderAttribute::create(
		const char8_t*, const ObjectCoderOptional<T, D> &optional) {
	return ObjectCoderAttribute(optional.getName());
}


template<typename C, typename Alloc, typename Base>
inline ObjectCoderBase<C, Alloc, Base>::ObjectCoderBase(
		Allocator &alloc, const BaseCoder &baseCoder) :
		alloc_(alloc), baseCoder_(baseCoder) {
}

template<typename C, typename Alloc, typename Base>
inline Alloc& ObjectCoderBase<C, Alloc, Base>::getAllocator() const {
	return alloc_;
}

template<typename C, typename Alloc, typename Base>
template<typename S, typename T>
inline void ObjectCoderBase<C, Alloc, Base>::encode(
		S &stream, const T &value) const {
	ObjectCoder::Impl::encodeGeneral(coder(), stream, value);
}

template<typename C, typename Alloc, typename Base>
template<typename S, typename T>
inline void ObjectCoderBase<C, Alloc, Base>::decode(S &stream, T &value) const {
	ObjectCoder::Impl::decodeGeneral(coder(), stream, value);
}

template<typename C, typename Alloc, typename Base>
template<typename Org, typename S, typename T, typename Traits>
inline void ObjectCoderBase<C, Alloc, Base>::encodeBy(
		Org &orgCoder, S &stream, const T &value, const Attribute &attr,
		const Traits&) const {
	ObjectCoder::Impl::encodeGeneralBy(
			orgCoder, baseCoder_, stream, value, attr, Traits());
}

template<typename C, typename Alloc, typename Base>
template<typename Org, typename S, typename T, typename Traits>
inline void ObjectCoderBase<C, Alloc, Base>::decodeBy(
		Org &orgCoder, S &stream, T &value, const Attribute &attr,
		const Traits&) const {
	ObjectCoder::Impl::decodeGeneralBy(
			orgCoder, baseCoder_, stream, value, attr, Traits());
}

template<typename C, typename Alloc, typename Base>
template<typename S, typename T, typename D>
inline void ObjectCoderBase<C, Alloc, Base>::encodeOptional(
		S &stream, const ObjectCoderOptional<T, D> &value) const {
	ObjectCoder::Impl::encodeOptional(coder(), stream, value);
}

template<typename C, typename Alloc, typename Base>
template<typename S, typename T, typename D>
inline void ObjectCoderBase<C, Alloc, Base>::decodeOptional(
		S &stream, const ObjectCoderOptional<T, D> &value) const {
	ObjectCoder::Impl::decodeOptional(coder(), stream, value);
}

template<typename C, typename Alloc, typename Base>
template<typename T>
inline T ObjectCoderBase<C, Alloc, Base>::create() const {
	return ObjectCoder::Impl::createGeneral<const C, T>(coder());
}

template<typename C, typename Alloc, typename Base>
template<typename T>
inline void ObjectCoderBase<C, Alloc, Base>::remove(T &value) const {
	ObjectCoder::Impl::removeGeneral(coder(), value);
}

template<typename C, typename Alloc, typename Base>
inline const C& ObjectCoderBase<C, Alloc, Base>::coder() const {
	return *static_cast<const C*>(this);
}


inline ObjectCoder::ObjectCoder() : BaseType(defaultAlloc_, FalseType()) {
}

template<typename Alloc>
inline ObjectCoder::AllocCoder<Alloc> ObjectCoder::withAllocator(Alloc &alloc) {
	return AllocCoder<Alloc>(alloc);
}

inline ObjectCoder::NonOptionalCoder<> ObjectCoder::withoutOptional() {
	return withoutOptional(getDefaultAllocator());
}

template<typename Alloc>
inline ObjectCoder::NonOptionalCoder<Alloc> ObjectCoder::withoutOptional(
		Alloc &alloc) {
	return NonOptionalCoder<Alloc>(alloc);
}

inline ObjectCoder::DefaultsCoder ObjectCoder::withDefaults() {
	return DefaultsCoder();
}

template<typename T, typename D>
inline ObjectCoderOptional<T, D> ObjectCoder::createOptional(
		T &value, const D &defaultValue, const char8_t *name) {
	return ObjectCoderOptional<T, D>(value, defaultValue, name);
}

inline ObjectCoder::Allocator& ObjectCoder::getDefaultAllocator() {
	return defaultAlloc_;
}


template<typename C, typename S, typename T>
inline void ObjectCoder::Impl::encodeGeneral(
		C &coder, S &stream, const T &value) {
	encodeWrapped(
			coder, stream, value, Attribute(), int(),
			detectStream<S>(static_cast<int*>(NULL)));
}

template<typename C, typename S, typename T>
inline void ObjectCoder::Impl::decodeGeneral(C &coder, S &stream, T &value) {
	decodeWrapped(
			coder, stream, value, Attribute(), int(),
			detectStream<S>(static_cast<int*>(NULL)));
}

template<typename C, typename Other, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeGeneralBy(
		C &coder, Other &otherCoder, S &stream, const T &value,
		const Attribute &attr, const Traits&) {
	otherCoder.encodeBy(coder, stream, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeGeneralBy(
		C &coder, const FalseType&, S &stream, const T &value,
		const Attribute &attr, const Traits&) {
	encodeInternal(
			coder, stream, value, attr, Traits(),
			detectType(
					value, detectDigits<T, Traits>(static_cast<int*>(NULL)),
					detectIterable<T>(static_cast<int*>(NULL)),
					detectString<T>(static_cast<int*>(NULL))));
}

template<typename C, typename Other, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeGeneralBy(
		C &coder, Other &otherCoder, S &stream, T &value,
		const Attribute &attr, const Traits&) {
	otherCoder.decodeBy(coder, stream, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeGeneralBy(
		C &coder, const FalseType&, S &stream, T &value,
		const Attribute &attr, const Traits&) {
	decodeInternal(
			coder, stream, value, attr, Traits(),
			detectType(
					value, detectDigits<T, Traits>(static_cast<int*>(NULL)),
					detectIterable<T>(static_cast<int*>(NULL)),
					detectString<T>(static_cast<int*>(NULL))));
}

template<typename S, typename T>
void ObjectCoder::Impl::encodeNumeric(
		S &stream, const T &value, const Attribute &attr,
		NumericPrecision presicion) {
	AbstractNumericEncoder::Wrapper<T> wrapper(value);
	stream.writeNumericGeneral(wrapper, attr, presicion);
}

template<typename S, typename T>
T ObjectCoder::Impl::decodeNumeric(
		S &stream, const Attribute &attr, NumericPrecision presicion) {
	T value;
	AbstractNumericDecoder::Wrapper<T> wrapper(value);
	stream.readNumericGeneral(wrapper, attr, presicion);
	return value;
}

template<typename C, typename S, typename T, typename D>
void ObjectCoder::Impl::encodeOptional(
		C &coder, S &stream, const ObjectCoderOptional<T, D> &value) {
	encodeInternal(
			coder, stream, value, Attribute::create(NULL, value), int(),
			TypeTag<TYPE_OPTIONAL>());
}

template<typename C, typename S, typename T, typename D>
void ObjectCoder::Impl::decodeOptional(
		C &coder, S &stream, const ObjectCoderOptional<T, D> &value) {
	decodeInternal(
			coder, stream, value, Attribute::create(NULL, value), int(),
			TypeTag<TYPE_OPTIONAL>());
}

template<typename C, typename T>
inline T ObjectCoder::Impl::createGeneral(C &coder) {
	return createInternal(
			coder,
			getAllocatorInternal<C, T>(coder, static_cast<int*>(NULL)),
			static_cast<T*>(NULL),
			detectPointer(static_cast<T*>(NULL)));
}

template<typename C, typename T>
inline void ObjectCoder::Impl::removeGeneral(C &coder, T &value) {
	removeInternal(
			getAllocatorInternal<C, T>(coder, static_cast<int*>(NULL)),
			value, detectPointer(static_cast<T*>(NULL)));
}

template<typename T>
inline void ObjectCoder::Impl::checkTooManyArguments(const T &value) {
	static_cast<void>(value);
	UTIL_STATIC_ASSERT((IsSame<T, EmptyValue>::VALUE));
}

template<typename T>
inline size_t ObjectCoder::Impl::toSize(const T &value) {
	typedef typename BoolType<(sizeof(size_t) < sizeof(T))>::Result Narrowing;
	return convertUInt<size_t, T>(value, Narrowing());
}

template<typename T>
inline T ObjectCoder::Impl::toUInt(size_t value) {
	typedef typename BoolType<(sizeof(T) < sizeof(size_t))>::Result Narrowing;
	return convertUInt<T, size_t>(value, Narrowing());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeWrapped(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TrueType&) {
	coder.encodeBy(coder, stream, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeWrapped(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const FalseType&) {
	ObjectOutStream<S> wrapped(stream);
	coder.encodeBy(coder, wrapped, value, attr, Traits());
}

template<typename C, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeWrapped(
		C &coder, std::ostream &stream, const T &value,
		const Attribute &attr, const Traits&, const FalseType&) {
	ObjectTextOutStream wrapped(stream);
	coder.encodeBy(coder, wrapped, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeWrapped(
		C &coder, S &stream, T &value, const Attribute &attr, const Traits&,
		const TrueType&) {
	coder.decodeBy(coder, stream, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeWrapped(
		C &coder, S &stream, T &value, const Attribute &attr, const Traits&,
		const FalseType&) {
	ObjectInStream<S> wrapped(stream);
	coder.decodeBy(coder, wrapped, value, attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_BOOL>&) {
	static_cast<void>(coder);
	stream.writeBool(value, attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_NUMERIC>&) {
	static_cast<void>(coder);
	stream.writeNumeric(value, attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_ENUM>&) {
	static_cast<void>(coder);

	if (T::WITH_DEFAULT && !C::DefaultsEncodable::VALUE) {
		if (stream.isTyped() && value.isDefault()) {
			return;
		}
	}

	stream.writeEnum(value, attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_STRING>&) {
	static_cast<void>(coder);
	stream.writeString(value.c_str(), value.size(), attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_LIST>&) {
	typename S::ListScope scope(stream, value.size(), attr);

	for (typename T::const_iterator it = value.begin();
			it != value.end(); ++it) {
		coder.encodeBy(
				coder, scope.stream(), *it, attr,
				filterTraitsForMembers(Traits()));
	}
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OBJECT>&) {

	if (detectPartialObject<T>(static_cast<int*>(NULL)).VALUE) {
		value(coder, stream, filterTraitsForMembers(Traits()), Encoding());
	}
	else {
		typename S::ObjectScope scope(stream, attr);
		value(
				coder, scope.stream(), filterTraitsForMembers(Traits()),
				Encoding());
	}
}

template<typename C, typename S, typename T, typename Traits>
void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OPTIONAL>&) {
	static_cast<void>(coder);

	if (!C::DefaultsEncodable::VALUE && stream.isTyped() &&
			value.getValue() == value.getDefaultValue()) {
		return;
	}

	coder.encodeBy(coder, stream, value.getValue(), attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OPTIONAL_OBJECT>&) {
	const Type type = (value == NULL ? TYPE_NULL : TYPE_OPTIONAL_OBJECT);
	typename S::ValueScope scope(stream, type, attr);

	if (value != NULL) {
		coder.encodeBy(
				coder, scope.stream(), *value, attr, OptionalObjectCoding());
	}
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::encodeInternal(
		C &coder, S &stream, const T &value, const Attribute &attr,
		const Traits&, const FalseType&) {
	static_cast<void>(coder);
	static_cast<void>(stream);
	static_cast<void>(value);
	static_cast<void>(attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_BOOL>&) {
	static_cast<void>(coder);
	value = stream.readBool(attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_NUMERIC>&) {
	static_cast<void>(coder);
	value = stream.template readNumeric<T>(attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_ENUM>&) {
	static_cast<void>(coder);

	if (T::WITH_DEFAULT) {
		Type type;
		if (stream.peekType(type, attr) && type == TYPE_NULL) {
			value.setDefault();
			return;
		}
	}

	stream.template readEnum<T>(value, attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_STRING>&) {
	static_cast<void>(coder);
	StringBuilder<T> builder(value);
	stream.readString(builder, attr);
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_LIST>&) {
	typedef typename T::value_type ElementType;

	size_t size;
	typename S::ListScope scope(stream, size, attr);
	value.reserve(size);

	for (size_t r = size; r > 0; r--) {
		value.push_back(coder.create<ElementType>());
		coder.decodeBy(
				coder, scope.stream(), value.back(), attr,
				filterTraitsForMembers(Traits()));
	}
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OBJECT>&) {

	if (detectPartialObject<T>(static_cast<int*>(NULL)).VALUE) {
		value(coder, stream, filterTraitsForMembers(Traits()), Decoding());
	}
	else {
		typename S::ObjectScope scope(stream, attr);
		value(
				coder, scope.stream(), filterTraitsForMembers(Traits()),
				Decoding());
	}
}

template<typename C, typename S, typename T, typename Traits>
void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OPTIONAL>&) {
	static_cast<void>(coder);

	Type type;
	if (stream.peekType(type, attr) && type == TYPE_NULL) {
		if (C::OptionalDecodable::VALUE) {
			value.getValue() = value.getDefaultValue();
		}
		return;
	}

	coder.decodeBy(coder, stream, value.getValue(), attr, Traits());
}

template<typename C, typename S, typename T, typename Traits>
void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const TypeTag<TYPE_OPTIONAL_OBJECT>&) {
	Type type;
	typename S::ValueScope scope(stream, type, attr);

	coder.remove(value);

	if (type == TYPE_NULL) {
		return;
	}

	value = coder.create<T>();

	coder.decodeBy(
			coder, scope.stream(), *value, attr, OptionalObjectCoding());
}

template<typename C, typename S, typename T, typename Traits>
inline void ObjectCoder::Impl::decodeInternal(
		C &coder, S &stream, T &value, const Attribute &attr,
		const Traits&, const FalseType&) {
	static_cast<void>(coder);
	static_cast<void>(stream);
	static_cast<void>(value);
	static_cast<void>(attr);
}

template<typename C, typename Alloc, typename T>
inline T* ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, T**, const TrueType&) {
	return createInternal<C, Alloc, T>(
			coder, alloc, TrueType(),
			detectOr(
					detectAllocConstructor<T>(static_cast<int*>(NULL)),
					detectIterable<T>(static_cast<int*>(NULL))));
}

template<typename C, typename Alloc, typename T>
inline T ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, T*, const FalseType&) {
	return createInternal<C, Alloc, T>(
			coder, alloc, FalseType(),
			detectOr(
					detectAllocConstructor<T>(static_cast<int*>(NULL)),
					detectIterable<T>(static_cast<int*>(NULL))));
}

template<typename C, typename Alloc, typename T>
inline T* ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, const TrueType&, const TrueType&) {
	typedef typename Alloc::template rebind<T>::other ElemAlloc;
	ElemAlloc a = alloc;

	UniqueAllocValue<ElemAlloc, T> allocValue(a, a.allocate(1));
	new(static_cast<void*>(allocValue.get())) T(coder.getAllocator());

	return allocValue.release();
}

template<typename C, typename Alloc, typename T>
inline T* ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, const TrueType&, const FalseType&) {
	static_cast<void>(coder);
	typedef typename Alloc::template rebind<T>::other ElemAlloc;
	ElemAlloc a = alloc;

	UniqueAllocValue<ElemAlloc, T> allocValue(a, a.allocate(1));
	new(static_cast<void*>(allocValue.get())) T();

	return allocValue.release();
}

template<typename C, typename Alloc, typename T>
inline T ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, const FalseType&, const TrueType&) {
	static_cast<void>(alloc);
	return T(coder.getAllocator());
}

template<typename C, typename Alloc, typename T>
inline T ObjectCoder::Impl::createInternal(
		C &coder, const Alloc &alloc, const FalseType&, const FalseType&) {
	static_cast<void>(coder);
	static_cast<void>(alloc);
	return T();
}

template<typename Alloc, typename T>
inline void ObjectCoder::Impl::removeInternal(
		const Alloc &alloc, T *&value, const TrueType&) {
	if (value == NULL) {
		return;
	}

	typedef typename Alloc::template rebind<T>::other ElemAlloc;
	ElemAlloc a = alloc;

	UniqueAllocValue<ElemAlloc, T> allocValue(a, value);
	value = NULL;

	a.destroy(allocValue.get());
	allocValue.reset();
}

template<typename Alloc, typename T>
inline void ObjectCoder::Impl::removeInternal(
		const Alloc &alloc, T &value, const FalseType&) {
	static_cast<void>(alloc);
	static_cast<void>(value);
}

template<typename C, typename T>
inline typename C::Allocator::template rebind<T>::other
ObjectCoder::Impl::getAllocatorInternal(
		C &coder,
		typename C::Allocator::template rebind<int>::other::value_type*) {
	return coder.getAllocator();
}

template<typename C, typename T>
inline StdAllocator<T, typename C::Allocator>
ObjectCoder::Impl::getAllocatorInternal(
		C &coder, void*) {
	return coder.getAllocator();
}

template<typename D, typename S>
inline D ObjectCoder::Impl::convertUInt(const S &src, const TrueType&) {
	UTIL_STATIC_ASSERT(std::numeric_limits<S>::is_integer);
	UTIL_STATIC_ASSERT(!std::numeric_limits<S>::is_signed);
	UTIL_STATIC_ASSERT(sizeof(D) < sizeof(S));

	if (src > static_cast<S>(std::numeric_limits<D>::max())) {
		errorOutOfRange();
	}

	return static_cast<D>(src);
}

template<typename D, typename S>
inline D ObjectCoder::Impl::convertUInt(const S &src, const FalseType&) {
	UTIL_STATIC_ASSERT(std::numeric_limits<S>::is_integer);
	UTIL_STATIC_ASSERT(!std::numeric_limits<S>::is_signed);

	return static_cast<D>(src);
}


template<typename Alloc, typename T>
ObjectCoder::Impl::UniqueAllocValue<Alloc, T>::UniqueAllocValue(
		Alloc &alloc, T *value) :
		alloc_(alloc), value_(value) {
}

template<typename Alloc, typename T>
ObjectCoder::Impl::UniqueAllocValue<Alloc, T>::~UniqueAllocValue() {
	try {
		reset();
	}
	catch (...) {
	}
}

template<typename Alloc, typename T>
void ObjectCoder::Impl::UniqueAllocValue<Alloc, T>::reset() {
	T *org = value_;
	value_ = NULL;
	alloc_.deallocate(org, 1);
}

template<typename Alloc, typename T>
T* ObjectCoder::Impl::UniqueAllocValue<Alloc, T>::get() throw() {
	return value_;
}

template<typename Alloc, typename T>
T* ObjectCoder::Impl::UniqueAllocValue<Alloc, T>::release() throw() {
	T *org = value_;
	value_ = NULL;
	return org;
}


template<typename S>
inline ObjectCoder::BaseScope<S>::BaseScope(Stream &base, const Attribute&) :
		base_(base) {
}

template<typename S>
inline ObjectCoder::BaseScope<S>::BaseScope(
		Stream &base, size_t, const Attribute&) :
		base_(base) {
}

template<typename S>
inline ObjectCoder::BaseScope<S>::BaseScope(
		Stream &base, Type, const Attribute&) :
		base_(base) {
}

template<typename S>
inline S& ObjectCoder::BaseScope<S>::stream() {
	return base_;
}


template<typename T, typename Char>
inline ObjectCoder::StringBuilder<T, Char>::StringBuilder(T &dest) :
		dest_(dest) {
}

template<typename T, typename Char>
inline void ObjectCoder::StringBuilder<T, Char>::operator()(
		const void *data, size_t size) {
	UTIL_STATIC_ASSERT(sizeof(CharType) == 1);
	dest_.append(static_cast<const CharType*>(data), size);
}


template<typename Alloc>
inline ObjectCoder::AllocCoder<Alloc>::AllocCoder(Allocator &alloc) :
		BaseType(alloc, BaseCoder()) {
}


template<typename Alloc>
inline ObjectCoder::NonOptionalCoder<Alloc>::NonOptionalCoder(
		Allocator &alloc) :
		BaseType(alloc, BaseCoder()) {
}


inline ObjectCoder::DefaultsCoder::DefaultsCoder() :
		BaseType(ObjectCoder::getDefaultAllocator(), BaseCoder()) {
}


template<typename S>
inline ObjectInStream<S>::ObjectInStream(S &base) : base_(base) {
}

template<typename S>
inline void ObjectInStream<S>::reserve(size_t size) {
	static_cast<void>(size);
}

template<typename S>
inline bool ObjectInStream<S>::peekType(Type &type, const Attribute&) {
	type = util::ObjectCoder::TYPE_NULL;
	return false;
}

template<typename S>
inline bool ObjectInStream<S>::readBool(const Attribute&) {
	return !!readNumeric<int8_t>(Attribute());
}

template<typename S>
template<typename T>
inline T ObjectInStream<S>::readNumeric(const Attribute&, NumericPrecision) {
	T value;
	base_.readAll(&value, sizeof(value));
	return value;
}

template<typename S>
template<typename T>
void ObjectInStream<S>::readNumericGeneral(
		T &value, const Attribute&, NumericPrecision) {
	const std::pair<void*, size_t> &binary = value.getBinary();
	base_.readAll(binary.first, binary.second);
}

template<typename S>
template<typename T>
inline void ObjectInStream<S>::readEnum(T &value, const Attribute&) {
	value.accept(readNumeric<typename T::IntegerType>(Attribute()));
}

template<typename S>
template<typename A>
inline void ObjectInStream<S>::readString(A &action, const Attribute&) {
	readBinary(action, Attribute());
}

template<typename S>
template<typename A>
inline void ObjectInStream<S>::readBinary(A &action, const Attribute&) {
	const size_t size =
			ObjectCoder::Impl::toSize(readNumeric<uint64_t>(Attribute()));
	action(size);
	base_.readAll(size, action);
}

template<typename S>
inline typename ObjectInStream<S>::Locator ObjectInStream<S>::locator() {
	return base_.locator();
}


template<typename S>
inline ObjectInStream<S>::Scope::Scope(
		ObjectInStream &base, size_t &size, const Attribute&) :
		BaseType(base, Attribute()) {
	size = ObjectCoder::Impl::toSize(base.readNumeric<uint64_t>(Attribute()));
}

template<typename S>
inline ObjectInStream<S>::Scope::Scope(
		ObjectInStream &base, Type &type, const Attribute&) :
		BaseType(base, Attribute()) {
	type = static_cast<Type>(base.readNumeric<int8_t>(Attribute()));
}


template<typename S>
inline ObjectOutStream<S>::ObjectOutStream(S &base) : base_(base) {
}

template<typename S>
inline void ObjectOutStream<S>::reserve(size_t size) {
	static_cast<void>(size);
}

template<typename S>
inline void ObjectOutStream<S>::writeBool(bool value, const Attribute&) {
	writeNumeric(static_cast<int8_t>(value ? 1 : 0), Attribute());
}

template<typename S>
template<typename T>
inline void ObjectOutStream<S>::writeNumeric(
		const T &value, const Attribute&, NumericPrecision) {
	base_.writeAll(&value, sizeof(value));
}

template<typename S>
template<typename T>
inline void ObjectOutStream<S>::writeNumericGeneral(
		const T &value, const Attribute&, NumericPrecision) {
	const std::pair<const void*, size_t> binary = value.getBinary();
	base_.writeAll(binary.first, binary.second);
}

template<typename S>
template<typename T>
inline void ObjectOutStream<S>::writeEnum(
		const T &value, const Attribute&) {
	writeNumeric(value.toInteger(), Attribute());
}

template<typename S>
inline void ObjectOutStream<S>::writeString(
		const char8_t *data, size_t size, const Attribute&) {
	writeBinary(data, size, Attribute());
}

template<typename S>
inline void ObjectOutStream<S>::writeBinary(
		const void *data, size_t size, const Attribute&) {
	writeNumeric(ObjectCoder::Impl::toUInt<uint64_t>(size), Attribute());
	base_.writeAll(data, size);
}


template<typename S>
inline ObjectOutStream<S>::Scope::Scope(
		ObjectOutStream &base, size_t size, const Attribute&) :
		BaseType(base, Attribute()) {
	base.writeNumeric(ObjectCoder::Impl::toUInt<uint64_t>(size), Attribute());
}

template<typename S>
inline ObjectOutStream<S>::Scope::Scope(
		ObjectOutStream &base, Type type, const Attribute&) :
		BaseType(base, Attribute()) {
	base.writeNumeric(static_cast<int8_t>(type), Attribute());
}


template<typename T>
ObjectFormatter::Base<T> ObjectFormatter::operator()(
		const T &value) const {
	return Base<T>(value, defaultCoder_);
}

template<typename T, typename C>
ObjectFormatter::Base<T, C> ObjectFormatter::operator()(
		const T &value, C &coder) const {
	return Base<T, C>(value, coder);
}


template<typename T, typename C>
ObjectFormatter::Base<T, C>::Base(const T &value, C &coder) :
		value_(value), coder_(coder) {
}

template<typename T, typename C>
void ObjectFormatter::Base<T, C>::format(std::ostream &stream) const {
	ObjectTextOutStream wrapped(stream);
	wrapped.setSingleLine(true);

	coder_.encode(wrapped, value_);
}

template<typename C, typename T>
std::ostream& operator<<(
		std::ostream &s, const ObjectFormatter::Base<C, T> &formatterBase) {
	formatterBase.format(s);
	return s;
}


template<typename T>
void ObjectTextOutStream::writeNumeric(
		const T &value, const Attribute &attr, NumericPrecision) {
	ObjectCoder::Impl::encodeNumeric(*this, value, attr, NumericPrecision());
}

template<typename T>
void ObjectTextOutStream::writeNumericGeneral(
		const T &value, const Attribute &attr, NumericPrecision) {
	writeHead(attr, true);
	if (value.isInteger()) {
		base_ << value.getInt64();
	}
	else {
		base_ << value.getDouble();
	}
	writeTail();
}

template<typename T>
void ObjectTextOutStream::writeEnum(const T &value, const Attribute &attr) {
	writeHead(attr, true);

	const char8_t *str = value.toString();
	if (str == NULL) {
		base_ << value.toInteger();
	}
	else {
		base_ << str;
	}

	writeTail();
}


template<typename B>
AbstractNumericEncoder::Wrapper<B>::Wrapper(const Base &base) : base_(base) {
}

template<typename B>
AbstractNumericEncoder::Wrapper<B>::~Wrapper() {
}

template<typename B>
bool AbstractNumericEncoder::Wrapper<B>::isInteger() const {
	return std::numeric_limits<Base>::is_integer;
}

template<typename B>
std::pair<const void*, size_t>
AbstractNumericEncoder::Wrapper<B>::getBinary() const {
	return std::pair<const void*, size_t>(&base_, sizeof(base_));
}

template<typename B>
int64_t AbstractNumericEncoder::Wrapper<B>::getInt64() const {
	return static_cast<int64_t>(base_);
}

template<typename B>
double AbstractNumericEncoder::Wrapper<B>::getDouble() const {
	return static_cast<double>(base_);
}


template<typename B>
AbstractNumericDecoder::Wrapper<B>::Wrapper(Base &base) : base_(base) {
}

template<typename B>
AbstractNumericDecoder::Wrapper<B>::~Wrapper() {
}

template<typename B>
bool AbstractNumericDecoder::Wrapper<B>::isInteger() const {
	return std::numeric_limits<Base>::is_integer;
}

template<typename B>
std::pair<void*, size_t>
AbstractNumericDecoder::Wrapper<B>::getBinary() const {
	return std::pair<void*, size_t>(&base_, sizeof(base_));
}

template<typename B>
void AbstractNumericDecoder::Wrapper<B>::accept(int64_t value) const {
	base_ = static_cast<Base>(value);
}

template<typename B>
void AbstractNumericDecoder::Wrapper<B>::accept(double value) const {
	base_ = static_cast<Base>(value);
}


template<typename B>
inline AbstractEnumEncoder::Wrapper<B>::Wrapper(Base &base) : base_(base) {
}

template<typename B>
inline AbstractEnumEncoder::Wrapper<B>::~Wrapper() {
}

template<typename B>
inline AbstractEnumEncoder::IntegerType
AbstractEnumEncoder::Wrapper<B>::toInteger() const {
	return base_.toInteger();
}

template<typename B>
inline const char8_t* AbstractEnumEncoder::Wrapper<B>::toString() const {
	return base_.toString();
}


template<typename B>
inline AbstractEnumDecoder::Wrapper<B>::Wrapper(Base &base) : base_(base) {
}

template<typename B>
inline AbstractEnumDecoder::Wrapper<B>::~Wrapper() {
}

template<typename B>
inline void AbstractEnumDecoder::Wrapper<B>::accept(IntegerType value) const {
	base_.accept(value);
}

template<typename B>
inline bool AbstractEnumDecoder::Wrapper<B>::accept(
		const char8_t *value) const {
	return base_.accept(value);
}


template<typename T>
T AbstractObjectInStream::readNumeric(
		const Attribute &attr, NumericPrecision precision) {
	return ObjectCoder::Impl::decodeNumeric<
			AbstractObjectInStream, T>(*this, attr, precision);
}

template<typename T>
void AbstractObjectInStream::readEnum(T &value, const Attribute &attr) {
	AbstractEnumDecoder::Wrapper<T> wrapper(value);
	readEnumGeneral(wrapper, attr);
}

template<typename A>
void AbstractObjectInStream::readString(A &action, const Attribute &attr) {
	ActionWrapper<A> wrapper(action);
	readStringGeneral(wrapper, attr);
}

template<typename A>
void AbstractObjectInStream::readBinary(A &action, const Attribute &attr) {
	ActionWrapper<A> wrapper(action);
	readBinaryGeneral(wrapper, attr);
}


template<typename B, typename S>
AbstractObjectInStream::Wrapper<B, S>::Wrapper(Base &base) : base_(base) {
}

template<typename B, typename S>
AbstractObjectInStream::Wrapper<B, S>::~Wrapper() {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectInStream::Wrapper<B, S>::Wrapper(
		Other &base, const Attribute &attr) :
		scope_(base, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectInStream::Wrapper<B, S>::Wrapper(
		Other &base, Type &type, const Attribute &attr) :
		scope_(base, type, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectInStream::Wrapper<B, S>::Wrapper(
		Other &base, size_t &size, const Attribute &attr) :
		scope_(base, size, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::reserve(size_t size) {
	base_.reserve(size);
}

template<typename B, typename S>
bool AbstractObjectInStream::Wrapper<B, S>::peekType(
		Type &type, const Attribute &attr) {
	return base_.peekType(type, attr);
}

template<typename B, typename S>
bool AbstractObjectInStream::Wrapper<B, S>::readBool(const Attribute &attr) {
	return base_.readBool(attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::readNumericGeneral(
		const AbstractNumericDecoder &value, const Attribute &attr,
		NumericPrecision precision) {
	base_.readNumericGeneral(value, attr, precision);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::readEnumGeneral(
		const AbstractEnumDecoder &value, const Attribute &attr) {
	base_.readEnum(value, attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::readStringGeneral(
		Action &action, const Attribute &attr) {
	base_.readString(action, attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::readBinaryGeneral(
		Action &action, const Attribute &attr) {
	base_.readBinary(action, attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::duplicate(
		StreamStorage &storage) const {
	UTIL_STATIC_ASSERT(sizeof(Wrapper) <= sizeof(storage));
	new (getRawAddress(&storage)) Wrapper(*this);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::pushObject(
		StreamStorage &storage, const Attribute &attr) {
	typedef typename B::ObjectScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::pushValue(
		StreamStorage &storage, Type &type, const Attribute &attr) {
	typedef typename B::ValueScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, type, attr);
}

template<typename B, typename S>
void AbstractObjectInStream::Wrapper<B, S>::pushList(
		StreamStorage &storage, size_t &size, const Attribute &attr) {
	typedef typename B::ListScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, size, attr);
}

template<typename B, typename S>
AbstractObjectInStream::BaseLocator*
AbstractObjectInStream::Wrapper<B, S>::createLocator(
		LocatorStorage &storage) {
	typedef LocatorWrapper<B> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	return new (getRawAddress(&storage)) Sub(base_);
}


template<typename B>
AbstractObjectInStream::ActionWrapper<B>::ActionWrapper(Base &base) :
		base_(base) {
}

template<typename B>
AbstractObjectInStream::ActionWrapper<B>::~ActionWrapper() {
}

template<typename B>
void AbstractObjectInStream::ActionWrapper<B>::operator()(size_t size) {
	base_(size);
}

template<typename B>
void AbstractObjectInStream::ActionWrapper<B>::operator()(
		const void *data, size_t size) {
	base_(data, size);
}


template<typename B>
AbstractObjectInStream::LocatorWrapper<B>::LocatorWrapper(BaseStream &stream) :
		base_(stream.locator()) {
}

template<typename B>
AbstractObjectInStream::LocatorWrapper<B>::~LocatorWrapper() {
}

template<typename B>
AbstractObjectInStream::BaseLocator*
AbstractObjectInStream::LocatorWrapper<B>::duplicate(
		LocatorStorage &storage) const {
	UTIL_STATIC_ASSERT(sizeof(Base) <= sizeof(storage));
	return new (getRawAddress(&storage)) LocatorWrapper(*this);
}

template<typename B>
void AbstractObjectInStream::LocatorWrapper<B>::locate() const {
	base_.locate();
}


template<typename T>
void AbstractObjectOutStream::writeNumeric(
		const T &value, const Attribute &attr, NumericPrecision precision) {
	ObjectCoder::Impl::encodeNumeric(*this, value, attr, precision);
}

template<typename T>
void AbstractObjectOutStream::writeEnum(
		const T &value, const Attribute &attr) {
	AbstractEnumEncoder::Wrapper<const T> wrapper(value);
	writeEnumGeneral(wrapper, attr);
}


template<typename B, typename S>
AbstractObjectOutStream::Wrapper<B, S>::Wrapper(Base &base) : base_(base) {
}

template<typename B, typename S>
AbstractObjectOutStream::Wrapper<B, S>::~Wrapper() {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectOutStream::Wrapper<B, S>::Wrapper(
		Other &base, const Attribute &attr) :
		scope_(base, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectOutStream::Wrapper<B, S>::Wrapper(
		Other &base, Type type, const Attribute &attr) :
		scope_(base, type, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
template<typename Other>
AbstractObjectOutStream::Wrapper<B, S>::Wrapper(
		Other &base, size_t size, const Attribute &attr) :
		scope_(base, size, attr),
		base_(scope_.stream()) {
}

template<typename B, typename S>
bool AbstractObjectOutStream::Wrapper<B, S>::isTyped() const {
	return base_.isTyped();
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::reserve(size_t size) {
	base_.reserve(size);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::writeBool(
		bool value, const Attribute &attr) {
	base_.writeBool(value, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::writeNumericGeneral(
		const AbstractNumericEncoder &value, const Attribute &attr,
		NumericPrecision precision) {
	base_.writeNumericGeneral(value, attr, precision);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::writeEnumGeneral(
		const AbstractEnumEncoder &value, const Attribute &attr) {
	base_.writeEnum(value, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::writeString(
		const char8_t *data, size_t size, const Attribute &attr) {
	base_.writeString(data, size, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::writeBinary(
		const void *data, size_t size, const Attribute &attr) {
	base_.writeBinary(data, size, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::pushObject(
		StreamStorage &storage, const Attribute &attr) {
	typedef typename B::ObjectScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::duplicate(
		StreamStorage &storage) const {
	UTIL_STATIC_ASSERT(sizeof(Wrapper) <= sizeof(storage));
	new (getRawAddress(&storage)) Wrapper(*this);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::pushValue(
		StreamStorage &storage, Type type, const Attribute &attr) {
	typedef typename B::ValueScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, type, attr);
}

template<typename B, typename S>
void AbstractObjectOutStream::Wrapper<B, S>::pushList(
		StreamStorage &storage, size_t size, const Attribute &attr) {
	typedef typename B::ListScope SubScope;
	typedef typename SubScope::Stream SubBase;
	typedef Wrapper<SubBase, SubScope> Sub;

	UTIL_STATIC_ASSERT(sizeof(Sub) <= sizeof(storage));
	new (getRawAddress(&storage)) Sub(base_, size, attr);
}


template<typename Base>
inline AbstractObjectCoder::Wrapper<Base>
AbstractObjectCoder::operator()(const Base &base) const {
	return Wrapper<Base>(base);
}


template<typename Base>
inline AbstractObjectCoder::Wrapper<Base>::Wrapper(const BaseCoder &base) :
		BaseType(base.getAllocator(), base) {
}

template<typename Base>
template<typename S, typename T>
inline void AbstractObjectCoder::Wrapper<Base>::encode(
		S &stream, const T &value) const {
	BaseType::encode(Out::Selector<>()(stream)(), value);
}

template<typename Base>
template<typename S, typename T>
inline void AbstractObjectCoder::Wrapper<Base>::decode(
		S &stream, T &value) const {
	BaseType::decode(In::Selector<>()(stream)(), value);
}


} 

#endif
