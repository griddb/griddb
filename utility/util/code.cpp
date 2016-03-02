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




#include "util/code.h"
#include "util/os.h"
#include <iomanip>

#ifndef _WIN32
#include <iconv.h>
#if defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__sun)
#ifndef _LIBICONV_VERSION
#error 
#endif
#undef iconv
#define UTIL_CODE_CONVERTER_ICONV_FUNCTION ::libiconv
#define UTIL_CODE_CONVERTER_ICONV_INBUF_CAST(inbuf) inbuf
#else
#define UTIL_CODE_CONVERTER_ICONV_FUNCTION ::iconv
#define UTIL_CODE_CONVERTER_ICONV_INBUF_CAST(inbuf) const_cast<char**>(inbuf)
#endif
#endif 

namespace util {





namespace detail {

template<typename T>
void FloatingNumberFormatter<T>::operator()(const T &src, std::ostream &dest) {
	LocaleUtils::CLocaleScope scope(dest);
	dest << std::setprecision(std::numeric_limits<T>::digits10 + 3) << src;
}

template struct FloatingNumberFormatter<float>;
template struct FloatingNumberFormatter<double>;

}	





#ifdef _WIN32
const Code Code::CHAR(CP_ACP);
const Code Code::ISO88591(1252);
const Code Code::UTF8(CP_UTF8);
const Code Code::WCHAR_T(0);
#else
const Code Code::CHAR("");
const Code Code::ISO88591("ISO-8859-1");
const Code Code::UTF8("UTF-8");
#endif

#ifdef _WIN32
CodeConverter::CodeConverter(Code fromCode, Code toCode) :
		toCode_(toCode), fromCode_(fromCode) {
}
#else
CodeConverter::CodeConverter(Code fromCode, Code toCode) :
		descriptor_(::iconv_open(toCode, fromCode)) {
	if (descriptor_ == reinterpret_cast<iconv_t>(-1)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}
#endif 

CodeConverter::~CodeConverter() {
#ifdef _WIN32
#else
	iconv_close(descriptor_);
#endif
}

size_t CodeConverter::operator()(const char **inBuf, size_t *inBytesLeft,
		char **outBuf, size_t *outBytesLeft) {
#ifdef _WIN32
	int inWCharsLen = MultiByteToWideChar(fromCode_, 0, *inBuf,
			static_cast<int>(*inBytesLeft), NULL, 0);

	if (inWCharsLen > 0) {
		wchar_t *inWChars = new wchar_t[inWCharsLen];

		inWCharsLen = MultiByteToWideChar(fromCode_, 0, *inBuf,
				static_cast<int>(*inBytesLeft), inWChars, inWCharsLen);

		if (inWCharsLen > 0) {
			int outBytesWritten = WideCharToMultiByte(toCode_, 0, inWChars,
					inWCharsLen, *outBuf, static_cast<int>(*outBytesLeft), 0, 0);

			delete[] inWChars;

			if (outBytesWritten > 0) {
				*inBuf += *inBytesLeft;
				*inBytesLeft = 0;
				*outBuf += outBytesWritten;
				*outBytesLeft -= outBytesWritten;
				return outBytesWritten;
			}
		} else {
			delete[] inWChars;
		}
	}

	return static_cast<size_t>(-1);
#else
	if (reset()) {
		return convertToChars(inBuf, inBytesLeft, outBuf, outBytesLeft);
	} else {
		return static_cast<size_t>(-1);
	}
#endif 
}

bool CodeConverter::operator()(const std::string &inBuf, std::string &outBuf) {
#ifdef _WIN32
	int inWCharsLen = MultiByteToWideChar(fromCode_, 0, inBuf.c_str(),
			static_cast<int>(inBuf.size()), NULL, 0);

	if (inWCharsLen > 0) {
		wchar_t *inWChars = new wchar_t[inWCharsLen];

		inWCharsLen = MultiByteToWideChar(fromCode_, 0, inBuf.c_str(),
				static_cast<int>(inBuf.size()), inWChars, inWCharsLen);

		if (inWCharsLen > 0) {
			int outCharsLen = WideCharToMultiByte(toCode_, 0, inWChars,
					inWCharsLen, NULL, 0, 0, 0);

			if (outCharsLen > 0) {
				char *outChars = new char[outCharsLen];

				outCharsLen = WideCharToMultiByte(toCode_, 0, inWChars,
						inWCharsLen, outChars, outCharsLen, 0, 0);

				if (outCharsLen > 0) {
					outBuf.append(outChars, outCharsLen);
					delete[] outChars;
					return true;
				} else {
					delete[] outChars;
				}
			} else {
				delete[] inWChars;
			}
		} else {
			delete[] inWChars;
		}
	}

	return false;
#else
	if (reset()) {
		return convertToString(inBuf.data(), inBuf.size(), outBuf);
	} else {
		return false;
	}
#endif 
}

#ifdef _WIN32
bool CodeConverter::operator()(const std::string &inBuf, std::wstring &outBuf) {
	int outWCharsLen = MultiByteToWideChar(fromCode_, 0, inBuf.c_str(),
			static_cast<int>(inBuf.size()), NULL, 0);

	if (outWCharsLen > 0) {
		wchar_t *outWChars = new wchar_t[outWCharsLen];

		outWCharsLen = MultiByteToWideChar(fromCode_, 0, inBuf.c_str(),
				static_cast<int>(inBuf.size()), outWChars, outWCharsLen);

		if (outWCharsLen > 0) {
			outBuf.append(outWChars, outWCharsLen);
			delete[] outWChars;
			return true;
		} else {
			delete[] outWChars;
		}
	}

	return false;
}
#endif 

#ifdef _WIN32
bool CodeConverter::operator()(const std::wstring &inbuf, std::string &outbuf) {
	int outCharsLen = WideCharToMultiByte(toCode_, 0, inbuf.c_str(),
			static_cast<int>(inbuf.size()), NULL, 0, 0, 0);

	if (outCharsLen > 0) {
		char *outbuf_c = new char[outCharsLen];

		outCharsLen = WideCharToMultiByte(toCode_, 0, inbuf.c_str(),
				static_cast<int>(inbuf.size()), outbuf_c, outCharsLen, 0, 0);

		if (outCharsLen > 0) {
			outbuf.append(outbuf_c, outCharsLen);
			delete[] outbuf_c;
			return true;
		} else {
			delete[] outbuf_c;
		}
	}

	return false;
}
#endif 

#ifndef _WIN32
size_t CodeConverter::convertToChars(const char **inBuf, size_t *inBytesLeft,
		char **outBuf, size_t *outBytesLeft) {
	return UTIL_CODE_CONVERTER_ICONV_FUNCTION(descriptor_,
			UTIL_CODE_CONVERTER_ICONV_INBUF_CAST(inBuf), inBytesLeft,
			outBuf, outBytesLeft);
}
#endif 

#ifndef _WIN32
template<class OutStringType>
bool CodeConverter::convertToString(const char *inbuf, size_t inBytesLeft,
		OutStringType &outbuf) {
	size_t outLen = inBytesLeft;

	for (;;) {
		typedef typename OutStringType::value_type OutChar;
		
		OutChar *outBufChars = new OutChar[outLen];
		OutChar *outBufCharsPtr = outBufChars;
		size_t outBytesLeft = outLen;

		const size_t result = convertToChars(&inbuf, &inBytesLeft,
				reinterpret_cast<char**>(&outBufCharsPtr), &outBytesLeft);

		if (result != static_cast<size_t>(-1)) {
			outbuf.append(outBufChars, outLen - outBytesLeft);
			delete[] outBufChars;
			return true;
		} else if (errno == E2BIG) {
			outbuf.append(outBufChars, outLen - outBytesLeft);
			delete[] outBufChars;
			outLen *= 2;
			continue;
		} else {
			delete[] outBufChars;
			return false;
		}
	}
}
#endif 

#ifndef _WIN32
bool CodeConverter::reset() {
	return convertToChars(NULL, 0, NULL, 0) != static_cast<size_t>(-1);
}
#endif 







const uint32_t* CRC32::getTable() {
	class Table {
	public:
		Table() {
			uint32_t c, n, k;
			for (n = 0; n < 256; n++) {
				c = n;
				for (k = 0; k < 8; k++) {
					if (c & 1) {
						c = 0xedb88320L ^ (c >> 1);
					} else {
						c = c >> 1;
					}
				}
				data_[n] = c;
			}
		}

		const uint32_t *get() {
			return data_;
		}

	private:
		uint32_t data_[256];
	};

	static Table table;
	return table.get();
}

const uint16_t* CRC16::getTable() {
	class Table {
	public:
		Table() {
			uint16_t c;
			uint32_t n, k;
			for (n = 0; n < 256; n++) {
				c = static_cast<uint16_t>(n);
				for (k = 0; k < 8; k++) {
					if (c & 1) {
						c = static_cast<uint16_t>((c >> 1) ^ 0x8408);
					} else {
						c = static_cast<uint16_t>(c >> 1);
					}
				}
				data_[n] = c;
			}
		}

		const uint16_t *get() {
			return data_;
		}

	private:
		uint16_t data_[256];
	};

	static Table table;
	return table.get();
}











static const char gBase64_enc[] = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
		'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
		'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
		'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
		'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/' };

static const char gBase64_dec[] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1,
		63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31,
		32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
		50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
		-1, -1, -1, -1, -1, -1, -1, -1, -1 };

char* Base64Converter::encodeUnit(char o[4], const char *i, size_t ilen) {
	const uint8_t *const ibuf = reinterpret_cast<const uint8_t*>(i); 

	if (ilen > 2) {
		o[0] = gBase64_enc[ibuf[0] >> 2];
		o[1] = gBase64_enc[((ibuf[0] & 0x03) << 4) + (ibuf[1] >> 4)];
		o[2] = gBase64_enc[((ibuf[1] & 0x0f) << 2) + (ibuf[2] >> 6)];
		o[3] = gBase64_enc[ibuf[2] & 0x3f];
	} else if (ilen == 2) {
		o[0] = gBase64_enc[ibuf[0] >> 2];
		o[1] = gBase64_enc[((ibuf[0] & 0x03) << 4) + (ibuf[1] >> 4)];
		o[2] = gBase64_enc[(ibuf[1] & 0x0f) << 2];
		o[3] = '=';
	} else {
		o[0] = gBase64_enc[ibuf[0] >> 2];
		o[1] = gBase64_enc[(ibuf[0] & 0x03) << 4];
		o[2] = o[3] = '=';
	}

	return o;
}

size_t Base64Converter::decodeUnit(char o[3], const char i[4]) {
	const uint8_t *const ibuf = reinterpret_cast<const uint8_t*>(i); 
	int c;

	size_t index = 0;
	while (index < 4) {
		c = ibuf[index];
		if (c == '=') {
			break;
		}

		if (c == ' ') {
			c = '+';
		}

		if ((c = gBase64_dec[c]) == -1) {
			
			return 0;
		}

		switch (index) {
		case 0: {
			o[0] = static_cast<char>(c << 2);
			break;
		}
		case 1: {
			o[0] |= static_cast<char>(c >> 4);
			o[1] = static_cast<char>((c & 0x0f) << 4);
			break;
		}
		case 2: {
			o[1] |= static_cast<char>(c >> 2);
			o[2] = static_cast<char>((c & 0x03) << 6);
			break;
		}
		case 3: {
			o[2] |= static_cast<char>(c);
			break;
		}
		} 

		++index;
	} 

	
	
	
	
	
	

	if (index < 2)
		return 0;
	return index - 1;
}

size_t Base64Converter::encode(char *obuf, const char *ibuf, size_t iblen) {
	const size_t step_max(iblen / 3);
	size_t step = 0;

	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	char* it(obuf);

	

	while (step < step_max) {
		encodeUnit(it, ib, 3);
		it += 4;
		ib += 3;
		++step;
	}

	size_t ret = step * 4;

	if (ib != ie) {
		encodeUnit(it, ib, iblen % 3);
		ret += 4;
	}

	return ret;
}

size_t Base64Converter::decode(char *obuf, const char *ibuf, size_t iblen) {
	const size_t step_max(iblen / 4);
	size_t step = 0;

	const char* ib(ibuf);
	
	char* it(obuf);
	size_t dec_len;

	while (step < step_max) {
		dec_len = decodeUnit(it, ib);
		if (0 == dec_len) {
			
			return 0;
		}

		it += dec_len;
		ib += 4;
		++step;
	}

	return step * 3;
}

std::ostream& Base64Converter::encode(std::ostream &os, std::istream &is) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char i[3];
	char o[4];
	size_t rsize;

	while (!is.eof()) {
		is.read(i, 3);
		rsize = static_cast<size_t>(is.gcount());
		if (rsize == 0) {
			continue;
		}

		encodeUnit(o, i, rsize);
		os.write(o, 4);
	}

	return os;
}

std::ostream& Base64Converter::decode(std::ostream &os, std::istream &is) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char i[4];
	char o[3];
	size_t rsize, dsize;
	size_t tsize = 0;
	char* si(i);

	while (!is.eof()) {
		is.read(si, static_cast<std::streamsize>(4 - tsize));
		rsize = static_cast<size_t>(is.gcount());
		if (rsize == 0) {
			continue;
		}

		tsize += rsize;
		if (tsize != 4) {
			si += rsize;
			continue;
		}

		si = i;
		tsize = 0;

		dsize = decodeUnit(o, i);
		if (0 == dsize) {
			break;
		}

		os.write(o, static_cast<std::streamsize>(dsize));
	}

	return os;
}





inline static bool _addEscape(char &o, char c, char e) {
	switch (c) {
	case ' ':
		o = ' ';
		return true;
	case '\r':
		o = 'r';
		return true;
	case '\n':
		o = 'n';
		return true;
	case 0x00:
		o = '0';
		return true;
	case '\t':
		o = 't';
		return true;
	case '\f':
		o = 'f';
		return true;
	case '\v':
		o = 'v';
		return true;
	case '\'':
		o = '\'';
		return true;
	case '\"':
		o = '\"';
		return true;
	}

	if (c == e) {
		o = c;
		return true;
	}

	return false;
}

inline static char _delEscape(char c, char) {
	switch (c) {
	case ' ':
		return ' ';
	case 'r':
		return '\r';
	case 'n':
		return '\n';
	case '0':
		return 0x00;
	case 't':
		return '\t';
	case 'f':
		return '\f';
	case 'v':
		return '\v';
	case '\'':
		return '\'';
	case '\"':
		return '\"';
	}

	return c;
}

std::ostream& EscapeConverter::encode(
		std::ostream &os, std::istream &is, char e) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char c = 0x00;
	char o;

	while (!is.eof()) {
		is.get(c);
		if (!is.good())
			break;

		if (_addEscape(o, c, e)) {
			os << e << o;
		} else {
			os << c;
		}
	}

	return os;
}

size_t EscapeConverter::encode(
		char *obuf, const char *ibuf, size_t iblen, char e) {
	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	size_t ret = 0;

	char c, o;

	while (ib != ie) {
		c = *ib;
		if (_addEscape(o, c, e)) {
			*(obuf + ret) = e;
			++ret;
			*(obuf + ret) = o;
		} else {
			*(obuf + ret) = c;
		}

		++ret;
		++ib;
	}

	return ret;
}

std::ostream& EscapeConverter::decode(
		std::ostream &os, std::istream &is, char e) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char c = 0x00;
	bool raw = true;

	while (!is.eof()) {
		is.get(c);
		if (!is.good())
			break;

		if (raw) {
			if (c == e) {
				raw = false;
			} else {
				os << c;
			}
		} else {
			os << _delEscape(c, e);
			raw = true;
		}
	}

	return os;
}

size_t EscapeConverter::decode(
		char *obuf, const char *ibuf, size_t iblen, char e) {
	char c;
	bool raw = true;

	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	size_t ret = 0;

	while (ib != ie) {
		c = *ib;

		if (raw) {
			if (c == e) {
				raw = false;
			} else {
				*(obuf + ret) = c;
				++ret;
			}
		} else {
			*(obuf + ret) = _delEscape(c, e);
			++ret;

			raw = true;
		}

		++ib;
	}

	return ret;
}





inline static unsigned char _toHexHalf(unsigned char c, bool cap) {
	if (c < 10) {
		return static_cast<unsigned char>('0' + c);
	}

	return static_cast<unsigned char>((cap ? 'A' : 'a') + c - 10);
}

inline static unsigned char _toBinHalf(unsigned char i) {
	if (i >= static_cast<unsigned char>('0') &&
			i <= static_cast<unsigned char>('9')) {
		return static_cast<unsigned char>(i - static_cast<unsigned char>('0'));
	}

	if (i >= static_cast<unsigned char>('A') &&
			i <= static_cast<unsigned char>('Z')) {
		return static_cast<unsigned char>(
				i - static_cast<unsigned char>('A') + 10);
	}

	return static_cast<unsigned char>(
			i - static_cast<unsigned char>('a') + 10);
}

char* HexConverter::encode(char o[2], char c, bool cap) {
	o[0] = static_cast<char>(_toHexHalf(
			static_cast<unsigned char>(
					static_cast<unsigned char>(c) >> 4U), cap));
	o[1] = static_cast<char>(_toHexHalf(c & 0x0f, cap));
	return o;
}

char HexConverter::decode(const char o[2]) {
	return static_cast<char>(_toBinHalf(o[0]) << 4 | _toBinHalf(o[1]));
}

std::ostream& HexConverter::encode(std::ostream &os, std::istream &is, bool cap) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char c;
	char o[2];

	while (!is.eof()) {
		is.get(c);
		if (!is.good())
			break;
		os.write(encode(o, c, cap), 2);
	}

	return os;
}

size_t HexConverter::encode(char *obuf, const char *ibuf, size_t iblen,
		bool cap) {
	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	size_t ret = 0;
	char o[2];

	while (ib != ie) {
		encode(o, *ib, cap);
		*(obuf + ret) = o[0];
		*(obuf + ret + 1) = o[1];
		ret += 2;
		++ib;
	}

	return ret;
}

std::ostream& HexConverter::decode(std::ostream &os, std::istream &is) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char i[2];

	while (!is.eof()) {
		is.get(i[0]);
		if (!is.good())
			break;

		is.get(i[1]);
		if (!is.good()) {
			i[1] = 0x00;
		}

		os << decode(i);
	}

	return os;
}

size_t HexConverter::decode(char *obuf, const char *ibuf, size_t iblen) {
	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	char i[2];
	size_t ret = 0;

	while (ib != ie) {
		i[0] = *ib;
		if (ib + 1 == ie) {
			i[1] = 0x00;
			++ib;
		} else {
			i[1] = *(ib + 1);
			ib += 2;
		}

		*(obuf + ret) = decode(i);

		++ret;
	}

	return ret;
}




const uint8_t URLConverter::URL_TABLE[] = {
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 
		1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
		0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, };

size_t URLConverter::encode(char *obuf, const char *ibuf, size_t iblen) {
	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	size_t ret = 0;
	char c;
	char o[2];

	while (ib != ie) {
		c = *ib;
		if (c == ' ') {
			*(obuf + ret) = '+';
			++ret;
		} else if (URL_TABLE[static_cast<uint8_t>(c)]) {
			HexConverter::encode(o, c, true);

			*(obuf + ret) = '%';
			*(obuf + ret + 1) = o[0];
			*(obuf + ret + 2) = o[1];
			ret += 3;
		} else {
			*(obuf + ret) = c;
			++ret;
		}

		++ib;
	}

	return ret;
}

std::ostream& URLConverter::encode(std::ostream &os, std::istream &is) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char c;
	char o[2];

	while (!is.eof()) {
		is.get(c);

		if (!is.good())
			break;
		if (URL_TABLE[static_cast<uint8_t>(c)]) {
			os << '%';
			HexConverter::encode(o, c, true);
			os.write(o, 2);
		} else {
			os << c;
		}
	}

	return os;
}

size_t URLConverter::decode(char *obuf, const char *ibuf, size_t iblen) {
	const char* ib(ibuf);
	const char* ie(ibuf + iblen);
	char i[2];
	char c;
	bool raw = true;
	size_t ret = 0;

	while (ib != ie) {
		c = *ib;
		++ib;

		if (raw) {
			if (c == '%') {
				raw = false;
				continue;
			}

			if (c == '+') {
				c = ' ';
			}

			*(obuf + ret) = c;

			++ret;
		} else {
			raw = true;

			i[0] = c;
			if (ib != ie) {
				i[1] = *ib;
				++ib;
			} else {
				i[1] = 0x00;
			}

			*(obuf + ret) = HexConverter::decode(i);
			++ret;
		}
	}

	return ret;
}

std::ostream& URLConverter::decode(std::ostream &os, std::istream &is) {
	LocaleUtils::CLocaleScope osLocaleScope(os);
	LocaleUtils::CLocaleScope isLocaleScope(is);

	char i[2];
	char c;
	bool raw = true;

	while (!is.eof()) {
		is.get(c);
		if (!is.good())
			break;

		if (raw) {
			if (c == '%') {
				raw = false;
				continue;
			}

			if (c == '+') {
				c = ' ';
			}

			os << c;
		} else {
			raw = true;

			i[0] = c;
			is.get(i[1]);
			if (!is.good()) {
				i[1] = 0x00;
			}

			os << HexConverter::decode(i);
		}
	}

	return os;
}







namespace detail {
void StreamErrors::throwUnexpectedEnd() {
	UTIL_THROW_UTIL_ERROR(CODE_DECODE_FAILED,
			"Decode failed (detail=unexpected end of stream)");
}
void StreamErrors::throwPositionOutOfRange() {
	UTIL_THROW_UTIL_ERROR(CODE_DECODE_FAILED,
			"Decode failed (detail=position out of range)");
}
void StreamErrors::throwUnexpectedRemaining() {
	UTIL_THROW_UTIL_ERROR(CODE_DECODE_FAILED,
			"Decode failed (detail=unexpected remaining of stream)");
}
} 

} 
