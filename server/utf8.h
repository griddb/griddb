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
/*!
	@file
	@brief Definition of UTF8 subroutines, used only in function_string.h
*/
#ifndef UTF8_H_
#define UTF8_H_

/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
static const unsigned char sgsUtf8Trans1[] = {
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
	0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x00, 0x01, 0x02, 0x03,
	0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03,
	0x00, 0x01, 0x00, 0x00,
};

#define _READ_UTF8 sgsUtf8Read

/*!
 * @brief Read one UTF-8 character
 *
 * @param _zIn First byte of UTF-8 character
 * @param _pzNext Write first byte past UTF-8 char here
 *
 * @return
 * @note cf.(Public Domain)http://www.sqlite.org
 */
static int sgsUtf8Read(const char *_zIn, const char **_pzNext) {
	int c;
	const unsigned char *zIn = reinterpret_cast<const unsigned char *>(_zIn);

	c = *(zIn++);
	if (c >= 0xc0) {
		c = sgsUtf8Trans1[c - 0xc0];
		while ((*zIn & 0xc0) == 0x80) {
			c = (c << 6) + (0x3f & *(zIn++));
		}
		if (c < 0x80 || (c & 0xFFFFF800) == 0xD800 ||
			(c & 0xFFFFFFFE) == 0xFFFE) {
			c = 0xFFFD;
		}
	}
	*_pzNext = reinterpret_cast<const char *>(zIn);
	return c;
}

#define _SKIP_UTF8(zIn)                                                 \
	{                                                                   \
		if ((static_cast<unsigned char>(*zIn++)) >= 0xc0) {             \
			while ((static_cast<unsigned char>(*zIn) & 0xc0) == 0x80) { \
				zIn++;                                                  \
			}                                                           \
		}                                                               \
	}

#define _SKIP_UTF8_WITH_MAX(zIn, max)                                   \
	{                                                                   \
		if ((static_cast<unsigned char>(*zIn++)) >= 0xc0) {             \
			while (zIn < max &&                                         \
				   (static_cast<unsigned char>(*zIn) & 0xc0) == 0x80) { \
				zIn++;                                                  \
			}                                                           \
		}                                                               \
	}

#define _SKIP_UTF8_ITERATOR(zIn, end)                                   \
	{                                                                   \
		if ((static_cast<unsigned char>(*zIn++)) >= 0xc0) {             \
			while (it != end &&                                         \
				   (static_cast<unsigned char>(*zIn) & 0xc0) == 0x80) { \
				zIn++;                                                  \
			}                                                           \
		}                                                               \
	}

#define _COPY_UTF8_CHAR(zIn, zOut)                                      \
	{                                                                   \
		*zOut++ = *zIn++;                                               \
		if (static_cast<unsigned char>(*(zIn++)) >= 0xc0) {             \
			while ((static_cast<unsigned char>(*zIn) & 0xc0) == 0x80) { \
				*(zOut++) = *(zIn++);                                   \
			}                                                           \
		}                                                               \
	}

/*!
 * @brief strlen of UTF-8
 *
 * @param z UTF-8 string
 *
 * @return length
 */
static int _utf8Strlen(const char *z) {
	if (z == 0) return 0;
	int len = 0;
	const char *end = &z[strlen(z)];
	while (*z) {
		len++;
		_SKIP_UTF8_WITH_MAX(z, end);
	}
	return len;
}

/*!
 * @brief Substring of UTF-8
 *
 * @param z UTF-8 string
 * @param start Start position
 * @param length Substring's max length
 * @param out Obtained substring
 */
static void _utf8Substring(
	const char *z, int start, int length, util::String &out) {
	out = "";
	if (z == 0 || z[0] == '\0') {
		return;
	}

	const char *end = &z[strlen(z)];
	int i = 0;
	for (i = 0; i < start; i++) {
		_SKIP_UTF8_WITH_MAX(z, end);
		if (*z == '\0' || z >= end) {
			out = "";
			return;
		}
	}

	if (length == -1) {
		out += z;
	}
	else {
		for (i = 0; i < length; i++) {
			if (*z == '\0') {
				break;
			}
			out.append(1, *z);
			if (static_cast<unsigned char>(*(z++)) >= 0xc0) {
				while (*z && (static_cast<unsigned char>(*z) & 0xc0) == 0x80) {
					out.append(1, *z++);
				}
			}
		}
	}
	return;
}

#endif
