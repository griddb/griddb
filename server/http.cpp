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
	@brief Implementations of http message classes
*/
#include "http.h"
#include "gs_error_common.h"
#include "ebb_request_parser.h"
#include <locale>

#if HTTP_JSON_ENABLD
#include "json.h"
#include "picojson.h"
#endif

class HttpRequest::Parser {
public:
	explicit Parser(HttpRequest &base);

	void setMessage(HttpMessage &message);

	bool parse(bool eof);
	void expectMore(bool eof);
	bool rewriteToRequest(int32_t *status, String *firstLine);
	ebb_request& getRequest();

	void flushHeaderField();

	static void acceptPath(
			ebb_request *request, const char8_t *at, size_t length);
	static void acceptQueryString(
			ebb_request *request, const char8_t *at, size_t length);
	static void acceptFragment(
			ebb_request *request, const char8_t *at, size_t length);
	static void acceptHeaderField(
			ebb_request *request, const char8_t *at, size_t length, int index);
	static void acceptHeaderValue(
			ebb_request *request, const char8_t *at, size_t length, int index);
	static ebb_request* getParserRequest(void *data);

	static Parser& getParser(ebb_request *request);
	static HttpRequest& getBase(ebb_request *request);
	static HttpMessage& getMessage(ebb_request *request);

	static const char8_t* findStr(
			const char8_t *buf, size_t bufSize, const char8_t *key);

private:
	HttpRequest &base_;
	HttpMessage *message_;
	String fieldName_;
	String fieldValue_;
	ebb_request request_;
};

const int32_t HttpMessage::DEFAULT_VERSION_MAJOR = 1;
const int32_t HttpMessage::DEFAULT_VERSION_MINOR = 1;

const char8_t HttpMessage::HEADER_ACCEPT[] = "Accept";
const char8_t HttpMessage::HEADER_ACCEPT_ENCODING[] = "Accept-Encoding";
const char8_t HttpMessage::HEADER_CONNECTION[] = "Connection";
const char8_t HttpMessage::HEADER_CONTENT_ENCODING[] = "Content-Encoding";
const char8_t HttpMessage::HEADER_CONTENT_TYPE[] = "Content-Type";
const char8_t HttpMessage::HEADER_CONTENT_LENGTH[] = "Content-Length";
const char8_t HttpMessage::HEADER_HOST[] = "Host";

const char8_t HttpMessage::CONNECTION_CLOSE[] = "close";

const char8_t HttpMessage::CONTENT_TYPE_OCTET_STREAM[] =
		"application/octet-stream";
const char8_t HttpMessage::CONTENT_TYPE_JSON[] = "application/json";
const char8_t HttpMessage::CONTENT_TYPE_FORM[] =
		"application/www-form-urlencoded";
const char8_t HttpMessage::CONTENT_TYPE_X_FORM[] =
		"application/x-www-form-urlencoded";
const char8_t HttpMessage::CONTENT_CODING_IDENTITY[] = "identity";

const char8_t HttpMessage::CRLF[] = "\r\n";

const char8_t HttpMessage::CHARS_HTAB[] = "\t";
const char8_t HttpMessage::CHARS_SP[] = " ";
const char8_t HttpMessage::CHARS_EQ[] = "=";
const char8_t HttpMessage::CHARS_COMMA[] = ",";
const char8_t HttpMessage::CHARS_SEMI[] = ";";
const char8_t HttpMessage::CHARS_DQUOTE[] = "\"";
const char8_t HttpMessage::CHARS_BSLASH[] = "\\";

const char8_t HttpMessage::CHARS_SEPARATOR[] = " \"(),/@:;<=>?[\\]{}";
const char8_t HttpMessage::CHARS_TOKEN68_SYMBOL[] = "-._~+/";

const std::pair<uint8_t, uint8_t> HttpMessage::CHARS_VCHAR(0x21, 0x7e);
const std::pair<uint8_t, uint8_t> HttpMessage::CHARS_OBS(0x80, 0xff);
const std::pair<uint8_t, uint8_t> HttpMessage::CHARS_UPPER_ALPHA(0x41, 0x5a);
const std::pair<uint8_t, uint8_t> HttpMessage::CHARS_LOWER_ALPHA(0x61, 0x7a);
const std::pair<uint8_t, uint8_t> HttpMessage::CHARS_DIGIT(0x30, 0x39);

HttpMessage::HttpMessage(const Allocator &alloc) :
		headerList_(alloc),
		startLine_(alloc),
		contentEncodingCustomized_(false),
		keepAlive_(false),
		messageBuf_(alloc),
		contentBuf_(alloc),
		wroteSize_(0) {
	clear();
}

void HttpMessage::clear() {
	version_ = Version(DEFAULT_VERSION_MAJOR, DEFAULT_VERSION_MINOR);
	headerList_.clear();
	startLine_.clear();
	contentEncodingCustomized_ = false;
	keepAlive_ = false;
	messageRef_ = BufferRef();
	contentRef_ = BufferRef();
	messageBuf_.clear();
	contentBuf_.clear();
	wroteSize_ = 0;
}

void HttpMessage::build() {
	Buffer &buf = prepareMessageBuffer();
	buf.resize(0);

	if (startLine_.empty()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION, "");
	}

	const size_t contentSize = getContentSize();
	{
		util::NormalOStringStream oss;
		oss << contentSize;
		addHeader(HEADER_CONTENT_LENGTH, oss.str().c_str(), true);
	}

	if (contentSize > 0 && findHeader(HEADER_CONTENT_TYPE) == NULL) {
		addHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_OCTET_STREAM, true);
	}

	if (keepAlive_ || isHttp10OrLess()) {
		removeHeader(HEADER_CONNECTION);
	}
	else {
		addHeader(HEADER_CONNECTION, CONNECTION_CLOSE, true);
	}

	if (!contentEncodingCustomized_) {
		addHeader(HEADER_ACCEPT_ENCODING, CONTENT_CODING_IDENTITY, true);
	}

	const size_t headerSize = getHeaderSize();
	buf.reserve(headerSize + contentSize);

	appendBuffer(buf, startLine_);
	appendBuffer(buf, CRLF);

	for (HeaderList::const_iterator it = headerList_.begin();
			it != headerList_.end(); ++it) {
		appendBuffer(buf, it->name_);
		appendBuffer(buf, ": ");
		appendBuffer(buf, it->value_);
		appendBuffer(buf, CRLF);
	}
	appendBuffer(buf, CRLF);

	assert(headerSize == buf.size());

	appendBuffer(buf, getContentData(), contentSize);
}

HttpMessage::Version& HttpMessage::getVersion() {
	return version_;
}

HttpMessage::HeaderList& HttpMessage::getHeaderList() {
	return headerList_;
}

HttpMessage::HeaderField* HttpMessage::findHeader(const char8_t *name) {
	for (HeaderList::iterator it = headerList_.begin();
			it != headerList_.end(); ++it) {
		if (FieldParser::compareToken(it->name_.c_str(), name) == 0) {
			return &(*it);
		}
	}

	return NULL;
}

void HttpMessage::addHeader(
		const char8_t *name, const char8_t *value, bool overwriting) {
	if (overwriting) {
		removeHeader(name);
	}

	const Allocator &alloc = headerList_.get_allocator();
	headerList_.push_back(
			HeaderField(String(name, alloc), String(value, alloc)));
}

void HttpMessage::removeHeader(const char8_t *name) {
	for (HeaderList::iterator it = headerList_.begin();
			it != headerList_.end();) {
		if (FieldParser::compareToken(it->name_.c_str(), name) == 0) {
			it = headerList_.erase(it);
		}
		else {
			++it;
		}
	}
}

void HttpMessage::setStartLine(const char8_t *line) {
	startLine_ = line;
}

bool HttpMessage::isContentEncodingCustomized() const {
	return contentEncodingCustomized_;
}

void HttpMessage::setContentEncodingCustomized(bool customized) {
	contentEncodingCustomized_ = customized;
}

bool HttpMessage::isKeepAlive() const {
	return keepAlive_;
}

void HttpMessage::setKeepAlive(bool keepAlive) {
	keepAlive_ = keepAlive;
}

bool HttpMessage::matchContentTypeName(
		const char8_t *type, bool failOnUnmatch) {

	const HeaderField *field = findHeader(HEADER_CONTENT_TYPE);
	if (field == NULL) {
		if (failOnUnmatch) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
					"Content type is not specified (expected=" << type << ")");
		}
		return false;
	}

	bool matched = false;
	do {
		const String &value = field->value_;
		if (value.size() > strlen(type)) {
			FieldParser parser(value.c_str(), value.c_str() + value.size());

			StringRange storedType;
			StringRange storedSubType;
			if (!parser.nextToken(storedType) || !parser.nextLiteral("/") ||
					!parser.nextToken(storedSubType)) {
				break;
			}

			const Allocator &alloc = value.get_allocator();
			const String stored(storedType.first, storedSubType.second, alloc);
			matched = (FieldParser::compareToken(type, stored.c_str()) == 0);
			break;
		}

		matched = (FieldParser::compareToken(value.c_str(), type) == 0);
	}
	while (false);

	if (!matched && failOnUnmatch) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
				"Content type does not match ("
				"expected=" << type << ", actual=" << field->value_ << ")");
	}

	return matched;
}

bool HttpMessage::isJsonType() {
	return matchContentTypeName(CONTENT_TYPE_JSON);
}

picojson::value HttpMessage::toJsonValue() {
	matchContentTypeName(CONTENT_TYPE_JSON, true);

#if HTTP_JSON_ENABLD
	const char8_t *begin = static_cast<const char8_t*>(getContentData());
	const char8_t *end = begin + getContentSize();

	return JsonUtils::parseAll(begin, end);	
#else
	GS_COMMON_THROW_USER_ERROR(
			GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION, "");
#endif
}

void HttpMessage::setJsonValue(const picojson::value &value) {
#if HTTP_JSON_ENABLD
	const u8string content = value.serialize();
	Buffer &buf = prepareContentBuffer();
	buf.reserve(content.size());

	addHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON, true);
	buf.resize(0);
	appendBuffer(buf, content.c_str(), content.size());
#else
	GS_COMMON_THROW_USER_ERROR(
			GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION, "");
#endif
}

bool HttpMessage::acceptChunkedContent(
		const void *data, size_t size, bool eof) {
	const size_t crlfSize = sizeof(CRLF) - 1;

	contentBuf_.reserve(size);
	contentBuf_.clear();
	setContentRef(NULL, 0);

	bool checkOnly = true;
	for (;;) {
		const char8_t *it = static_cast<const char8_t*>(data);
		const char8_t *end = it + size;
		bool lastReached = false;
		bool completed = false;

		while (it != end) {
			const size_t maxSize = static_cast<size_t>(end - it);
			size_t chunkSize = 0;

			if (*it == '0') {
				it++;
				lastReached = true;
			}
			else {
				do {
					int32_t digit;
					if (FieldParser::charMatches(*it, CHARS_DIGIT)) {
						digit = *it - CHARS_DIGIT.first;
					}
					else if (FieldParser::charMatches(*it, CHARS_UPPER_ALPHA)) {
						digit = (*it - CHARS_UPPER_ALPHA.first) + 10;
					}
					else if (FieldParser::charMatches(*it, CHARS_LOWER_ALPHA)) {
						digit = (*it - CHARS_LOWER_ALPHA.first) + 10;
					}
					else {
						if (chunkSize == 0) {
							GS_COMMON_THROW_USER_ERROR(
									GS_ERROR_HTTP_INVALID_MESSAGE,
									"Invalid HTTP chunk size character");
						}
						break;
					}

					const size_t digitSize = static_cast<size_t>(digit);
					if (chunkSize > maxSize / 16 ||
							digitSize > maxSize - chunkSize * 16) {
						break;
					}
					chunkSize = chunkSize * 16 + digitSize;
				}
				while (++it != end);
			}

			const char8_t *lineEnd =
					HttpRequest::Parser::findStr(it, end - it, CRLF);
			if (lineEnd == NULL) {
				break;
			}

			if (*it != *CRLF && strchr(CHARS_SEPARATOR, *it) == NULL) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Invalid HTTP chunk extensions character");
				break;
			}

			it = lineEnd + crlfSize;
			if (lastReached ||
					chunkSize + crlfSize > static_cast<size_t>(end - it)) {
				break;
			}

			if (!checkOnly) {
				const size_t offset = contentBuf_.size();
				contentBuf_.resize(offset + chunkSize);
				memcpy(&contentBuf_[offset], it, chunkSize);
			}

			it += chunkSize;
			if (memcmp(it, CRLF, crlfSize) != 0) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Invalid HTTP chunk data delimiter");
			}
			it += crlfSize;
		}

		if (lastReached) {
			for (;;) {
				const char8_t *lineEnd =
						HttpRequest::Parser::findStr(it, end - it, CRLF);
				if (lineEnd == NULL) {
					break;
				}

				if (it == lineEnd) {
					it += crlfSize;
					if (it != end) {
						GS_COMMON_THROW_USER_ERROR(
								GS_ERROR_HTTP_INVALID_MESSAGE,
								"Unexpected character after HTTP chunked body");
					}
					completed = true;
					break;
				}
				it = lineEnd + crlfSize;
			}
		}

		if (!completed) {
			contentBuf_.clear();
			if (eof) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Unexpected end of stream at HTTP chunked body");
			}
			return false;
		}
		else if (!checkOnly) {
			return true;
		}
		checkOnly = false;
	}
}

const void* HttpMessage::getMessageData() const {
	return (messageBuf_.empty() ? messageRef_.first : messageBuf_.data());
}

size_t HttpMessage::getMessageSize() const {
	return (messageBuf_.empty() ? messageRef_.second : messageBuf_.size());
}

const void* HttpMessage::getContentData() const {
	return (contentBuf_.empty() ? contentRef_.first : contentBuf_.data());
}

size_t HttpMessage::getContentSize() const {
	return (contentBuf_.empty() ? contentRef_.second : contentBuf_.size());
}

void HttpMessage::setMessageRef(const void *data, size_t size) {
	messageBuf_.clear();
	messageRef_ = BufferRef(data, size);
	wroteSize_ = 0;
}

void HttpMessage::setContentRef(const void *data, size_t size) {
	contentBuf_.clear();
	contentRef_ = BufferRef(data, size);
}

HttpMessage::Buffer& HttpMessage::prepareMessageBuffer() {
	if (messageRef_.second > 0) {
		setMessageRef(NULL, 0);
		setContentRef(NULL, 0);
	}
	return messageBuf_;
}

HttpMessage::Buffer& HttpMessage::prepareContentBuffer() {
	if (contentRef_.second > 0) {
		setContentRef(NULL, 0);
	}
	return contentBuf_;
}

bool HttpMessage::readFrom(util::File &file, size_t *readSize) {
	if (readSize != NULL) {
		*readSize = 0;
	}

	const size_t initialSize = 1024;
	for (size_t expansion = initialSize;;) {
		const size_t curSize = messageBuf_.size();

		if (messageBuf_.capacity() - curSize < expansion) {
			messageBuf_.reserve(curSize + expansion);
		}
		const size_t extraSize = messageBuf_.capacity() - curSize;
		messageBuf_.resize(curSize + extraSize);

		int64_t subResult;
		try {
			subResult =
					file.read(messageBuf_.data() + curSize, extraSize);
		}
		catch (std::exception &e) {
			messageBuf_.resize(curSize);
			GS_COMMON_RETHROW_USER_ERROR(e, "");
		}

		if (subResult <= 0) {
			messageBuf_.resize(curSize);
			return (subResult == 0);
		}

		const size_t subSize = static_cast<size_t>(subResult);
		messageBuf_.resize(curSize + subSize);
		expansion = (subSize < extraSize ? 0 : initialSize);

		if (readSize != NULL) {
			*readSize += subSize;
		}
	}
}

bool HttpMessage::writeTo(util::File &file) {
	const size_t total = getMessageSize();
	const uint8_t *data = static_cast<const uint8_t*>(getMessageData());

	while (wroteSize_ < total) {
		int64_t wroteResult;
		try {
			const size_t rest = total - wroteSize_;
			wroteResult = file.write(data + wroteSize_, rest);
		}
		catch (std::exception &e) {
			GS_COMMON_RETHROW_USER_ERROR(e, "");
		}

		if (wroteResult < 0) {
			return false;
		}
		wroteSize_ += static_cast<size_t>(wroteResult);
	}
	return true;
}

bool HttpMessage::isWrote() const {
	return (wroteSize_ >= getMessageSize());
}

size_t& HttpMessage::getWroteSize() {
	return wroteSize_;
}

bool HttpMessage::isHttp10OrLess() const {
	return (version_.first < 1 ||
			(version_.first == 1 && version_.second == 0));
}

HttpMessage::Formatter HttpMessage::formatter() const {
	return Formatter(*this);
}

size_t HttpMessage::getHeaderSize() const {
	size_t size = 0;

	size += startLine_.size();
	size += strlen(CRLF);

	for (HeaderList::const_iterator it = headerList_.begin();
			it != headerList_.end(); ++it) {
		size += it->name_.size();
		size += strlen(": ");
		size += it->value_.size();
		size += strlen(CRLF);
	}
	size += strlen(CRLF);

	return size;
}

void HttpMessage::appendBuffer(Buffer &buf, const void *data, size_t size) {
	const size_t offset = buf.size();
	buf.resize(offset + size);
	memcpy(buf.data() + offset, data, size);
}

void HttpMessage::appendBuffer(Buffer &buf, const String &str) {
	appendBuffer(buf, str.c_str(), str.size());
}

void HttpMessage::appendBuffer(Buffer &buf, const char8_t *str) {
	appendBuffer(buf, str, strlen(str));
}

HttpMessage::HeaderField::HeaderField(
		const String &name, const String &value) :
		name_(name), value_(value) {
}

HttpMessage::FieldParser::FieldParser(
		const char8_t *begin, const char8_t *end) :
		src_(begin, end), cur_(begin), error_(NULL) {
	assert(begin <= end);
}

bool HttpMessage::FieldParser::nextSeparatedParameter(
		StringRange &name, StringRange &value, String &valueStorage,
		bool optional) {
	const char8_t *last = cur_;
	do {
		nextSpace(NULL, true);

		if (!nextLiteral(CHARS_SEMI, optional)) {
			break;
		}

		nextSpace(NULL, true);

		if (!nextParameter(name, value, valueStorage, optional)) {
			break;
		}
		return checkResult(true, optional, NULL);
	}
	while (false);

	return checkResult(false, optional, last);
}

bool HttpMessage::FieldParser::nextParameter(
		StringRange &name, StringRange &value, String &valueStorage,
		bool optional) {
	const char8_t *last = cur_;
	do {
		if (!nextToken(name, optional)) {
			break;
		}

		nextSpace(NULL, true);

		if (!nextLiteral(CHARS_EQ, optional)) {
			break;
		}

		nextSpace(NULL, true);

		if (!nextToken(value, optional) &&
				!nextQuotedString(value, valueStorage, optional)) {
			break;
		}
		return checkResult(true, optional, NULL);
	}
	while (false);

	name = StringRange();
	value = StringRange();
	return checkResult(false, optional, last);
}

bool HttpMessage::FieldParser::nextQuotedString(
		StringRange &value, String &storage, bool optional) {
	const char8_t *last = cur_;
	do {
		if (!nextLiteral(CHARS_DQUOTE, optional)) {
			break;
		}

		storage.clear();
		for (; cur_ != src_.second; ++cur_) {
			const bool escaping = matches(CHARS_BSLASH);
			if (escaping && ++cur_ == src_.second) {
				break;
			}

			if (!matches(CHARS_HTAB) && !matches(CHARS_SP) &&
					!matches(CHARS_VCHAR) && !matches(CHARS_OBS)) {
				break;
			}

			if (escaping) {
				if (storage.empty()) {
					storage.append(last + 1, cur_ - 1);
				}
				storage += *cur_;
			}
			else {
				if (matches(CHARS_EQ)) {
					break;
				}
			}
		}

		if (storage.empty()) {
			value = StringRange(last + 1, cur_);
		}
		else {
			value = StringRange(
					storage.c_str(), storage.c_str() + storage.size());
		}

		if (!nextLiteral(CHARS_DQUOTE, optional)) {
			break;
		}

		return checkResult(true, optional, NULL);
	}
	while (false);

	value = StringRange();
	return checkResult(false, optional, last);
}

bool HttpMessage::FieldParser::nextToken(StringRange &token, bool optional) {
	const char8_t *last = cur_;
	for (; cur_ != src_.second; ++cur_) {
		if (!matches(CHARS_VCHAR) || strchr(CHARS_SEPARATOR, *cur_) != NULL) {
			break;
		}
	}

	if (cur_ != src_.second && strchr(CHARS_SEPARATOR, *cur_) == NULL) {
		cur_ = last;
	}

	token = StringRange(last, cur_);
	return checkResult(token.first != token.second, optional, last);
}

bool HttpMessage::FieldParser::nextToken68(StringRange &token, bool optional) {
	const char8_t *last = cur_;

	bool eqStarted = false;
	for (; cur_ != src_.second; ++cur_) {
		if (cur_ != last && !eqStarted) {
			if (matches(CHARS_EQ)) {
				eqStarted = true;
				continue;
			}
		}

		if (eqStarted) {
			if (!matches(CHARS_EQ)) {
				break;
			}
		}
		else {
			if (!matches(CHARS_UPPER_ALPHA) &&
					!matches(CHARS_LOWER_ALPHA) &&
					!matches(CHARS_DIGIT) &&
					strchr(CHARS_TOKEN68_SYMBOL, *cur_) == NULL) {
				break;
			}
		}
	}

	if (cur_ != src_.second && strchr(CHARS_SEPARATOR, *cur_) == NULL) {
		cur_ = last;
	}

	token = StringRange(last, cur_);
	return checkResult(token.first != token.second, optional, last);
}

bool HttpMessage::FieldParser::nextLiteral(const char8_t *str, bool optional) {
	const char8_t *last = cur_;
	do {
		const size_t size = strlen(str);
		if (cur_ + size > src_.second) {
			break;
		}

		if (memcmp(cur_, str, size) != 0) {
			break;
		}

		cur_ += size;
		return checkResult(true, optional, NULL);
	}
	while (false);

	return checkResult(false, optional, last);
}

bool HttpMessage::FieldParser::nextElement(bool optional) {
	const char8_t *last = cur_;
	for (;; ++cur_) {
		if (cur_ != src_.first) {
			nextSpace(NULL, true);
		}

		if (cur_ == src_.second) {
			break;
		}

		if (!matches(CHARS_COMMA)) {
			return checkResult(true, optional, NULL);
		}
	}

	return checkResult(false, optional, last);
}

bool HttpMessage::FieldParser::nextSpace(StringRange *space, bool optional) {
	const char8_t *last = cur_;

	for (; cur_ != src_.second; ++cur_) {
		if (!matches(CHARS_HTAB) && !matches(CHARS_SP)) {
			break;
		}
	}

	if (space != NULL) {
		*space = StringRange(last, cur_);
	}

	return checkResult(cur_ != last, optional, last);
}

bool HttpMessage::FieldParser::checkEnd(bool optional) {
	return checkResult(cur_ == src_.second, optional, NULL);
}

const char8_t* HttpMessage::FieldParser::getError() const {
	return error_;
}

int32_t HttpMessage::FieldParser::compareToken(
		const char8_t *value1, const char8_t *value2) {
	const char8_t *it1 = value1;
	const char8_t *it2 = value2;
	for (;; ++it1, ++it2) {
		if (*it1 == '\0' || *it2 == '\0') {
			const size_t len1 = strlen(it1);
			const size_t len2 = strlen(it2);
			return (len1 < len2 ? -1 : (len1 > len2 ? 1 : 0));
		}

		const int32_t comp =
				normalizeTokenChar(*it1) - normalizeTokenChar(*it2);
		if (comp != 0) {
			return comp;
		}
	}
}

void HttpMessage::FieldParser::normalizeToken(String &value) {
	for (String::iterator it = value.begin(); it != value.end(); ++it) {
		*it = normalizeTokenChar(*it);
	}
}

char8_t HttpMessage::FieldParser::normalizeTokenChar(const char8_t ch) {
	if (charMatches(ch, CHARS_UPPER_ALPHA)) {
		return static_cast<char8_t>(ch -
				static_cast<int32_t>(CHARS_UPPER_ALPHA.first) +
				static_cast<int32_t>(CHARS_LOWER_ALPHA.first));
	}
	return ch;
}

bool HttpMessage::FieldParser::isDigit(const char8_t ch) {
	return charMatches(ch, CHARS_DIGIT);
}

bool HttpMessage::FieldParser::charMatches(
		const char8_t ch, const std::pair<uint8_t, uint8_t> &range) {
	return (range.first <= static_cast<uint8_t>(ch) &&
			static_cast<uint8_t>(ch) <= range.second);
}

bool HttpMessage::FieldParser::matches(const char8_t (&ch)[2]) const {
	return matches(*ch);
}

bool HttpMessage::FieldParser::matches(const char8_t ch) const {
	return (*cur_  == ch);
}

bool HttpMessage::FieldParser::matches(
		const std::pair<uint8_t, uint8_t> &range) const {
	return charMatches(*cur_, range);
}

bool HttpMessage::FieldParser::checkResult(
		bool matched, bool optional, const char8_t *last) {
	if (matched) {
		return true;
	}

	if (!optional && error_ != NULL) {
		error_ = cur_;
	}

	if (last != NULL) {
		cur_ = last;
	}

	return false;
}

HttpMessage::FieldBuilder::FieldBuilder(String &dest) :
		dest_(dest), elementPrepared_(false) {
}

void HttpMessage::FieldBuilder::prepareElement() {
	if (elementPrepared_) {
		dest_ += *CHARS_COMMA;
	}
	elementPrepared_ = true;
}

void HttpMessage::FieldBuilder::addSeparatedParameter(
		const char8_t *name, const char8_t *value) {
	dest_ += *CHARS_SEMI;
	addParameter(name, value);
}

void HttpMessage::FieldBuilder::addParameter(
		const char8_t *name, const char8_t *value) {
	dest_ += name;
	dest_ += *CHARS_EQ;
	addQuotable(value);
}

void HttpMessage::FieldBuilder::addQuotable(const char8_t *value) {
	const char8_t *const end = value + strlen(value);
	bool quoting = (value != end);
	if (!quoting) {
		for (const char8_t *it = value; it != end; ++it) {
			if (!FieldParser::charMatches(*it, CHARS_VCHAR) ||
					strchr(CHARS_SEPARATOR, *it) != NULL) {
				quoting = true;
				break;
			}
		}
	}

	if (quoting) {
		for (const char8_t *it = value; it != end; ++it) {
			if (*it == *CHARS_BSLASH || *it == *CHARS_DQUOTE) {
				dest_ += *CHARS_BSLASH;
			}
			dest_ += *it;
		}
	}
	else {
		dest_ += value;
	}
}

void HttpMessage::FieldBuilder::addString(const char8_t *value) {
	dest_ += value;
}

void HttpMessage::FieldBuilder::addSpace() {
	dest_ += *CHARS_SP;
}

const HttpMessage::String& HttpMessage::FieldBuilder::toString() const {
	return dest_;
}

HttpMessage::Formatter::Formatter(const HttpMessage &message) :
		message_(message),
		firstLine_(NULL),
		limit_(200) {
}

void HttpMessage::Formatter::setFirstLine(const char8_t *firstLine) {
	firstLine_ = firstLine;
}

void HttpMessage::Formatter::setLimit(size_t limit) {
	limit_ = limit;
}

void HttpMessage::Formatter::format(std::ostream &s) const {
	const size_t size = message_.getMessageSize();
	s << '"';

	const char8_t *it = static_cast<const char8_t*>(message_.getMessageData());
	const char8_t *const end = it + size;

	if (firstLine_ != NULL) {
		const char8_t *lineEnd = HttpRequest::Parser::findStr(it, size, CRLF);
		if (lineEnd != NULL) {
			it = lineEnd;
			s << firstLine_;
		}
	}

	size_t rest = limit_;
	for (; it != end; ++it) {
		if (HttpMessage::FieldParser::charMatches(*it, CHARS_VCHAR) ||
				*it == ' ') {
			if (rest <= 0) {
				break;
			}
			s << *it;
			rest--;
		}
		else if (*it == '\r' || *it == '\n' || *it == '\\' || *it == '"') {
			if (rest <= 1) {
				break;
			}
			switch (*it) {
			case '\r':
				s << "\\r";
				break;
			case '\n':
				s << "\\n";
				break;
			case '\\':
				s << "\\\\";
				break;
			case '"':
				s << "\\\"";
				break;
			}
			rest -= 2;
		}
		else {
			const char8_t *digits = "0123456789abcdef";
			if (rest <= 3) {
				break;
			}
			s << "\\x" <<
					digits[(static_cast<uint32_t>(*it) >> 4) & 0xf] <<
					digits[*it & 0xf];
			rest -= 4;
		}
	}
	s << '"';

	if (it != end) {
		s << "...(" << size << "bytes)";
	}
}

std::ostream& operator<<(
		std::ostream &s, const HttpMessage::Formatter &formatter) {
	formatter.format(s);
	return s;
}

const char8_t HttpAuth::HEADER_REQUEST[] = "Authorization";
const char8_t HttpAuth::HEADER_RESPONSE[] = "WWW-Authenticate";

HttpAuth::HttpAuth(const Allocator &alloc) :
		type_(TYPE_BASIC),
		paramList_(alloc),
		hashFunc_(NULL) {
}

void HttpAuth::initialize(Type type) {
	for (ParamList::iterator it = paramList_.begin();
			it != paramList_.end(); ++it) {
		it->first = false;
		it->second.clear();
	}

	type_ = type;

	if (type == TYPE_DIGEST) {
		setQopFlags(QOP_AUTH);
		setAlgorithm(ALGORITHM_MD5);
	}
}

void HttpAuth::setHashFunc(HashFunc *hashFunc) {
	hashFunc_ = hashFunc;
}

void HttpAuth::make(HttpMessage &message, bool requesting) const {
	message.addHeader(
			getHeaderName(requesting), make(requesting).c_str(), true);
}

HttpAuth::String HttpAuth::make(
		bool requesting, HttpMessage::FieldBuilder *builder) const {
	if (builder == NULL) {
		const Allocator &alloc = paramList_.get_allocator();

		String field(alloc);
		HttpMessage::FieldBuilder builderStorage(field);
		return make(requesting, &builderStorage);
	}

	builder->addString(getTypeName(type_, true));
	if (type_ == TYPE_BASIC && requesting) {
		builder->addSpace();

		u8string userPass;
		userPass += getParam(PARAM_USERNAME);
		userPass += ":";
		userPass += getParam(PARAM_PASSWORD);

		util::NormalIStringStream iss(userPass);
		util::NormalOStringStream oss;
		util::HexConverter::encode(oss, iss);

		builder->addString(oss.str().c_str());
	}
	else if (type_ == TYPE_DIGEST) {
		builder->addSpace();

		for (ParamList::const_iterator it = paramList_.begin();
				it != paramList_.end(); ++it) {
			if (it->first) {
				const Param param =
						static_cast<Param>(paramList_.begin() - it);
				builder->prepareElement();
				builder->addParameter(
						getParamName(param, true), it->second.c_str());
			}
		}
	}

	return builder->toString();
}

bool HttpAuth::accept(HttpMessage &request, bool requesting) {
	const HttpMessage::HeaderField *field =
			request.findHeader(getHeaderName(requesting));
	if (field == NULL) {
		return false;
	}

	return accept(field->value_.c_str(), requesting);
}

bool HttpAuth::accept(const char8_t *value, bool requesting) {
	typedef HttpMessage::StringRange StringRange;

	const Allocator &alloc = paramList_.get_allocator();

	HttpAuth another(alloc);
	String valueStorage(alloc);

	HttpMessage::FieldParser parser(value, value + strlen(value));

	const bool parameterExpected = !(type_ == TYPE_BASIC && requesting);
	bool typeMatched = false;
	while (parser.checkEnd(true)) {
		if (!parser.nextElement(false)) {
			break;
		}

		StringRange name;
		StringRange value;
		if (!typeMatched) {
			if (!parser.nextToken(value, true)) {
				continue;
			}
			const String valueStr(value.first, value.second, alloc);

			Type type;
			if (resolveTypeName(valueStr.c_str(), &type) && type == type_) {
				typeMatched = true;
				another.type_ = type_;
				another.paramList_ = newParamList(alloc);
			}
			else {
				typeMatched = false;
			}
		}
		else if (parser.nextParameter(name, value, valueStorage, true)) {
			if (!parameterExpected) {
				continue;
			}

			const String nameStr(name.first, name.second, alloc);

			Param param;
			if (resolveParamName(nameStr.c_str(), &param)) {
				const String valueStr(value.first, value.second, alloc);
				another.setParam(param, valueStr.c_str());
			}
		}
		else if (parser.nextToken68(value, true)) {
			if (parameterExpected) {
				continue;
			}

			util::NormalIStringStream iss(u8string(value.first, value.second));
			util::NormalOStringStream oss;
			util::HexConverter::decode(oss, iss);

			const u8string &userPassStr = oss.str();
			const size_t pos = userPassStr.find(':');
			if (pos == u8string::npos) {
				continue;
			}

			const char8_t *userPass = userPassStr.c_str();
			const String user(userPass, userPass + pos, alloc);
			const String password(
					userPass + pos, userPass + userPassStr.size(), alloc);

			setParam(PARAM_USERNAME, user.c_str());
			setParam(PARAM_PASSWORD, password.c_str());

			typeMatched = false;
		}
		else {
			typeMatched = false;
		}
	}

	const bool found = !another.paramList_.empty();
	if (found) {
		another.validate(requesting);
	}

	paramList_.swap(another.paramList_);
	return found;
}

void HttpAuth::validate(bool requesting) const {
	const ParamList &list = paramList_;

	const int32_t qopFlags = getQopFlags();
	const Algorithm algorithm = getAlgorithm();

	if (requesting) {
		if (type_ == TYPE_BASIC) {
			if (!list[PARAM_USERNAME].first ||
					!list[PARAM_PASSWORD].first) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Some of authentication parameters are not specified "
						"in basic authentication");
			}
		}
		else if (type_ == TYPE_DIGEST) {
			const bool ncRequired = (qopFlags != QOP_NONE);
			const bool cnonceRequired =
					(ncRequired || algorithm == ALGORITHM_MD5_SESS);

			if (!list[PARAM_REALM].first ||
					!list[PARAM_NONCE].first ||
					!list[PARAM_USERNAME].first ||
					!list[PARAM_URI].first ||
					!list[PARAM_OPAQUE].first ||
					(ncRequired && !list[PARAM_NC].first) ||
					(cnonceRequired && !list[PARAM_CNONCE].first) ||
					!list[PARAM_RESPONSE].first) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Some of authentication parameters are not specified "
						"in digest authentication");
			}
		}
	}
	else {
		if (type_ == TYPE_BASIC) {
			if (!list[PARAM_REALM].first) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Realm is not specified in basic authentication");
			}
		}
		else if (type_ == TYPE_DIGEST) {
			if (!list[PARAM_REALM].first ||
					!list[PARAM_NONCE].first ||
					!list[PARAM_OPAQUE].first) {
				GS_COMMON_THROW_USER_ERROR(
						GS_ERROR_HTTP_INVALID_MESSAGE,
						"Some of authentication parameters are not specified "
						"in digest authentication");
			}
		}
	}
}

HttpAuth::Type HttpAuth::getType() const {
	return type_;
}

void HttpAuth::setSecret(
			const char8_t *method, const char8_t *secret,
			const char8_t *bodyHash) {
	if (type_ == TYPE_BASIC) {
		setParam(PARAM_PASSWORD, secret);
	}
	else if (type_ == TYPE_DIGEST) {
		if (hashFunc_ == NULL) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION, "");
		}

		const int32_t qopFlags = getQopFlags();
		const Algorithm algorithm = getAlgorithm();

		const Allocator &alloc = paramList_.get_allocator();

		String ha1(alloc);
		if (algorithm == ALGORITHM_MD5_SESS) {
			String base(alloc);
			base += secret;
			base += ":";
			base += getParam(PARAM_NONCE);
			base += ":";
			base += getParam(PARAM_CNONCE);
			(*hashFunc_)(base, &ha1);
		}
		else {
			ha1 = secret;
		}

		String ha2(alloc);
		{
			String base(alloc);
			base += method;
			base += ":";
			base += getParam(PARAM_URI);
			if (qopFlags == QOP_AUTH_INT) {
				if (bodyHash == NULL) {
					GS_COMMON_THROW_USER_ERROR(
							GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
				}
				base += ":";
				base += bodyHash;
			}
			(*hashFunc_)(base, &ha2);
		}

		String resp(alloc);
		{
			String base(alloc);
			base += ha1;
			base += ":";
			base += getParam(PARAM_NONCE);
			if (qopFlags != QOP_NONE) {
				base += ":";
				base += getParam(PARAM_NC);
				base += ":";
				base += getParam(PARAM_CNONCE);
				base += ":";
				base += getParam(PARAM_QOP);
			}
			base += ":";
			base += ha2;
			(*hashFunc_)(base, &resp);
		}
		setParam(PARAM_RESPONSE, resp.c_str());
	}
}

int32_t HttpAuth::getQopFlags() const {
	const char8_t *strValue = findParam(PARAM_QOP);

	if (strValue == NULL) {
		return QOP_NONE;
	}

	const Allocator &alloc = paramList_.get_allocator();

	int32_t flags = 0;
	const char8_t *const end = strValue + strlen(strValue);
	for (const char8_t *it = strValue; it != end;) {
		const char8_t *delim = strchr(it, ',');
		if (delim == NULL) {
			delim = end;
		}

		const String flagStr(it, delim, alloc);
		int32_t curFlag = 0;
		for (int32_t i = QOP_NONE << 1; i != MAX_QOP; i <<= 1) {
			const Qop qop = static_cast<Qop>(i);
			if (HttpMessage::FieldParser::compareToken(
					strValue, getQopName(qop, true)) == 0) {
				curFlag = i;
			}
		}

		if (curFlag == 0) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INVALID_MESSAGE,
					"Unknown qop value in digest authentication "
					"(value=" << flagStr << ", qop=" << strValue << ")");
		}

		flags |= curFlag;
		it = delim + (delim == end ? 0 : 1);
	}

	if (flags == 0) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_INVALID_MESSAGE,
				"Illegal qop in digest authentication "
				"(qop=" << strValue << ")");
	}

	return flags;
}

void HttpAuth::setQopFlags(int32_t value) {
	if (value == QOP_NONE) {
		setParam(PARAM_QOP, NULL);
		return;
	}

	const Allocator &alloc = paramList_.get_allocator();
	String flagsStr(alloc);

	for (int32_t i = QOP_NONE << 1; i != MAX_QOP; i <<= 1) {
		if ((value & i) != 0) {
			if (!flagsStr.empty()) {
				flagsStr += ",";
			}
			flagsStr += getQopName(static_cast<Qop>(i), true);
		}
	}

	setParam(PARAM_QOP, flagsStr.c_str());
}

HttpAuth::Algorithm HttpAuth::getAlgorithm() const {
	const char8_t *strValue = findParam(PARAM_ALGORITHM);

	if (strValue == NULL) {
		return ALGORITHM_MD5;
	}

	for (int32_t i = 0; i != MAX_ALGORITHM; i++) {
		const Algorithm value = static_cast<Algorithm>(i);
		if (HttpMessage::FieldParser::compareToken(
				strValue, getAlgorithmName(value, true)) == 0) {
			return value;
		}
	}

	GS_COMMON_THROW_USER_ERROR(
			GS_ERROR_HTTP_INVALID_MESSAGE,
			"Unknown algorithm in digest authentication "
			"(algorithm=" << strValue << ")");
}

void HttpAuth::setAlgorithm(Algorithm value) {
	setParam(PARAM_ALGORITHM, getAlgorithmName(value, true));
}

const char8_t* HttpAuth::getParam(Param param) const {
	const char8_t *value = findParam(param);
	if (value == NULL) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
	}

	return value;
}

const char8_t* HttpAuth::findParam(Param param) const {
	if (paramList_.empty()) {
		return NULL;
	}

	const Value &src = paramList_[param];
	return (src.first ? src.second.c_str() : NULL);
}

void HttpAuth::setParam(Param param, const char8_t *value) {
	if (paramList_.empty()) {
		const Allocator &alloc = paramList_.get_allocator();
		paramList_ = newParamList(alloc);
	}

	Value &dest = paramList_[param];

	dest.first = (value != NULL);
	if (dest.first) {
		dest.second = value;
	}
}

bool HttpAuth::resolveParamName(const char8_t *str, Param *value) {
	*value = MAX_PARAM;
	for (int32_t i = 0; i < MAX_PARAM; i++) {
		const Param param = static_cast<Param>(i);
		const char8_t *name = getParamName(param, false);
		if (name == NULL) {
			continue;
		}
		if (HttpMessage::FieldParser::compareToken(str, name) == 0) {
			*value = param;
			return true;
		}
	}
	return false;
}

bool HttpAuth::resolveTypeName(const char8_t *str, Type *value) {
	*value = MAX_TYPE;
	for (int32_t i = 0; i < MAX_TYPE; i++) {
		const Type type = static_cast<Type>(i);
		const char8_t *name = getTypeName(type, false);
		if (name == NULL) {
			continue;
		}
		if (HttpMessage::FieldParser::compareToken(str, name) == 0) {
			*value = type;
			return true;
		}
	}
	return false;
}

const char8_t* HttpAuth::getHeaderName(bool requesting) {
	return (requesting ? HEADER_REQUEST : HEADER_RESPONSE);
}

const char8_t* HttpAuth::getTypeName(Type type, bool failOnUnknown) {
	switch (type) {
	case TYPE_BASIC:
		return "Basic";
	case TYPE_DIGEST:
		return "Digest";
	default:
		if (failOnUnknown) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
		}
		return NULL;
	}
}

const char8_t* HttpAuth::getParamName(Param param, bool failOnUnknown) {
	switch (param) {
	case PARAM_REALM:
		return "realm";
	case PARAM_QOP:
		return "qop";
	case PARAM_NONCE:
		return "nonce";
	case PARAM_OPAQUE:
		return "opaque";
	case PARAM_USERNAME:
		return "username";
	case PARAM_URI:
		return "uri";
	case PARAM_ALGORITHM:
		return "algorithm";
	case PARAM_NC:
		return "nc";
	case PARAM_CNONCE:
		return "cnonce";
	case PARAM_RESPONSE:
		return "response";
	case PARAM_PASSWORD:
		break;
	default:
		break;
	}

	if (failOnUnknown) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
	}
	return NULL;
}

const char8_t* HttpAuth::getQopName(Qop qop, bool failOnUnknown) {
	switch (qop) {
	case QOP_AUTH:
		return "auth";
	case QOP_AUTH_INT:
		return "auth-int";
	default:
		if (failOnUnknown) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
		}
		return NULL;
	}
}

const char8_t* HttpAuth::getAlgorithmName(
		Algorithm Algorithm, bool failOnUnknown) {
	switch (Algorithm) {
	case ALGORITHM_MD5:
		return "MD5";
	case ALGORITHM_MD5_SESS:
		return "MD5-sess";
	default:
		if (failOnUnknown) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
		}
		return NULL;
	}
}

HttpAuth::ParamList HttpAuth::newParamList(const Allocator &alloc) {
	ParamList list(alloc);
	list.resize(MAX_PARAM, Value(false, String(alloc)));
	return list;
}

const uint16_t HttpRequest::DEFAULT_HTTP_PORT = 80;

const HttpRequest::Method HttpRequest::METHOD_GET = EBB_GET;
const HttpRequest::Method HttpRequest::METHOD_POST = EBB_POST;

HttpRequest::HttpRequest(const Allocator &alloc) :
		message_(alloc),
		pathElements_(alloc),
		parameterMap_(ParameterMap::key_compare(), alloc),
		fragment_(alloc),
		directory_(false),
		scheme_(alloc),
		method_(METHOD_GET),
		host_(alloc),
		port_(DEFAULT_HTTP_PORT) {
}

HttpMessage& HttpRequest::getMessage() {
	return message_;
}

const HttpMessage& HttpRequest::getMessage() const {
	return message_;
}

void HttpRequest::clear() {
	message_.clear();
	pathElements_.clear();
	parameterMap_.clear();
	fragment_.clear();
	directory_ = false;
	scheme_.clear();
	method_ = METHOD_GET;
	host_.clear();
	port_ = DEFAULT_HTTP_PORT;
}

HttpMessage* HttpRequest::parse(bool eof) {
	Parser parser(*this);
	if (!parser.parse(eof)) {
		return NULL;
	}

	return &message_;
}

HttpMessage& HttpRequest::build() {
	const HttpMessage::Version &version = message_.getVersion();
	if (version.first > 1 || (version.first == 1 && version.second >= 1)) {
		if (!host_.empty()) {
			util::NormalOStringStream oss;
			oss << host_;
			if (port_ != DEFAULT_HTTP_PORT) {
				oss << ":";
				oss << port_;
			}
			message_.addHeader(
					HttpMessage::HEADER_HOST, oss.str().c_str(), true);
		}
	}

	util::NormalOStringStream oss;
	oss << methodToString(method_, true);
	oss << " ";

	for (PathElements::const_iterator it = pathElements_.begin();
			it != pathElements_.end(); ++it) {
		oss << "/";
		encodeURL(oss, it->c_str());
	}

	if (pathElements_.empty() || directory_) {
		oss << "/";
	}

	if (!parameterMap_.empty()) {
		oss << "?";
		encodeQueryString(oss);
	}

	if (!fragment_.empty()) {
		oss << "#";
		encodeURL(oss, fragment_.c_str());
	}

	oss << " HTTP/";
	oss << version.first;
	oss << ".";
	oss << version.second;

	message_.setStartLine(oss.str().c_str());
	message_.build();

	return message_;
}

HttpRequest::PathElements& HttpRequest::getPathElements() {
	return pathElements_;
}

HttpRequest::ParameterMap& HttpRequest::getParameterMap() {
	return parameterMap_;
}

HttpRequest::String& HttpRequest::getFragment() {
	return fragment_;
}

bool HttpRequest::isDirectory() const {
	return directory_;
}

void HttpRequest::setDirectory(bool directory) {
	directory_ = directory;
}

HttpRequest::Method HttpRequest::getMethod() const {
	return method_;
}

void HttpRequest::setMethod(Method method) {
	method_ = method;
}

void HttpRequest::acceptURL(const char8_t *url) {
	scheme_.clear();
	host_.clear();
	port_ = 80;
	acceptPath("");

	const char8_t *end = url + strlen(url);
	const char8_t *it = url;
	if (it == end) {
		return;
	}

	const char8_t *path = it;
	do {
		if (*it == '/') {
			break;
		}

		const char8_t *schemeSep = "://";
		it = strstr(it, schemeSep);
		if (it == NULL) {
			return;
		}
		scheme_.assign(url, it);
		it += strlen(schemeSep);

		const char8_t *host = it;
		path = strchr(it, '/');
		if (path == NULL) {
			path = end;
		}

		const char8_t *portStr = NULL;
		for (it = path; it != host;) {
			if (!HttpMessage::FieldParser::isDigit(*(--it))) {
				if (*it == ':') {
					portStr = it + 1;
					break;
				}
			}
		}

		if (portStr == NULL) {
			it = path;
		}
		else {
			port_ = util::LexicalConverter<uint16_t>()(
					u8string(portStr, path));
		}

		host_.assign(host, it);
	}
	while (false);
	acceptPath(path);
}

void HttpRequest::acceptPath(const char8_t *path) {
	pathElements_.clear();
	parameterMap_.clear();
	fragment_.clear();

	const char8_t *end = path + strlen(path);
	if (path == end) {
		return;
	}

	Parser parser(*this);
	ebb_request &request = parser.getRequest();

	const char8_t *fragment = strchr(path, '#');
	const char8_t *queryEnd = end;
	if (fragment != NULL) {
		queryEnd = fragment;
		fragment++;
	}

	const char8_t *query = strchr(path, '?');
	if (query != NULL) {
		query++;
	}

	Parser::acceptPath(&request, path, end - path);

	if (query != NULL) {
		Parser::acceptQueryString(&request, query, queryEnd - query);
	}

	if (fragment != NULL) {
		Parser::acceptFragment(&request, fragment, end - fragment);
	}
}

const char8_t* HttpRequest::getScheme() const {
	return scheme_.c_str();
}

const char8_t* HttpRequest::getHost() const {
	return host_.c_str();
}

uint16_t HttpRequest::getPort() const {
	return port_;
}

const char8_t* HttpRequest::methodToString(Method method, bool failOnUnknown) {
	switch (method) {
	case METHOD_GET:
		return "GET";
	case METHOD_POST:
		return "POST";
	default:
		if (failOnUnknown) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER, "");
		}
		return NULL;
	}
}

HttpRequest::Formatter HttpRequest::formatter() const {
	return Formatter(*this);
}

void HttpRequest::encodeQueryString(std::ostream &os) {
	for (ParameterMap::const_iterator it = parameterMap_.begin();
			it != parameterMap_.end(); ++it) {
		if (it != parameterMap_.begin()) {
			os << "&";
		}
		encodeURL(os, it->first.c_str());
		os << "=";
		encodeURL(os, it->second.c_str());
	}
}

void HttpRequest::encodeURL(std::ostream &os, const char8_t *str) {
	util::NormalIStringStream iss((u8string(str)));
	util::URLConverter::encode(os, iss);
}

HttpRequest::Formatter::Formatter(const HttpRequest &request) :
		base_(request.getMessage()) {
}

HttpRequest::Formatter::Base& HttpRequest::Formatter::base() {
	return base_;
}

void HttpRequest::Formatter::format(std::ostream &s) const {
	s << base_;
}

std::ostream& operator<<(
		std::ostream &s, const HttpRequest::Formatter &formatter) {
	formatter.format(s);
	return s;
}

HttpRequest::Parser::Parser(HttpRequest &base) :
		base_(base),
		message_(&base.getMessage()),
		fieldName_(base.pathElements_.get_allocator()),
		fieldValue_(base.pathElements_.get_allocator()) {
	ebb_request_init(&request_);
	request_.data = this;
}

void HttpRequest::Parser::setMessage(HttpMessage &message) {
	message_ = &message;
}

bool HttpRequest::Parser::parse(bool eof) {
	const char8_t *data =
			static_cast<const char8_t*>(message_->getMessageData());
	const size_t dataSize = message_->getMessageSize();

	const char8_t *const separator = "\r\n\r\n";
	const char8_t *const headerEnd = findStr(data, dataSize, separator);
	if (headerEnd == NULL) {
		expectMore(eof);
		return false;
	}

	base_.pathElements_.clear();
	base_.parameterMap_.clear();
	base_.fragment_.clear();
	fieldName_.clear();
	fieldValue_.clear();

	ebb_request_parser parser;
	ebb_request_parser_init(&parser);
	parser.data = &request_;

	request_.on_path = acceptPath;
	request_.on_query_string = acceptQueryString;
	request_.on_fragment = acceptFragment;
	request_.on_header_field = acceptHeaderField;
	request_.on_header_value = acceptHeaderValue;
	parser.new_request = getParserRequest;

	ebb_request_parser_execute(&parser, data, dataSize);
	if (parser.current_request != NULL) {
		expectMore(eof);
		return false;
	}

	flushHeaderField();

	const char8_t *const body = headerEnd + strlen(separator);
	const size_t bodySize = data + dataSize - body;

	if ((request_.transfer_encoding & EBB_CHUNKED) == 0) {
		if (request_.content_length > 0 &&
				bodySize < request_.content_length) {
			expectMore(eof);
			return false;
		}
		message_->setContentRef(body, bodySize);
	}
	else {
		if (!message_->acceptChunkedContent(body, bodySize, eof)) {
			return false;
		}
	}

	if (message_->matchContentTypeName(
			HttpMessage::CONTENT_TYPE_FORM) ||
			message_->matchContentTypeName(
					HttpMessage::CONTENT_TYPE_X_FORM)) {
		acceptQueryString(&request_, body, bodySize);
	}

	base_.setMethod(request_.method);
	message_->getVersion() = HttpMessage::Version(
			request_.version_major, request_.version_minor);

	{
		HttpMessage::HeaderField *field = (message_->isHttp10OrLess() ?
				NULL : message_->findHeader(HttpMessage::HEADER_CONNECTION));
		message_->setKeepAlive(
				!message_->isHttp10OrLess() && (field == NULL ||
				HttpMessage::FieldParser::compareToken(
						field->value_.c_str(),
						HttpMessage::CONNECTION_CLOSE) != 0));
	}

	if (message_->isHttp10OrLess() && !eof &&
			message_->findHeader(
					HttpMessage::HEADER_CONTENT_LENGTH) == NULL) {
		return false;
	}

	if (!message_->isContentEncodingCustomized()) {
		const HttpMessage::HeaderField *encoding = message_->findHeader(
				HttpMessage::HEADER_CONTENT_ENCODING);
		if (encoding != NULL) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_HTTP_INVALID_MESSAGE,
					"Unsupported content encoding ("
					"value=" << encoding->value_ << ")");
		}
	}

	return true;
}

void HttpRequest::Parser::expectMore(bool eof) {
	if (eof) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_INVALID_MESSAGE,
				"Connection unexpectedly closed or incomplete HTTP message");
	}
}

bool HttpRequest::Parser::rewriteToRequest(
		int32_t *status, String *firstLine) {
	HttpMessage::Buffer &buf = message_->prepareMessageBuffer();

	char8_t *data = reinterpret_cast<char8_t*>(buf.data());
	const size_t dataSize = buf.size();

	const char8_t *const separator = HttpMessage::CRLF;
	const char8_t *lineEnd = findStr(data, dataSize, separator);
	const char8_t *version = findStr(data, dataSize, "HTTP/");
	if (lineEnd == NULL || version == NULL) {
		return false;
	}
	else if (version != data) {
		return true;
	}
	const size_t lineSize = lineEnd - data;

	firstLine->assign(data, lineSize);

	size_t versionSize = 0;
	size_t statusPos = 0;
	for (; versionSize < lineSize; versionSize++) {
		size_t spaceCount = 0;
		for (;; spaceCount++) {
			const char8_t ch = data[versionSize + spaceCount];
			if (ch != ' ' && ch != '\t' && ch != '\r') {
				break;
			}
		}
		if (spaceCount > 0) {
			statusPos = std::min(versionSize + spaceCount, lineSize);
			break;
		}
	}

	const size_t statusSize = std::max(statusPos,
			std::min(statusPos + 3, lineSize)) - statusPos;
	*status = util::LexicalConverter<int32_t>()(
			u8string(data + statusPos, statusSize));

	const char8_t *prefix = "GET /";
	size_t prefixSize = strlen(prefix);

	size_t paddingSize;
	const size_t minLineSize = prefixSize + 1 + versionSize;
	if (minLineSize > lineSize) {
		assert(dataSize == buf.size());

		buf.resize(dataSize + (minLineSize - lineSize));
		data = reinterpret_cast<char8_t*>(buf.data());
		memmove(
				data + minLineSize, data + lineSize, dataSize - lineSize);

		paddingSize = 0;
	}
	else {
		paddingSize = lineSize - minLineSize;
	}

	String line(fieldName_.get_allocator());
	line.append(prefix);
	line.append(paddingSize, 'a');
	line.append(" ");
	line.append(data, versionSize);
	assert(line.size() == std::max<size_t>(lineSize, minLineSize));

	memcpy(buf.data(), line.c_str(), line.size());
	return true;
}

ebb_request& HttpRequest::Parser::getRequest() {
	return request_;
}

void HttpRequest::Parser::flushHeaderField() {
	if (fieldName_.empty()) {
		return;
	}

	message_->addHeader(fieldName_.c_str(), fieldValue_.c_str(), false);
	fieldName_.clear();
	fieldValue_.clear();
}

void HttpRequest::Parser::acceptPath(
		ebb_request *request, const char8_t *at, size_t length) {
	HttpRequest &base = getBase(request);
	const Allocator &alloc = base.pathElements_.get_allocator();

	const char8_t *const end = at + length;
	for (const char8_t *entry = at; entry != end;) {
		const char8_t *const elementEnd = std::find(entry, end, '/');
		if (elementEnd == entry) {
			entry++;
			continue;
		}

		util::NormalIStringStream elementIn(u8string(entry, elementEnd));

		util::NormalOStringStream elementOut;
		util::URLConverter::decode(elementOut, elementIn);

		base.pathElements_.push_back(String(elementOut.str().c_str(), alloc));
		entry = elementEnd;
	}

	base.directory_ = (length == 0 || at[length - 1] == '/');
}

void HttpRequest::Parser::acceptQueryString(
		ebb_request *request, const char8_t *at, size_t length) {
	HttpRequest &base = getBase(request);
	const Allocator &alloc = base.parameterMap_.get_allocator();

	const char8_t *end = at + length;
	for (const char8_t *entry = at; entry != end;) {
		const char8_t *const entryEnd = std::find(entry, end, '&');
		if (entryEnd == entry) {
			entry++;
			continue;
		}
		const char8_t *const nameEnd = std::find(entry, entryEnd, '=');

		util::NormalIStringStream nameIn(u8string(entry, nameEnd));
		util::NormalIStringStream valueIn(u8string(
				(nameEnd == entryEnd ? entryEnd : nameEnd + 1), entryEnd));

		util::NormalOStringStream nameOut;
		util::NormalOStringStream valueOut;
		util::URLConverter::decode(nameOut, nameIn);
		util::URLConverter::decode(valueOut, valueIn);

		base.parameterMap_.insert(std::make_pair(
				String(nameOut.str().c_str(), alloc),
				String(valueOut.str().c_str(), alloc)));
		entry = entryEnd;
	}
}

void HttpRequest::Parser::acceptFragment(
		ebb_request *request, const char8_t *at, size_t length) {
	HttpRequest &base = getBase(request);

	util::NormalIStringStream fragmentIn(u8string(at, length));

	util::NormalOStringStream fragmentOut;
	util::URLConverter::decode(fragmentOut, fragmentIn);
	base.fragment_ = fragmentOut.str().c_str();
}

void HttpRequest::Parser::acceptHeaderField(
		ebb_request *request, const char8_t *at, size_t length, int index) {
	static_cast<void>(index);

	Parser &parser = getParser(request);
	parser.flushHeaderField();

	parser.fieldName_.assign(at, length);
}

void HttpRequest::Parser::acceptHeaderValue(
		ebb_request *request, const char8_t *at, size_t length, int index) {
	static_cast<void>(index);

	Parser &parser = getParser(request);

	parser.fieldValue_.append(at, length);
}

ebb_request* HttpRequest::Parser::getParserRequest(void *data) {
	return static_cast<ebb_request*>(data);
}

HttpRequest::Parser& HttpRequest::Parser::getParser(ebb_request *request) {
	assert(request->data != NULL);
	return *static_cast<HttpRequest::Parser*>(request->data);
}

HttpRequest& HttpRequest::Parser::getBase(ebb_request *request) {
	return getParser(request).base_;
}

HttpMessage& HttpRequest::Parser::getMessage(ebb_request *request) {
	return *getParser(request).message_;
}

const char8_t* HttpRequest::Parser::findStr(
		const char8_t *buf, size_t bufSize, const char8_t *key)
{
	const size_t keySize = strlen(key);
	if (keySize == 0) {
		return buf;
	}
	else if (bufSize < keySize) {
		return NULL;
	}

	const char8_t *end = buf + (bufSize - keySize) + 1;
	for (const char8_t *it = buf; it < end;) {
		const char8_t *cur =
				static_cast<const char8_t*>(memchr(it, key[0], end - it));
		if (cur == NULL) {
			return NULL;
		}
		if (memcmp(cur + 1, key + 1, keySize - 1) == 0) {
			return cur;
		}
		it = cur + 1;
	}
	return NULL;
}

HttpResponse::HttpResponse(const Allocator &alloc) :
		alloc_(alloc),
		message_(alloc),
		status_(HTTP_STATUS_SUCCESS),
		firstLine_(alloc) {
}

HttpMessage& HttpResponse::getMessage() {
	return message_;
}

const HttpMessage& HttpResponse::getMessage() const {
	return message_;
}

void HttpResponse::clear() {
	message_.clear();
	status_ = HTTP_STATUS_SUCCESS;
}

HttpMessage* HttpResponse::parse(bool eof) {
	HttpRequest request(alloc_);
	HttpRequest::Parser parser(request);

	parser.setMessage(message_);
	if (!parser.rewriteToRequest(&status_, &firstLine_)) {
		return NULL;
	}

	if (!parser.parse(eof)) {
		return NULL;
	}

	return &message_;
}

HttpMessage& HttpResponse::build() {
	util::NormalOStringStream oss;

	oss << " HTTP/";
	oss << message_.getVersion().first;
	oss << ".";
	oss << message_.getVersion().second;

	oss << " ";
	oss << status_;

	message_.setStartLine(oss.str().c_str());
	message_.build();

	return message_;
}

HttpResponse::Status HttpResponse::getStatus() const {
	return status_;
}

void HttpResponse::setStatus(Status status) {
	status_ = status;
}

const char8_t* HttpResponse::getFirstLine() const {
	return firstLine_.c_str();
}

bool HttpResponse::isSuccess() const {
	return status_ / 100 == HTTP_STATUS_SUCCESS / 100;
}

void HttpResponse::checkSuccess() const {
	if (!isSuccess()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
				"Unexpected response status code (code=" << status_ << ")");
	}
}

bool HttpResponse::isRedirection() const {
	return status_ / 100 == HTTP_STATUS_REDIRECTION / 100;
}

const char8_t* HttpResponse::getLocation() {
	HttpMessage::HeaderField *field = message_.findHeader("Location");
	if (field == NULL) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
				"Location header is not found");
	}

	return field->value_.c_str();
}

void HttpResponse::setBadRequestError() {
	status_ = 400;
}

void HttpResponse::setAuthError() {
	status_ = 401;
}

void HttpResponse::setMethodError() {
	status_ = 405;
}

void HttpResponse::setInternalServerError() {
	status_ = 500;
}

HttpResponse::Formatter HttpResponse::formatter() const {
	return HttpResponse::Formatter(*this);
}

HttpResponse::Formatter::Formatter(const HttpResponse &response) :
		base_(response.getMessage()) {
	base_.setFirstLine(response.getFirstLine());
}

HttpResponse::Formatter::Base& HttpResponse::Formatter::base() {
	return base_;
}

void HttpResponse::Formatter::format(std::ostream &s) const {
	s << base_;
}

std::ostream& operator<<(
		std::ostream &s, const HttpResponse::Formatter &formatter) {
	formatter.format(s);
	return s;
}
