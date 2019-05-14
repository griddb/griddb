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
	@brief Definitions of http message classes
*/
#ifndef HTTP_H_
#define HTTP_H_

#include "util/container.h"

#ifndef HTTP_JSON_ENABLD
#define HTTP_JSON_ENABLD 1
#endif

namespace picojson {
class value;
} 

class HttpMessage {
public:
	static const int32_t DEFAULT_VERSION_MAJOR;
	static const int32_t DEFAULT_VERSION_MINOR;

	static const char8_t HEADER_ACCEPT[];
	static const char8_t HEADER_ACCEPT_ENCODING[];
	static const char8_t HEADER_CONNECTION[];
	static const char8_t HEADER_CONTENT_ENCODING[];
	static const char8_t HEADER_CONTENT_TYPE[];
	static const char8_t HEADER_CONTENT_LENGTH[];
	static const char8_t HEADER_HOST[];

	static const char8_t CONNECTION_CLOSE[];

	static const char8_t CONTENT_TYPE_OCTET_STREAM[];
	static const char8_t CONTENT_TYPE_JSON[];
	static const char8_t CONTENT_TYPE_FORM[];
	static const char8_t CONTENT_TYPE_X_FORM[];

	static const char8_t CONTENT_CODING_IDENTITY[];

	static const char8_t CRLF[];

	struct HeaderField;
	class FieldParser;
	class FieldBuilder;
	class Formatter;

	typedef util::StdAllocator<void, void> Allocator;
	typedef util::BasicString< char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, void> > String;

	typedef std::pair<int32_t, int32_t> Version;
	typedef std::vector<
			HeaderField, util::StdAllocator<HeaderField, void> > HeaderList;
	typedef util::XArray<
			uint8_t, util::StdAllocator<uint8_t, void> > Buffer;

	typedef std::pair<const char8_t*, const char8_t*> StringRange;

	explicit HttpMessage(const Allocator &alloc);

	HttpMessage(const HttpMessage &another);
	HttpMessage& operator=(const HttpMessage &another);

	void clear();
	void build();

	Version& getVersion();

	HeaderList& getHeaderList();
	HeaderField* findHeader(const char8_t *name);
	void addHeader(
			const char8_t *name, const char8_t *value, bool overwriting);
	void removeHeader(const char8_t *name);
	void setStartLine(const char8_t *line);

	bool isContentEncodingCustomized() const;
	void setContentEncodingCustomized(bool customized);

	bool isKeepAlive() const;
	void setKeepAlive(bool keepAlive);

	bool matchContentTypeName(const char8_t *type, bool failOnUnmatch = false);

	bool isJsonType();
	picojson::value toJsonValue();
	void setJsonValue(const picojson::value &value);

	bool acceptChunkedContent(const void *data, size_t size, bool eof);

	const void* getMessageData() const;
	size_t getMessageSize() const;

	const void* getContentData() const;
	size_t getContentSize() const;

	void setMessageRef(const void *data, size_t size);
	void setContentRef(const void *data, size_t size);

	Buffer& prepareMessageBuffer();
	Buffer& prepareContentBuffer();

	bool readFrom(util::File &file, size_t *readSize);
	bool writeTo(util::File &file);

	bool isWrote() const;
	size_t& getWroteSize();

	bool isHttp10OrLess() const;

	Formatter formatter() const;

private:
	static const char8_t CHARS_HTAB[2];
	static const char8_t CHARS_SP[2];
	static const char8_t CHARS_EQ[2];
	static const char8_t CHARS_COMMA[2];
	static const char8_t CHARS_SEMI[2];
	static const char8_t CHARS_DQUOTE[2];
	static const char8_t CHARS_BSLASH[2];

	static const char8_t CHARS_SEPARATOR[];
	static const char8_t CHARS_TOKEN68_SYMBOL[];

	static const std::pair<uint8_t, uint8_t> CHARS_VCHAR;
	static const std::pair<uint8_t, uint8_t> CHARS_OBS;
	static const std::pair<uint8_t, uint8_t> CHARS_UPPER_ALPHA;
	static const std::pair<uint8_t, uint8_t> CHARS_LOWER_ALPHA;
	static const std::pair<uint8_t, uint8_t> CHARS_DIGIT;

	typedef std::pair<const void*, size_t> BufferRef;

	size_t getHeaderSize() const;

	static void appendBuffer(Buffer &buf, const void *data, size_t size);
	static void appendBuffer(Buffer &buf, const String &str);
	static void appendBuffer(Buffer &buf, const char8_t *str);

	Version version_;
	HeaderList headerList_;
	String startLine_;
	bool contentEncodingCustomized_;
	bool keepAlive_;

	BufferRef messageRef_;
	BufferRef contentRef_;

	Buffer messageBuf_;
	Buffer contentBuf_;

	size_t wroteSize_;
};

struct HttpMessage::HeaderField {
	HeaderField(const String &name, const String &value);

	String name_;
	String value_;
};

class HttpMessage::FieldParser {
public:
	FieldParser(const char8_t *begin, const char8_t *end);

	bool nextSeparatedParameter(
			StringRange &name, StringRange &value, String &valueStorage,
			bool optional = false);
	bool nextParameter(
			StringRange &name, StringRange &value, String &valueStorage,
			bool optional = false);
	bool nextQuotedString(
			StringRange &value, String &storage, bool optional = false);

	bool nextToken(StringRange &token, bool optional = false);
	bool nextToken68(StringRange &token, bool optional = false);
	bool nextLiteral(const char8_t *str, bool optional = false);

	bool nextElement(bool optional = false);
	bool nextSpace(
			StringRange *space = NULL, bool optional = false);

	bool checkEnd(bool optional = false);

	const char8_t* getError() const;

	static int32_t compareToken(const char8_t *value1, const char8_t *value2);
	static void normalizeToken(String &value);
	static char8_t normalizeTokenChar(const char8_t ch);

	static bool isDigit(const char8_t ch);

	static bool charMatches(
			const char8_t ch, const std::pair<uint8_t, uint8_t> &range);

private:
	bool matches(const char8_t (&ch)[2]) const;
	bool matches(const char8_t ch) const;
	bool matches(const std::pair<uint8_t, uint8_t> &range) const;

	bool checkResult(bool matched, bool optional, const char8_t *last);

	const StringRange src_;
	const char8_t *cur_;
	const char8_t *error_;
};

class HttpMessage::FieldBuilder {
public:
	explicit FieldBuilder(String &dest);

	void prepareElement();
	void addSeparatedParameter(const char8_t *name, const char8_t *value);
	void addParameter(const char8_t *name, const char8_t *value);
	void addQuotable(const char8_t *value);
	void addString(const char8_t *value);
	void addSpace();

	const String& toString() const;

private:
	String &dest_;
	bool elementPrepared_;
};

class HttpMessage::Formatter {
public:
	explicit Formatter(const HttpMessage &message);

	void setFirstLine(const char8_t *firstLine);
	void setLimit(size_t limit);

	void format(std::ostream &s) const;

private:
	const HttpMessage &message_;
	const char8_t *firstLine_;
	size_t limit_;
};

std::ostream& operator<<(
		std::ostream &s, const HttpMessage::Formatter &formatter);

struct HttpAuth {
public:
	typedef HttpMessage::Allocator Allocator;
	typedef HttpMessage::String String;

	enum Type {
		TYPE_BASIC,
		TYPE_DIGEST,
		MAX_TYPE
	};

	enum Param {
		PARAM_REALM,
		PARAM_QOP,
		PARAM_NONCE,
		PARAM_OPAQUE,
		PARAM_USERNAME,
		PARAM_URI,
		PARAM_ALGORITHM,
		PARAM_NC,
		PARAM_CNONCE,
		PARAM_RESPONSE,
		PARAM_PASSWORD,
		MAX_PARAM
	};

	enum Qop {
		QOP_NONE = 1 << 0,
		QOP_AUTH = 1 << 1,
		QOP_AUTH_INT = 1 << 2,
		MAX_QOP = 1 << 3
	};

	enum Algorithm {
		ALGORITHM_MD5,
		ALGORITHM_MD5_SESS,
		MAX_ALGORITHM
	};

	static const char8_t HEADER_REQUEST[];
	static const char8_t HEADER_RESPONSE[];

	typedef void (HashFunc)(const String&, String*);

	explicit HttpAuth(const Allocator &alloc);

	void initialize(Type type);
	void setHashFunc(HashFunc *hashFunc);

	void make(HttpMessage &message, bool requesting) const;
	String make(
			bool requesting,
			HttpMessage::FieldBuilder *builder = NULL) const;

	bool accept(HttpMessage &message, bool requesting);
	bool accept(const char8_t *value, bool requesting);
	void validate(bool requesting) const;

	Type getType() const;

	void setSecret(
			const char8_t *method, const char8_t *secret,
			const char8_t *bodyHash = NULL);

	int32_t getQopFlags() const;
	void setQopFlags(int32_t value);

	Algorithm getAlgorithm() const;
	void setAlgorithm(Algorithm value);

	const char8_t* getParam(Param param) const;
	const char8_t* findParam(Param param) const;
	void setParam(Param param, const char8_t *value);

	static bool resolveParamName(const char8_t *str, Param *value);
	static bool resolveTypeName(const char8_t *str, Type *value);

	static const char8_t* getHeaderName(bool requesting);
	static const char8_t* getTypeName(Type type, bool failOnUnknown);
	static const char8_t* getParamName(Param param, bool failOnUnknown);
	static const char8_t* getQopName(Qop qop, bool failOnUnknown);
	static const char8_t* getAlgorithmName(
			Algorithm Algorithm, bool failOnUnknown);

private:
	typedef std::pair<bool, String> Value;
	typedef std::vector<Value, util::StdAllocator<Value, void> > ParamList;

	static ParamList newParamList(const Allocator &alloc);

	Type type_;
	ParamList paramList_;
	HashFunc *hashFunc_;
};

class HttpRequest {
public:
	class Parser;
	class Formatter;

	typedef HttpMessage::Allocator Allocator;
	typedef HttpMessage::String String;

	typedef std::vector<
			String, util::StdAllocator<String, void> > PathElements;
	typedef std::map< String, String, std::less<String>, util::StdAllocator<
			std::pair<const String, String>, void> > ParameterMap;

	typedef int32_t Method;

	static const uint16_t DEFAULT_HTTP_PORT;

	static const Method METHOD_GET;
	static const Method METHOD_POST;

	explicit HttpRequest(const Allocator &alloc);

	HttpMessage& getMessage();
	const HttpMessage& getMessage() const;

	void clear();
	HttpMessage* parse(bool eof);
	HttpMessage& build();

	PathElements& getPathElements();
	ParameterMap& getParameterMap();
	String& getFragment();

	bool isDirectory() const;
	void setDirectory(bool directory);

	Method getMethod() const;
	void setMethod(Method method);

	void acceptURL(const char8_t *url);
	void acceptPath(const char8_t *path);

	const char8_t* getScheme() const;
	const char8_t* getHost() const;
	uint16_t getPort() const;

	static const char8_t* methodToString(Method method, bool failOnUnknown);

	Formatter formatter() const;

private:
	void encodeQueryString(std::ostream &os);
	static void encodeURL(std::ostream &os, const char8_t *str);

	HttpMessage message_;

	PathElements pathElements_;
	ParameterMap parameterMap_;
	String fragment_;
	bool directory_;

	String scheme_;
	Method method_;
	String host_;
	uint16_t port_;
};

class HttpRequest::Formatter {
public:
	typedef HttpMessage::Formatter Base;

	explicit Formatter(const HttpRequest &request);
	Base& base();
	void format(std::ostream &s) const;

private:
	Base base_;
};

std::ostream& operator<<(
		std::ostream &s, const HttpRequest::Formatter &formatter);

class HttpResponse {
public:
	class Formatter;

	typedef HttpMessage::Allocator Allocator;
	typedef HttpMessage::String String;

	typedef int32_t Status;

	explicit HttpResponse(const Allocator &alloc);

	HttpMessage& getMessage();
	const HttpMessage& getMessage() const;

	void clear();
	HttpMessage* parse(bool eof);
	HttpMessage& build();

	Status getStatus() const;
	void setStatus(Status status);

	const char8_t* getFirstLine() const;

	bool isSuccess() const;
	void checkSuccess() const;

	bool isRedirection() const;
	const char8_t* getLocation();

	void setBadRequestError();
	void setAuthError();
	void setMethodError();
	void setInternalServerError();

	Formatter formatter() const;

private:
	enum {
		HTTP_STATUS_SUCCESS = 200,
		HTTP_STATUS_REDIRECTION = 300
	};

	Allocator alloc_;
	HttpMessage message_;

	Status status_;
	String firstLine_;
};

class HttpResponse::Formatter {
public:
	typedef HttpMessage::Formatter Base;

	explicit Formatter(const HttpResponse &response);
	Base& base();
	void format(std::ostream &s) const;

private:
	Base base_;
};

std::ostream& operator<<(
		std::ostream &s, const HttpResponse::Formatter &formatter);

#endif
