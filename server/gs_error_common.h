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
	@brief Definitions of common errors
*/
#ifndef GS_ERROR_COMMON_H_
#define GS_ERROR_COMMON_H_

#ifndef GS_ERROR_COMMON_ONLY
#define GS_ERROR_COMMON_ONLY 0
#endif

#if GS_ERROR_COMMON_ONLY
#include "util/code.h"
#else
#include "gs_error.h"
#endif

enum GSCommonErrorCode {
	GS_ERROR_JSON_INVALID_SYNTAX = 121000,
	GS_ERROR_JSON_KEY_NOT_FOUND,
	GS_ERROR_JSON_VALUE_OUT_OF_RANGE,
	GS_ERROR_JSON_UNEXPECTED_TYPE,

	GS_ERROR_HTTP_INTERNAL_ILLEGAL_OPERATION = 122000,
	GS_ERROR_HTTP_INTERNAL_ILLEGAL_PARAMETER,
	GS_ERROR_HTTP_UNEXPECTED_MESSAGE,
	GS_ERROR_HTTP_INVALID_MESSAGE,

	GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION = 123000,
	GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER,
	GS_ERROR_SA_INVALID_CONFIG,
	GS_ERROR_SA_ADDRESS_CONFLICTED,
	GS_ERROR_SA_ADDRESS_NOT_ASSIGNED,
	GS_ERROR_SA_INVALID_ADDRESS,

	GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION = 124000,
	GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE,
	GS_ERROR_AUTH_INVALID_CREDENTIALS,

	GS_ERROR_SSL_INTERNAL_ILLEGAL_OPERATION = 125000,
	GS_ERROR_SSL_ILLEGAL_CONFIG,
	GS_ERROR_SSL_OPERATION_FAILED
};

class GSCommonException : public util::Exception {
public:
	explicit GSCommonException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
			Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~GSCommonException() throw() {}
};

class GSCommonExceptionRegenerator {
public:
	typedef util::Exception::NamedErrorCode NamedErrorCode;

	enum CauseType {
		CAUSE_NONE,
		CAUSE_CUSTOM,
		CAUSE_PLATFORM,
		CAUSE_UTILITY,
		CAUSE_GENERAL,
		CAUSE_OTHER
	};

	struct CustomFilter {
		static void filterCause(
				CauseType causeType, const NamedErrorCode *utilCode,
				NamedErrorCode &causeCode, bool &codeAsLiteral,
				const char8_t *&extraMessage);
	};

	template <typename V>
	GSCommonExceptionRegenerator(
			const V&, const util::Exception *customCause,
			UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw();

	template <typename T>
	T generate(const char8_t *typeName) const throw();

	template <typename V>
	void generate(
			const V&, util::Exception &dest,
			const char8_t *typeName) const throw();

private:
	typedef util::Exception::DuplicatedLiteralFlags DuplicatedLiteralFlags;
	typedef util::NormalOStringStream StrStream;
	typedef util::Exception::NoThrowString<StrStream::allocator_type> SafeStr;

	GSCommonExceptionRegenerator(const GSCommonExceptionRegenerator&);

	GSCommonExceptionRegenerator& operator=(
			const GSCommonExceptionRegenerator&);

	template <typename V>
	static util::Exception resolveCause(
			const V&, const std::exception *causeInHandling,
			const util::Exception *customCause,
			const char8_t *&extraMessage,
			NamedErrorCode &filteredCode,
			DuplicatedLiteralFlags &filteredLiteralFlags) throw();

	template <typename V>
	static void filterExtraMessage(
			const V&, CauseType causeType, const NamedErrorCode *utilCode,
			const NamedErrorCode &causeCode,
			const char8_t *&extraMessage) throw();

	template <typename V>
	static StrStream& generateMessage(
			const V&, const util::Exception &cause,
			const char8_t *extraMessage,
			const char8_t *message, StrStream &messageStream) throw();

	const char8_t *fileNameLiteral_;
	const char8_t *functionNameLiteral_;
	int32_t lineNumber_;
	const char8_t *extraMessage_;

	NamedErrorCode filteredCode_;
	DuplicatedLiteralFlags filteredLiteralFlags_;
	util::Exception cause_;

	StrStream messageStream_;
	SafeStr messageStr_;

	util::Exception base_;
};

class GSDefaultCommonExceptionRegenerator {
public:
	explicit GSDefaultCommonExceptionRegenerator(
			UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
			base_(int(), NULL, UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {
	}

	template <typename T>
	T generate(const char8_t *typeName) const throw() {
		return base_.generate<T>(typeName);
	}

private:
	GSCommonExceptionRegenerator base_;
};

class GSExceptionCoder {
public:
	GSExceptionCoder() {
	}

	template<typename E, typename Str>
	void decode(
			util::ArrayByteInStream &in, E &dest, Str &strBuf,
			const util::Exception::NamedErrorCode &defaultNamedCode) const;

private:
	struct EmptyParameterBuilder {
		void appendParameter(const char8_t *name, const char8_t *value) {
			static_cast<void>(name);
			static_cast<void>(value);
		}
	};

	GSExceptionCoder(const GSExceptionCoder&);
	GSExceptionCoder& operator=(const GSExceptionCoder&);

	template<typename Str, typename ParamBuilder>
	void decodeInternal(
			util::ArrayByteInStream &in, util::Exception &dest,
			Str &strBuf, ParamBuilder &paramBuilder,
			const util::Exception::NamedErrorCode &defaultNamedCode) const;

	template<typename E>
	static typename E::ParametersBuilder& getParametersBuilder(
			E &dest, typename E::ParametersBuilder* = NULL) {
		return dest;
	}

	template<typename E>
	static EmptyParameterBuilder& getParametersBuilder(...) {
		static EmptyParameterBuilder builder;
		return builder;
	}
};




template <typename V>
GSCommonExceptionRegenerator::GSCommonExceptionRegenerator(
		const V&, const util::Exception *customCause,
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST) throw() :
		fileNameLiteral_(fileNameLiteral),
		functionNameLiteral_(functionNameLiteral),
		lineNumber_(lineNumber),
		extraMessage_(NULL),
		filteredCode_(namedErrorCode),
		filteredLiteralFlags_(literalFlags),
		cause_(resolveCause(
				V(), causeInHandling, customCause, extraMessage_,
				filteredCode_, filteredLiteralFlags_)),
		messageStr_(generateMessage(
				V(), cause_, extraMessage_, message, messageStream_)) {
	static_cast<void>(typeNameLiteral);
	static_cast<void>(stackTraceMode);
}

template <typename T>
T GSCommonExceptionRegenerator::generate(
		const char8_t *typeName) const throw() {
	T exception;
	generate(int(), exception, typeName);

	return exception;
}

template <typename V>
void GSCommonExceptionRegenerator::generate(
		const V&, util::Exception &dest,
		const char8_t *typeName) const throw() {
	util::Exception cause;
	try {
		cause.assign(cause_, 1);
		throw cause;
	}
	catch (...) {
		dest = util::Exception(
				cause_.getNamedErrorCode(), messageStr_.get(),
				fileNameLiteral_, functionNameLiteral_, lineNumber_,
				&cause, typeName, util::Exception::STACK_TRACE_NONE,
				filteredLiteralFlags_);
	}
}

template <typename V>
util::Exception GSCommonExceptionRegenerator::resolveCause(
		const V&, const std::exception *causeInHandling,
		const util::Exception *customCause,
		const char8_t *&extraMessage,
		NamedErrorCode &filteredCode,
		DuplicatedLiteralFlags &filteredLiteralFlags) throw() {
	util::Exception baseCause;

	CauseType causeType;
	NamedErrorCode utilCode;
	if (causeInHandling == NULL) {
		causeType = CAUSE_NONE;
	}
	else if (customCause != NULL) {
		baseCause = *customCause;
		causeType = CAUSE_CUSTOM;
	}
	else {
		try {
			throw;
		}
		catch (const util::PlatformException &e) {
			baseCause = e;
			causeType = CAUSE_PLATFORM;
		}
		catch (const util::UtilityException &e) {
			baseCause = e;
			causeType = CAUSE_UTILITY;
			utilCode = e.getNamedErrorCode();
		}
		catch (const util::Exception &e) {
			baseCause = e;
			causeType = CAUSE_GENERAL;
		}
		catch (...) {
			causeType = CAUSE_OTHER;
			try {
				UTIL_RETHROW_UTIL_ERROR(CODE_DEFAULT, *causeInHandling, "");
			}
			catch (const util::UtilityException &e2) {
				utilCode = e2.getNamedErrorCode();
			}
			catch (...) {
			}
		}
	}

	const uint32_t codeFlag = (1U << util::Exception::FIELD_ERROR_CODE_NAME);
	bool codeAsLiteral = ((filteredLiteralFlags & codeFlag) == 0);
	if (filteredCode.isEmpty()) {
		filteredCode = baseCause.getNamedErrorCode();
		codeAsLiteral = ((baseCause.inheritLiteralFlags(codeFlag) & codeFlag) == 0);
	}

	const NamedErrorCode *utilCodeRef =
			(utilCode.isEmpty() ? NULL : &utilCode);

	filterExtraMessage(
			V(), causeType, utilCodeRef, filteredCode, extraMessage);
	CustomFilter::filterCause(
			causeType, utilCodeRef, filteredCode, codeAsLiteral, extraMessage);

	filteredLiteralFlags = static_cast<DuplicatedLiteralFlags>(
			(filteredLiteralFlags & ~codeFlag) |
			(codeAsLiteral ? 0U : codeFlag));

	util::Exception cause(
			filteredCode, NULL, NULL, NULL, 0, NULL, NULL,
			util::Exception::STACK_TRACE_NONE, filteredLiteralFlags);
	cause.append(baseCause);
	return cause;
}

template <typename V>
void GSCommonExceptionRegenerator::filterExtraMessage(
		const V&, CauseType causeType, const NamedErrorCode *utilCode,
		const NamedErrorCode &causeCode,
		const char8_t *&extraMessage) throw() {
	if (extraMessage != NULL) {
		return;
	}

	bool unexpected = false;
	if (utilCode == NULL) {
		if (causeCode.isEmpty()) {
			if (causeType == CAUSE_OTHER) {
				unexpected = true;
			}
			else if (causeType != CAUSE_PLATFORM) {
				extraMessage = "Internal error by empty error code";
			}
		}
	}
	else {
		switch (utilCode->getCode()) {
		case util::UtilityException::CODE_NO_MEMORY:
			break;
		case util::UtilityException::CODE_MEMORY_LIMIT_EXCEEDED:
			break;
		case util::UtilityException::CODE_SIZE_LIMIT_EXCEEDED:
			break;
		default:
			if (causeType == CAUSE_OTHER) {
				unexpected = true;
			}
			else {
				extraMessage = "Internal error by unexpected utility problem";
			}
			break;
		}
	}
	if (unexpected) {
		extraMessage = "Internal error by unexpected exception type";
	}
}

template <typename V>
GSCommonExceptionRegenerator::StrStream&
GSCommonExceptionRegenerator::generateMessage(
		const V&, const util::Exception &cause, const char8_t *extraMessage,
		const char8_t *message, StrStream &messageStream) throw() {

	StrStream baseStream;
	baseStream << cause.getField(util::Exception::FIELD_MESSAGE, 1);

	SafeStr baseStr(baseStream);
	const char8_t *strPtr = baseStr.get();
	const char8_t *baseMessage = (message != NULL && strlen(message) > 0) ?
			message : (strPtr != NULL && strlen(strPtr) > 0 ? strPtr : NULL);

	if (extraMessage == NULL) {
		if (baseMessage != NULL) {
			messageStream << baseMessage;
		}
	}
	else {
		messageStream << extraMessage;
		if (baseMessage != NULL) {
			messageStream << " (reason=" << baseMessage << ")";
		}
	}

	return messageStream;
}


template<typename E, typename Str>
void GSExceptionCoder::decode(
		util::ArrayByteInStream &in, E &dest, Str &strBuf,
		const util::Exception::NamedErrorCode &defaultNamedCode) const {
	decodeInternal(
			in, dest, strBuf, getParametersBuilder(dest), defaultNamedCode);
}

template<typename Str, typename ParamBuilder>
void GSExceptionCoder::decodeInternal(
		util::ArrayByteInStream &in, util::Exception &dest,
		Str &strBuf, ParamBuilder &paramBuilder,
		const util::Exception::NamedErrorCode &defaultNamedCode) const {
	strBuf.clear();
	dest = util::Exception();

	util::Exception top;
	util::Exception following;

	int32_t count;
	in >> count;

	for (int32_t i = 0; i < count; i++) {
		int32_t errorCode;
		Str optionMessage(strBuf);
		Str type(strBuf);
		Str fileName(strBuf);
		Str functionName(strBuf);
		int32_t lineNumber;

		in >> errorCode;
		in >> optionMessage;
		in >> type;
		in >> fileName;
		in >> functionName;
		in >> lineNumber;

		(i == 0 ? top : following).append(util::Exception(
				util::Exception::NamedErrorCode(errorCode),
				optionMessage.c_str(),
				fileName.c_str(),
				functionName.c_str(),
				lineNumber,
				NULL,
				type.c_str(),
				util::Exception::STACK_TRACE_NONE,
				util::Exception::LITERAL_ALL_DUPLICATED));
	}

	if (in.base().remaining() > 0 || !defaultNamedCode.isEmpty()) {
		Str errorCodeName(strBuf);

		if (in.base().remaining() > 0) {
			in >> errorCodeName;
		}

		util::Exception::NamedErrorCode topCode(top.getErrorCode());
		if (topCode.isEmpty()) {
			topCode = defaultNamedCode;
		}
		else {
			topCode = util::Exception::NamedErrorCode(
					top.getErrorCode(), errorCodeName.c_str());
		}

		dest.append(util::Exception(
				topCode,
				UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						top.getField(util::Exception::FIELD_MESSAGE)),
				UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						top.getField(util::Exception::FIELD_FILE_NAME)),
				UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						top.getField(util::Exception::FIELD_FUNCTION_NAME)),
				top.getLineNumber(),
				NULL,
				UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						top.getField(util::Exception::FIELD_TYPE_NAME)),
				util::Exception::STACK_TRACE_NONE,
				util::Exception::LITERAL_ALL_DUPLICATED));
	}
	else {
		dest.append(top);
	}

	if (count > 1) {
		dest.append(following);
	}

	if (in.base().remaining() > 0) {
		int32_t paramCount;
		in >> paramCount;

		for (int32_t i = 0; i < paramCount; i++) {
			Str name(strBuf);
			Str value(strBuf);

			in >> name;
			in >> value;

			paramBuilder.appendParameter(name.c_str(), value.c_str());
		}
	}
}

#define GS_COMMON_EXCEPTION_NAMED_CODE(codeSymbol) \
		util::Exception::makeNamedErrorCode(codeSymbol, #codeSymbol, "GS_ERROR_")
#define GS_COMMON_EXCEPTION_CREATE_DETAIL(type, errorCode, cause, message) \
		UTIL_EXCEPTION_CREATE_DETAIL( \
				type, GS_COMMON_EXCEPTION_NAMED_CODE(errorCode), cause, message)

#define GS_COMMON_EXCEPTION_CONVERT_CUSTOM(type, defaultCode, cause, message) \
		GS_COMMON_EXCEPTION_CREATE_DETAIL( \
				GSDefaultCommonExceptionRegenerator, \
				defaultCode, &(cause), message).generate<type>(#type)
#define GS_COMMON_EXCEPTION_CONVERT_CODED(defaultCode, cause, message) \
		GS_COMMON_EXCEPTION_CONVERT_CUSTOM( \
				util::Exception, defaultCode, cause, message)
#define GS_COMMON_EXCEPTION_CONVERT(cause, message) \
		GS_COMMON_EXCEPTION_CONVERT_CODED(0, cause, message)

#define GS_COMMON_THROW_CUSTOM_ERROR(type, errorCode, message) \
		throw GS_COMMON_EXCEPTION_CREATE_DETAIL(type, errorCode, NULL, message)
#define GS_COMMON_RETHROW_CUSTOM_ERROR(type, defaultCode, cause, message) \
		throw GS_COMMON_EXCEPTION_CONVERT_CUSTOM( \
				type, defaultCode, cause, message)

#if GS_ERROR_COMMON_ONLY
#define GS_COMMON_THROW_USER_ERROR(errorCode, message) \
		GS_COMMON_THROW_CUSTOM_ERROR(GSCommonException, errorCode, message)
#define GS_COMMON_RETHROW_USER_ERROR(cause, message) \
		GS_COMMON_RETHROW_CUSTOM_ERROR(GSCommonException, 0, cause, message)
#define GS_COMMON_EXCEPTION_MESSAGE(cause) \
		GS_COMMON_EXCEPTION_CONVERT( \
				cause, "").getField(util::Exception::FIELD_MESSAGE)
#else
#define GS_COMMON_THROW_USER_ERROR(errorCode, message) \
		GS_THROW_USER_ERROR(errorCode, message)
#define GS_COMMON_RETHROW_USER_ERROR(cause, message) \
		GS_RETHROW_USER_ERROR(cause, message)
#define GS_COMMON_EXCEPTION_MESSAGE(cause) \
		GS_EXCEPTION_MESSAGE(cause)
#endif 

#if GS_ERROR_COMMON_ONLY
#define GS_COMMON_LIBRARY_EXCEPTION_CONVERT(cause, dest, message) \
		util::LibraryTool::toLibraryException( \
				GS_COMMON_EXCEPTION_CONVERT(cause, message), dest)
#define GS_COMMON_CHECK_LIBRARY_ERROR(call, providerSrc, cause, message) \
		do { \
			try { \
				const int32_t code = (call); \
				if (util::LibraryTool::findError(code, cause)) { \
					throw util::LibraryTool::fromLibraryException< \
							GSCommonException>( \
							code, providerSrc.findExceptionProvider(), cause); \
				} \
			} \
			catch (...) { \
				std::exception e; \
				GS_COMMON_RETHROW_USER_ERROR(e, message); \
			} \
		} \
		while (false)
#else
#define GS_COMMON_LIBRARY_EXCEPTION_CONVERT(cause, dest, message) \
		GS_LIBRARY_EXCEPTION_CONVERT(cause, dest, message)
#define GS_COMMON_CHECK_LIBRARY_ERROR(code, providerSrc, cause, message) \
		GS_CHECK_LIBRARY_ERROR(code, providerSrc, cause, message)
#endif 

#endif
