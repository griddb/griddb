/*
	Copyright (c) 2014 TOSHIBA CORPORATION.

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
	@brief Implementation of Exception and Trace
*/

#include "gs_error.h"
#include "util/container.h"

GSExceptionRegenerator::GSExceptionRegenerator(
	UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST) throw()
	: fileNameLiteral_(fileNameLiteral),
	  functionNameLiteral_(functionNameLiteral),
	  critical_(false) {
	util::Exception cause;
	util::Exception::DuplicatedLiteralFlags filteredFlags;
	const util::Exception::DuplicatedLiteralFlags baseFlags =
		util::Exception::LITERAL_ALL_DUPLICATED;
	NamedErrorCode filteredCode;
	const char8_t *extraMessage = NULL;
	if (causeInHandling != NULL) {
		NamedErrorCode causeCode;
		bool userOrSystemThrown = false;
		bool unexpectedType = false;
		try {
			throw;
		}
		catch (const UserException &e) {
			cause = e;
			userOrSystemThrown = true;
			causeCode = selectErrorCode(e.getNamedErrorCode(),
				GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR));
			filteredFlags = e.inheritLiteralFlags(baseFlags) | literalFlags;
		}
		catch (const SystemException &e) {
			cause = e;
			critical_ = true;
			userOrSystemThrown = true;
			causeCode = selectErrorCode(e.getNamedErrorCode(),
				GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR));
			filteredFlags = e.inheritLiteralFlags(baseFlags) | literalFlags;
		}
		catch (const util::PlatformException &e) {
			cause = e;
			causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_PLATFORM_ERROR);
			filteredFlags = e.inheritLiteralFlags(baseFlags) | literalFlags;
		}
		catch (const util::UtilityException &e) {
			cause = e;
			switch (e.getErrorCode()) {
			case util::UtilityException::CODE_NO_MEMORY:
				causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_NO_MEMORY);
				break;
			case util::UtilityException::CODE_MEMORY_LIMIT_EXCEEDED:
				causeCode =
					GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_MEMORY_LIMIT_EXCEEDED);
				break;
			case util::UtilityException::CODE_SIZE_LIMIT_EXCEEDED:
				causeCode =
					GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_SIZE_LIMIT_EXCEEDED);
				break;
			default:
				causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR);
				extraMessage = "Internal error by unexpected utility problem";
				break;
			}
			filteredFlags = literalFlags;
		}
		catch (const util::Exception &e) {
			cause = e;
			if (e.getErrorCode() == 0) {
				critical_ = true;
				unexpectedType = true;
				causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR);
				filteredFlags = literalFlags;
			}
			else {
				causeCode = e.getNamedErrorCode();
				filteredFlags = e.inheritLiteralFlags(baseFlags) | literalFlags;
			}
		}
		catch (const std::bad_alloc &) {
			causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_NO_MEMORY);
			filteredFlags = literalFlags;
		}
		catch (...) {
			critical_ = true;
			unexpectedType = true;
			causeCode = GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR);
			filteredFlags = literalFlags;
		}
		filteredCode = selectErrorCode(namedErrorCode, causeCode);

		if (userOrSystemThrown &&
			causeCode.getCode() == GS_ERROR_CM_INTERNAL_ERROR &&
			cause.getErrorCode() == GS_ERROR_DEFAULT) {
			extraMessage = "Internal error by empty error code";
		}
		else if (unexpectedType) {
			extraMessage = "Internal error by unexpected exception type";
		}
	}
	else {
		filteredCode = namedErrorCode;
		filteredFlags = literalFlags;
	}
	filteredCode = selectErrorCode(
		filteredCode, GS_EXCEPTION_NAMED_CODE(GS_ERROR_CM_INTERNAL_ERROR));

	typedef util::NormalOStringStream StrStream;
	typedef util::Exception::NoThrowString<StrStream::allocator_type> SafeStr;

	StrStream baseStream;
	baseStream << cause.getField(util::Exception::FIELD_MESSAGE);
	SafeStr baseStr(baseStream);
	const char8_t *strPtr = baseStr.get();
	const char8_t *baseMessage =
		(message != NULL && strlen(message) > 0)
			? message
			: (strPtr == NULL || strlen(strPtr) > 0 ? strPtr : NULL);

	StrStream messageStream;
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

	base_ = util::Exception(filteredCode, SafeStr(messageStream).get(), NULL,
		NULL, lineNumber, causeInHandling, typeNameLiteral, stackTraceMode,
		filteredFlags);
}

void GSExceptionRegenerator::generate(
	util::Exception &dest, const char8_t *typeName) const throw() {
	util::Exception cause;
	try {
		cause.assign(base_, 1);
		throw cause;
	}
	catch (...) {
		dest = util::Exception(base_.getNamedErrorCode(),
			UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(base_.getField(
				util::Exception::FIELD_MESSAGE)),
			fileNameLiteral_, functionNameLiteral_, base_.getLineNumber(),
			&cause, typeName, util::Exception::STACK_TRACE_NONE,
			base_.inheritLiteralFlags());
	}
}

GSExceptionRegenerator::NamedErrorCode
GSExceptionRegenerator::getNamedErrorCode() const throw() {
	return base_.getNamedErrorCode();
}

/*!
	@brief Formats data by field type
*/
void GSExceptionRegenerator::formatField(
	std::ostream &stream, util::Exception::FieldType fieldType) const throw() {
	base_.formatField(stream, fieldType);
}

bool GSExceptionRegenerator::getCause(util::Exception &dest) const throw() {
	if (base_.getMaxDepth() > 0) {
		dest.assign(base_, 1);
		return true;
	}

	return false;
}

const GSExceptionRegenerator::NamedErrorCode &
GSExceptionRegenerator::selectErrorCode(
	const NamedErrorCode &primary, const NamedErrorCode &secondary) throw() {
	return (primary.isEmpty() ? secondary : primary);
}

GSTraceFormatter::GSTraceFormatter(const char8_t *secretHexKey)
	: secret_(true), traceLocationVisible_(false) {
	util::NormalIStringStream iss(secretHexKey);
	util::NormalOStringStream oss;
	util::HexConverter::decode(oss, iss);

	std::string keyStr = oss.str();
	secretKey_.reserve(keyStr.size());

	for (std::string::iterator it = keyStr.begin(); it != keyStr.end(); ++it) {
		secretKey_.push_back(static_cast<uint8_t>(*it));
	}

	if (secretKey_.empty()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Empty secret key");
	}
}

GSTraceFormatter::~GSTraceFormatter() {}

void GSTraceFormatter::setSecret(bool secret) {
	secret_ = secret;
}

void GSTraceFormatter::setTraceLocationVisible(bool visible) {
	traceLocationVisible_ = visible;
}

/*!
	@brief Formats error message
*/
void GSTraceFormatter::format(std::ostream &stream, util::TraceRecord &record) {
	util::Exception cause;
	GSExceptionRegenerator regenerator(record.namedErrorCode_, record.message_,
		NULL, NULL, 0, record.causeInHandling_, NULL,
		util::Exception::STACK_TRACE_NONE);

	if (regenerator.getCause(cause)) {
		record.cause_ = &cause;

		if (record.namedErrorCode_.isEmpty()) {
			record.namedErrorCode_ = regenerator.getNamedErrorCode();
		}
	}

	u8string message;
	try {
		util::NormalOStringStream oss;
		regenerator.formatField(oss, util::Exception::FIELD_MESSAGE);
		message = oss.str();
		record.message_ = message.c_str();
	}
	catch (std::bad_alloc &) {
		record.message_ =
			"(Failed to format error message by not enough memory)";
	}
	catch (...) {
		record.message_ = "(Failed to format error message by unknown error)";
	}

	Base::format(stream, record);
}

void GSTraceFormatter::formatFiltered(
	std::ostream &stream, const util::TraceRecord &record) {
	const bool codeAndSymbolCombined = true;

	stream << record.dateTime_ << " ";

	if (record.hostName_ != NULL) {
		stream << record.hostName_ << " ";
	}

	stream << util::Thread::getSelfId() << " ";

	formatLevel(stream, record);

	if (!codeAndSymbolCombined) {
		formatMainErrorCode(stream, record, false, " ");
	}

	if (record.tracerName_ != NULL) {
		stream << " " << record.tracerName_;
	}

	if (codeAndSymbolCombined) {
		if (formatMainErrorCode(stream, record, false, " [")) {
			formatMainErrorCode(stream, record, true, ":");
			stream << "]";
		}
	}
	else {
		formatMainErrorCode(stream, record, true, " ");
	}

	if (record.message_ != NULL && *record.message_ != '\0') {
		stream << (codeAndSymbolCombined ? " " : " : ") << record.message_;
	}

	if (record.cause_ != NULL) {
		stream << " <";
		if (secret_) {
			util::NormalOStringStream oss;
			formatMainLocation(oss, record, "");
			formatCause(oss, record, " ");
			util::NormalIStringStream iss(obfuscate(oss.str()));

			util::Base64Converter::encode(stream, iss);
		}
		else {
			formatMainLocation(stream, record, "");
			formatCause(stream, record, " ");
		}
		stream << ">";
	}
}

u8string GSTraceFormatter::obfuscate(const u8string &src) {
	assert(!secretKey_.empty());

	u8string dest = src;
	for (u8string::iterator it = dest.begin(); it != dest.end(); ++it) {
		*it ^= static_cast<char8_t>(
			secretKey_[(it - dest.begin()) % secretKey_.size()]);
	}

	return dest;
}
