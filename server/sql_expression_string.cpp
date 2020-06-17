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

#include "sql_expression_string.h"
#include "sql_utils_vdbe.h"
#include "query_function_string.h"


const SQLExprs::ExprRegistrar
SQLStringExprs::Registrar::REGISTRAR_INSTANCE((Registrar()));

void SQLStringExprs::Registrar::operator()() const {
	add<SQLType::FUNC_CHAR, Functions::Char>();
	add<SQLType::FUNC_GLOB, Functions::Glob>();
	add<SQLType::FUNC_HEX, Functions::Hex>();
	add<SQLType::FUNC_INSTR, Functions::Instr>();
	add<SQLType::FUNC_LENGTH, Functions::Length>();
	add<SQLType::FUNC_LIKE, Functions::Like>();
	add<SQLType::FUNC_LOWER, Functions::Lower>();
	add<SQLType::FUNC_LTRIM, Functions::Ltrim>();
	add<SQLType::FUNC_PRINTF, Functions::Printf>();
	add<SQLType::FUNC_QUOTE, Functions::Quote>();
	add<SQLType::FUNC_RANDOMBLOB, Functions::RandomBlob>();
	add<SQLType::FUNC_REPLACE, Functions::Replace>();
	add<SQLType::FUNC_RTRIM, Functions::Rtrim>();
	add<SQLType::FUNC_SUBSTR, Functions::SubstrWithoutBounds>();
	add<SQLType::FUNC_SUBSTR_WITH_BOUNDS, Functions::SubstrWithBounds>();
	add<SQLType::FUNC_TRANSLATE, StringFunctions::Translate>();
	add<SQLType::FUNC_TRIM, Functions::Trim>();
	add<SQLType::FUNC_UNICODE, Functions::Unicode>();
	add<SQLType::FUNC_UPPER, Functions::Upper>();
	add<SQLType::FUNC_ZEROBLOB, Functions::ZeroBlob>();
}


template<typename C>
inline typename C::WriterType& SQLStringExprs::Functions::Char::operator()(
		C &cxt) {
	return cxt.getResultWriter();
}

template<typename C>
inline typename C::WriterType& SQLStringExprs::Functions::Char::operator()(
		C &cxt, const int64_t *value) {
	typename C::WriterType &writer = cxt.getResultWriter();

	if (value == NULL || *value == 0) {
		cxt.finishFunction();
		return writer;
	}

	const uint64_t codeValue = static_cast<uint64_t>(*value);
	writer.appendCode(util::UTF8Utils::isIllegal(codeValue) ?
			static_cast<util::CodePoint>(
					util::UTF8Utils::ILLEGAL_CHARACTER) :
			static_cast<util::CodePoint>(codeValue));
	return writer;
}


template<typename C, typename R>
inline bool SQLStringExprs::Functions::Glob::operator()(
		C &cxt, R &value1, R &value2) {
	String str1(cxt.getAllocator());
	SQLValues::ValueUtils::partToString(str1, value1);

	String str2(cxt.getAllocator());
	SQLValues::ValueUtils::partToString(str2, value2);

	return SQLVdbeUtils::VdbeUtils::matchGlob(str2.c_str(), str1.c_str());
}


template<typename C, typename R>
inline typename C::WriterType&
SQLStringExprs::Functions::Hex::operator()(C &cxt, R *value) {
	typename C::WriterType &writer = cxt.getResultWriter();
	if (value != NULL) {
		SQLValues::ValueUtils::partToHexString(writer, *value);
	}
	return writer;
}


template<typename C, typename R>
int64_t SQLStringExprs::Functions::Instr::operator()(
		C &cxt, R &target, R &key, int64_t offset, int64_t repeat) {
	static_cast<void>(cxt);
	const int64_t foundPos =
			SQLValues::ValueUtils::findPartRepeatable(target, key, offset, repeat);
	if (foundPos <= 0) {
		return 0;
	}

	target.toBegin();
	return SQLValues::ValueUtils::codeLengthLimited(target, foundPos - 1) + 1;
}


template<typename C, typename R>
int64_t SQLStringExprs::Functions::Length::operator()(C &cxt, R &value) {
	static_cast<void>(cxt);
	return SQLValues::ValueUtils::codeLength(value);
}


template<typename C, typename R>
bool SQLStringExprs::Functions::Like::operator()(
		C &cxt, R &reader1, R &reader2) {
	static_cast<void>(cxt);
	return SQLValues::ValueUtils::matchPartLike<R, false>(reader2, reader1, NULL);
}

template<typename C, typename R>
bool SQLStringExprs::Functions::Like::operator()(
		C &cxt, R &reader1, R &reader2, R &reader3) {
	static_cast<void>(cxt);
	return SQLValues::ValueUtils::matchPartLike<R, true>(reader2, reader1, &reader3);
}


template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Lower::operator()(
		C &cxt, R &reader) {
	typename C::WriterType &writer = cxt.getResultWriter();

	for (util::CodePoint c; reader.nextCode(&c);) {
		writer.appendCode(SQLValues::ValueUtils::toLower(c));
	}

	return writer;
}


template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Ltrim::operator()(
		C &cxt, R &reader1) {
	SQLValues::StringReader reader2(SQLValues::ValueUtils::toStringBuffer(" "));

	return (*this)(cxt, reader1, reader2);
}

template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Ltrim::operator()(
		C &cxt, R &reader1, R &reader2) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const int64_t start = SQLValues::ValueUtils::findLtrimPart(reader1, reader2);
	reader1.toBegin();
	SQLValues::ValueUtils::subPart(
			writer, reader1, start, std::numeric_limits<int64_t>::max(),
			false, util::FalseType());

	return writer;
}


inline SQLStringExprs::Functions::Printf::Printf() :
		format_(NULL),
		argList_(NULL) {
}

inline SQLStringExprs::Functions::Printf::~Printf() {
	if (format_ != NULL) {
		util::StdAllocator<String, void> alloc = alloc_;
		alloc.destroy(format_);
		alloc.deallocate(format_, 1);
	}
	if (argList_ != NULL) {
		util::StdAllocator<ArgList, void> alloc = alloc_;
		alloc.destroy(argList_);
		alloc.deallocate(argList_, 1);
	}
}

template<typename C>
inline typename C::WriterType*
SQLStringExprs::Functions::Printf::operator()(C &cxt) {
	typename C::WriterType &writer = cxt.getResultWriter();

	assert(format_ != NULL);

	size_t argCount;
	const TupleValue *args;
	if (argList_ == NULL) {
		argCount = 0;
		args = NULL;
	}
	else {
		argCount = argList_->size();
		args = &(*argList_)[0];
	}

	SQLValues::ValueUtils::formatValue(
			cxt.getBase(), writer, SQLVdbeUtils::VdbeUtils::sqlPrintf(
					cxt.getBase(), format_->c_str(), args, argCount));

	return &writer;
}

template<typename C, typename R>
inline typename C::WriterType*
SQLStringExprs::Functions::Printf::operator()(C &cxt, R *format) {
	if (format == NULL) {
		cxt.finishFunction();
	}
	else {
		alloc_ = cxt.getAllocator();
		util::StdAllocator<String, void> alloc = alloc_;
		format_ = new (alloc.allocate(1)) String(alloc);
		SQLValues::ValueUtils::partToString(*format_, *format);
	}

	return NULL;
}

template<typename C>
inline typename C::WriterType*
SQLStringExprs::Functions::Printf::operator()(
		C &cxt, const TupleValue &argValue) {
	if (argList_ == NULL) {
		alloc_ = cxt.getAllocator();
		util::StdAllocator<ArgList, void> alloc = alloc_;
		argList_ = new (alloc.allocate(1)) ArgList(alloc);
	}

	argList_->push_back(argValue);
	return NULL;
}


template<typename C>
inline typename C::WriterType& SQLStringExprs::Functions::Quote::operator()(
		C &cxt, const TupleValue &value) {
	typename C::WriterType &writer = cxt.getResultWriter();

	if (value.getType() == TupleTypes::TYPE_BLOB) {
		writer.appendAll("X'");

		SQLValues::LobReader reader(value);
		SQLValues::ValueUtils::partToHexString(writer, reader);

		writer.appendAll("'");
	}
	else if (value.getType() == TupleTypes::TYPE_STRING) {
		writer.appendAll("'");

		const TupleString &str =
				SQLValues::ValueUtils::toString(cxt.getBase(), value);
		SQLValues::StringReader reader(str.getBuffer());
		size_t nonQuote = 0;
		for (; reader.hasNext(); reader.next()) {
			if (reader.get() == '\'') {
				if (nonQuote > 0) {
					writer.append(&reader.get() - nonQuote, nonQuote);
				}
				const char8_t quoted[] = "''";
				writer.appendAll(quoted);
				nonQuote = 0;
			}
			else {
				nonQuote++;
			}
		}

		if (nonQuote > 0) {
			reader.back(nonQuote);
			writer.append(&reader.get(), nonQuote);
		}

		writer.appendAll("'");
	}
	else if (SQLValues::ValueUtils::isNull(value)) {
		writer.appendAll("NULL");
	}
	else {
		SQLValues::ValueUtils::formatValue(cxt.getBase(), writer, value);
	}

	return writer;
}


template<typename C>
inline typename C::WriterType&
SQLStringExprs::Functions::RandomBlob::operator()(
		C &cxt, const int64_t *value) {
	const int64_t defaultSize = 1;

	const int64_t size = (value == NULL ? defaultSize : *value);
	uint64_t rest =
			static_cast<uint64_t>(std::max<int64_t>(size, defaultSize));

	typename C::WriterType &writer = cxt.initializeResultWriter(rest);

	while (rest > 0) {
		const size_t partSize =
				writer.append(SQLValues::ValueUtils::toLobCapacity(rest));
		SQLVdbeUtils::VdbeUtils::generateRandom(writer.lastData(), partSize);
		rest -= partSize;
	}

	return writer;
}


template<typename C, typename R>
inline typename C::WriterType&
SQLStringExprs::Functions::Replace::operator()(
		C &cxt, R &reader1, R &reader2, R &reader3) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const size_t keySize = reader2.getNext();

	for (;;) {
		const size_t rest = reader1.getNext();
		int64_t pos = SQLValues::ValueUtils::findPart(
				reader1, reader2,
				static_cast<int64_t>(reader1.getPrev()));
		size_t size = rest - reader1.getNext();
		if (size == 0) {
			break;
		}
		reader1.back(size);

		if (pos > 0) {
			const size_t stepSize = size - keySize;
			SQLValues::ValueUtils::subPart(
					writer, reader1, static_cast<int64_t>(stepSize));
			reader1.step(keySize);

			SQLValues::ValueUtils::subPart(
					writer, reader3,
					std::numeric_limits<int64_t>::max());
			reader3.toBegin();
		}
		else {
			SQLValues::ValueUtils::subPart(writer, reader1, size);
		}
		reader2.toBegin();
	}

	return writer;
}


template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Rtrim::operator()(
		C &cxt, R &reader1) {
	SQLValues::StringReader reader2(SQLValues::ValueUtils::toStringBuffer(" "));

	return (*this)(cxt, reader1, reader2);
}

template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Rtrim::operator()(
		C &cxt, R &reader1, R &reader2) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const int64_t end = SQLValues::ValueUtils::findRtrimPart(reader1, reader2);
	reader1.toBegin();
	SQLValues::ValueUtils::subPart(writer, reader1, 1, end - 1, true, util::FalseType());

	return writer;
}


template<typename Bounded>
template<typename C, typename R>
inline typename C::WriterType&
SQLStringExprs::Functions::Substr<Bounded>::operator()(
		C &cxt, R &value1, int64_t value2, int64_t value3) {
	return (*this)(cxt, value1, value2, &value3);
}

template<typename Bounded>
template<typename C, typename R>
inline typename C::WriterType&
SQLStringExprs::Functions::Substr<Bounded>::operator()(
		C &cxt, R &value1, int64_t value2, const int64_t *value3) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const int64_t start = value2;

	const bool limited = (value3 != NULL);
	const int64_t limit =
			(limited ? *value3 : std::numeric_limits<int64_t>::max());

	SQLValues::ValueUtils::subCodedPart(
			writer, value1, start, limit, limited, Bounded());

	return writer;
}


template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Trim::operator()(
		C &cxt, R &reader1) {
	SQLValues::StringReader reader2(SQLValues::ValueUtils::toStringBuffer(" "));

	return (*this)(cxt, reader1, reader2);
}

template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Trim::operator()(
		C &cxt, R &reader1, R &reader2) {
	typename C::WriterType &writer = cxt.getResultWriter();

	const int64_t start = SQLValues::ValueUtils::findLtrimPart(reader1, reader2);

	reader1.toBegin();
	reader2.toBegin();
	const int64_t end = SQLValues::ValueUtils::findRtrimPart(reader1, reader2);

	reader1.toBegin();
	SQLValues::ValueUtils::subPart(
			writer, reader1, start, std::max<int64_t>(end - start, 0), true,
			util::FalseType());

	return writer;
}


template<typename C, typename R>
inline std::pair<int64_t, bool>
SQLStringExprs::Functions::Unicode::operator()(C &cxt, R &value) {
	static_cast<void>(cxt);
	util::CodePoint c;
	if (!value.nextCode(&c)) {
		return std::pair<int64_t, bool>();
	}

	return std::pair<int64_t, bool>(static_cast<int64_t>(c), true);
}


template<typename C, typename R>
inline typename C::WriterType& SQLStringExprs::Functions::Upper::operator()(
		C &cxt, R &reader) {
	typename C::WriterType &writer = cxt.getResultWriter();

	for (util::CodePoint c; reader.nextCode(&c);) {
		writer.appendCode(SQLValues::ValueUtils::toUpper(c));
	}

	return writer;
}


template<typename C>
inline typename C::WriterType&
SQLStringExprs::Functions::ZeroBlob::operator()(
		C &cxt, const int64_t *value) {
	const int64_t defaultSize = 0;

	const int64_t size = (value == NULL ? defaultSize : *value);
	uint64_t rest =
			static_cast<uint64_t>(std::max<int64_t>(size, defaultSize));

	typename C::WriterType &writer = cxt.initializeResultWriter(rest);

	while (rest > 0) {
		const size_t partSize =
				writer.append(SQLValues::ValueUtils::toLobCapacity(rest));
		memset(writer.lastData(), 0, partSize);
		rest -= partSize;
	}

	return writer;
}
