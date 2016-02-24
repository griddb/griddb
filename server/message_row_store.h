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
/*!
	@file
    @brief Definition of MessageRowStore
*/
#ifndef MESSAGE_ROW_STORE_H_
#define MESSAGE_ROW_STORE_H_

#include "schema.h"
#include "value_processor.h" 


class ColumnInfo;
class DataStoreValueLimitConfig;
/*!
    @brief RowStore for message format
*/
class MessageRowStore {
protected:

	MessageRowStore(const DataStoreValueLimitConfig &dsValueLimitConfig, const ColumnInfo *columnInfoList, uint32_t columnCount);

public:

	virtual ~MessageRowStore();

	template<ColumnType C>
	struct ColumnTypeTraits {
		typedef void InvalidType;
		typedef InvalidType PrimitiveType;
	};

	/*!
		@brief Get variable size
	*/
	static inline uint32_t getVarSize(util::ByteStream<util::ArrayInStream> &in) {
		uint64_t currentPos = in.base().position();
		uint8_t byteData;
		in >> byteData;
		if (ValueProcessor::varSizeIs1Byte(byteData)) {
			return ValueProcessor::decode1ByteVarSize(byteData);
		} else {
			in.base().position(static_cast<size_t>(currentPos));
			uint32_t rawData;
			in >> rawData;
			return ValueProcessor::decode4ByteVarSize(rawData);
		}
	}

	/*!
		@brief Get variable size or OId
	*/
	static inline void getVarSizeOrOId(util::ByteStream<util::ArrayInStream> &in,
									   uint64_t &varSizeOrOId, bool &isOId) {
		size_t currentPos = in.base().position();
		uint8_t byteData;
		isOId = false;
		in >> byteData;
		if (ValueProcessor::varSizeIs1Byte(byteData)) {
			varSizeOrOId = ValueProcessor::decode1ByteVarSize(byteData);
		} else {
			if (ValueProcessor::varSizeIs4Byte(byteData)) {
				in.base().position(currentPos);
				uint32_t rawData;
				in >> rawData;
				varSizeOrOId = ValueProcessor::decode4ByteVarSize(rawData);
			} else {
				isOId = true;
				in.base().position(currentPos);
				uint64_t rawData;
				in >> rawData;
				varSizeOrOId = ValueProcessor::decode8ByteVarSize(rawData);
			}
		}
	}



	virtual bool next() = 0;

	virtual void reset() = 0;

	virtual uint64_t position() = 0;

	virtual void position(uint64_t p) = 0;

	virtual uint64_t getRowCount() = 0;


	void setRow(const MessageRowStore &source);

	void setField(const MessageRowStore &source, ColumnId columnId);



	const ColumnInfo* getColumnInfoList() const;

	uint32_t getColumnCount() const;

	ColumnId findVariableColumn(ColumnId startId) const;

	uint32_t getRowImageSize() const {return rowImageSize_;}        

	uint32_t getRowFixedSize() const {return rowFixedSize_;}        

	uint32_t getNullsOffset() const {return nullsOffset_;}          

	uint32_t getNullsBytes() const {return nullsBytes_;}            

	uint32_t getVariableColumnNum() const {return variableColumnNum_;}    


	virtual void getRowFixedPart(const void *&data, uint32_t &size) const;
	void getRowFixedPart(const uint8_t *&data, uint32_t &size) const;

	virtual uint8_t* getRowVariablePart() const;

	virtual void getField(ColumnId columnId, const void *&data, uint32_t &size) const;
	void getField(ColumnId columnId, const uint8_t *&data, uint32_t &size) const;

	template<ColumnType C>
	typename ColumnTypeTraits<C>::PrimitiveType getField(
			ColumnId columnId) const;

	virtual uint32_t getArrayLength(ColumnId columnId) const;

	virtual uint32_t getTotalArraySize(ColumnId columnId) const;

	virtual void getArrayElement(ColumnId columnId,
			uint32_t arrayIndex, const void *&data, uint32_t &size) const;
	void getArrayElement(ColumnId columnId,
			uint32_t arrayIndex, const uint8_t *&data, uint32_t &size) const;


	virtual void setRowId(RowId rowId);

	virtual void setRowFixedPart(const void *data, uint32_t size);

	virtual void beginRow();

	virtual void setField(ColumnId columnId, const void *data, uint32_t size);

	virtual void setFieldForRawData(ColumnId columnId, const void *data, uint32_t size);

	virtual void setField(ColumnId columnId, const Value &value);

	template<ColumnType C> void setField(ColumnId columnId,
			const typename ColumnTypeTraits<C>::PrimitiveType &value);

	virtual void addVariableFieldPart(const void *data, uint32_t size);

	virtual void setArrayField(ColumnId columnId);

	virtual void addArrayElement(const void *data, uint32_t size);

	virtual void setVarDataHeaderField(ColumnId columnId, uint32_t size);

	virtual void setVarSize(uint32_t varSize);

	virtual void setUInt64(uint64_t data);

protected:

	virtual void validate() const;


	const ColumnInfo& getColumnInfo(ColumnId columnId) const;

	bool isVariableColumn(ColumnId columnId) const;

	bool isLinkedVariableColumn(ColumnId columnId) const;  

	static uint32_t getColumnElementFixedSize(const ColumnInfo &info);

	template<typename S>
	class StreamResetter {
	public:
		StreamResetter(S &stream);
		~StreamResetter();

	private:
		S &stream_;
		size_t position_;
	};

protected:
	const DataStoreValueLimitConfig &dsValueLimitConfig_;
	uint32_t rowImageSize_;        
	uint32_t rowFixedSize_;        
	uint32_t nullsOffset_;         
	uint32_t nullsBytes_;          
	uint32_t variableColumnNum_;   

private:
	const ColumnInfo *columnInfoList_;
	uint32_t columnCount_;
};


/*!
    @brief RowStore for message format(from client)
*/
class InputMessageRowStore : public MessageRowStore {
public:

	InputMessageRowStore(const DataStoreValueLimitConfig &dsValueLimitConfig,
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		void *data, uint32_t size, uint64_t rowCount, uint32_t fixedRowSize,
		bool validateOnConstruct = true);
	InputMessageRowStore(const DataStoreValueLimitConfig &dsValueLimitConfig,
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		void *fixedData, uint32_t fixedDataSize, 
		void *varData, uint32_t varDataSize, 
		uint64_t rowCount, bool rowIdIncluded,
		bool validateOnConstruct = true);


	bool next();

	void reset();

	uint64_t position();

	void position(uint64_t p);

	uint64_t getRowCount();



	void getRowFixedPart(const void *&data, uint32_t &size) const;

	uint8_t* getRowVariablePart() const;

	/*!
		@brief Get field value
	*/
	void getField(ColumnId columnId, const void *&data, uint32_t &size) const {
		assert(hasActiveRow());

		const ColumnInfo &info = getColumnInfo(columnId);
		if (info.isVariable()) {
			util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;
			MessageRowStore::getVarSize(varDataIn); 

			for (uint32_t i = 0; i <= info.getColumnOffset(); ++i) {
				data = varData_ + varDataIn.base().position(); 
				size = MessageRowStore::getVarSize(varDataIn);
				if (size == 0) {
					data = NULL;
				}
				varDataIn.base().position(varDataIn.base().position() + size); 
			}
		}
		else {
			size = info.getColumnSize();
			data = fixedData_ + fixedDataIn_.base().position() + rowIdSize_ + info.getColumnOffset();
		}
	}

	/*!
		@brief Get field value
	*/
	void getField(ColumnId columnId, const uint8_t *&data, uint32_t &size) const {
		MessageRowStore::getField(columnId, data, size);
	}

	/*!
		@brief Get field value
	*/
	template<ColumnType C>
	typename ColumnTypeTraits<C>::PrimitiveType getField(
			ColumnId columnId) const {
		return MessageRowStore::getField<C>(columnId);
	}

	uint32_t getArrayLength(ColumnId columnId) const;

	uint32_t getTotalArraySize(ColumnId columnId) const;

	void getArrayElement(ColumnId columnId,
			uint32_t arrayIndex, const void *&data, uint32_t &size) const;

	bool getStartOffset(uint64_t startPos, uint64_t &fixedOffset, uint64_t &varOffset);
	void getPartialRowSet(uint64_t startPos, uint64_t rowNum, uint64_t &fixedOffset,
		uint64_t &fixedSize, uint64_t &varOffset, uint64_t &varSize);

	/*!
		@brief Reset field cursor position
	*/
	void resetFieldPos() {
		reset();
		nextFieldId_ = 0;
	}

	void getCurrentRowData(util::XArray<uint8_t> &rowData);


	/*!
		@brief Set field value
	*/
	void setField(const MessageRowStore &source, ColumnId columnId) {
		MessageRowStore::setField(source, columnId);
	}

	void setField(ColumnId columnId, const void *data, uint32_t size);

	void setFieldForRawData(ColumnId columnId, const void *data, uint32_t size);

	/*!
		@brief Set field value
	*/
	template<ColumnType C> void setField(ColumnId columnId,
			const typename ColumnTypeTraits<C>::PrimitiveType &value) {
		MessageRowStore::setField(columnId, value);
	}

	void setField(ColumnId columnId, const Value &value);

private:
	void validate() const;

	static util::ByteStream<util::ArrayInStream> getVarDataInput(
			void *data, uint32_t size, uint64_t rowCount, size_t fixedRowPartSize);

	bool hasActiveRow() const;

	size_t getVarDataOffset(ColumnId columnId) const {
		assert(isVariableColumn(columnId));

		const ColumnInfo &info = getColumnInfo(columnId);
		util::ByteStream<util::ArrayInStream> fixedDataIn = fixedDataIn_;
		util::ByteStream<util::ArrayInStream> varDataIn = varDataIn_;

		if (rowIdIncluded_) {
			RowId rowId;
			fixedDataIn >> rowId;
		}
		uint64_t varDataOffset;
		fixedDataIn >> varDataOffset;
		varDataIn.base().position(static_cast<size_t>(varDataOffset));

		MessageRowStore::getVarSize(varDataIn); 
		for (uint32_t i = 0; i < info.getColumnOffset(); ++i) {
			uint32_t varElemSize = MessageRowStore::getVarSize(varDataIn); 
			varDataIn.base().position(varDataIn.base().position() + varElemSize);
		}
		if (varDataIn.base().position() >= std::numeric_limits<size_t>::max()) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "Too large offset");
		}
		return static_cast<size_t>(varDataIn.base().position());
	}


	const uint64_t rowCount_;
	const bool rowIdIncluded_;
	const uint32_t rowIdSize_;
	util::ByteStream<util::ArrayInStream> varDataIn_;
	util::ByteStream<util::ArrayInStream> fixedDataIn_;
	uint8_t *varData_;
	uint8_t *fixedData_;
	uint64_t nextRowPosition_;
	uint32_t nextFieldId_; 
	uint32_t lastVarColumnNth_;    
	uint32_t lastVarColumnOffset_; 
};


/*!
    @brief RowStore for message format(to client)
*/
class OutputMessageRowStore : public MessageRowStore {
public:
	OutputMessageRowStore(const DataStoreValueLimitConfig &dsValueLimitConfig,
		const ColumnInfo *columnInfoList, uint32_t columnCount,
		util::XArray<uint8_t> &fixedData, util::XArray<uint8_t> &variableData,
		bool rowIdIncluded);


	bool next();

	void reset();

	uint64_t position();

	void position(uint64_t p);

	uint64_t getRowCount();


	void setVarSize(uint32_t varSize);

	void setUInt64(uint64_t data);

	void setNextOId(OId oId);

	void setRowId(RowId rowId);

	void setRowFixedPart(const void *data, uint32_t size);

	void beginRow();

	void setField(const MessageRowStore &source, ColumnId columnId) {
		MessageRowStore::setField(source, columnId);
	}

	void setField(ColumnId columnId, const void *data, uint32_t size);

	void setFieldForRawData(ColumnId columnId, const void *data, uint32_t size);

	template<ColumnType C> void setField(ColumnId columnId,
			const typename ColumnTypeTraits<C>::PrimitiveType &value);

	void setField(ColumnId columnId, const Value &value);

	void addVariableFieldPart(const void *data, uint32_t size);

	void setArrayField(ColumnId columnId);

	void addArrayElement(const void *data, uint32_t size);

	void setVarDataHeaderField(ColumnId columnId, uint32_t size);

	void getAllFixedPart(const uint8_t *&data, uint32_t &size) const;
	void getAllVariablePart(const uint8_t *&data, uint32_t &size) const;

private:
	uint32_t getFullRowFixedPartSize() const;

	size_t getFixedDataOffset(ColumnId columnId) const;

	const util::XArray<uint8_t> &varData_;
	const util::XArray<uint8_t> &fixedData_;
	util::ByteStream< util::XArrayOutStream<> > varDataOut_;
	util::ByteStream< util::XArrayOutStream<> > fixedDataOut_;
	util::XArray<bool> fieldsInitialized_;
	uint64_t rowCount_;
	uint32_t rowIdSize_;
	ColumnId lastColumnId_;
	bool rowIdIncluded_;
};



/*!
    @brief Row key decorder for message format
*/
class MessageRowKeyCoder {
public:
	explicit MessageRowKeyCoder(ColumnType keyType);

	void decode(util::ByteStream<util::ArrayInStream> &in,
			util::XArray<uint8_t> &rowKey) const;

private:
	const ColumnType keyType_;
};


template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_BOOL> {
	typedef bool PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_BYTE> {
	typedef int8_t PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_SHORT> {
	typedef int16_t PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_INT> {
	typedef int32_t PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_LONG> {
	typedef int64_t PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_FLOAT> {
	typedef float PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_DOUBLE> {
	typedef double PrimitiveType;
};

template<>
struct MessageRowStore::ColumnTypeTraits<COLUMN_TYPE_TIMESTAMP> {
	typedef Timestamp PrimitiveType;
};

template<ColumnType C>
typename MessageRowStore::ColumnTypeTraits<C>::PrimitiveType MessageRowStore::getField(
		ColumnId columnId) const {
	assert (C == getColumnInfo(columnId).getColumnType());

	const void *data;
	uint32_t size;
	getField(columnId, data, size);

	typename MessageRowStore::ColumnTypeTraits<C>::PrimitiveType value;
	assert (size == sizeof(value));
	memcpy(&value, data, sizeof(value));

	return value;
}

template<ColumnType C>
void MessageRowStore::setField(ColumnId columnId,
		const typename ColumnTypeTraits<C>::PrimitiveType &value) {
	assert (C == getColumnInfo(columnId).getColumnType());
	setField(columnId, &value, sizeof(value));
}

template<ColumnType C>
void OutputMessageRowStore::setField(ColumnId columnId,
		const typename ColumnTypeTraits<C>::PrimitiveType &value) {
	assert (C == getColumnInfo(columnId).getColumnType());
	setField(columnId, &value, sizeof(value));
}

#endif
