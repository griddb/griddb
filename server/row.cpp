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
	@brief Implementation of RowArray and RowArray::Row
*/
#include "util/trace.h"
#include "btree_map.h"
#include "collection.h"
#include "data_store.h"
#include "data_store_common.h"
#include "hash_map.h"
#include "transaction_manager.h"
#include "blob_processor.h"
#include "gs_error.h"
#include "message_schema.h"
#include "string_array_processor.h"
#include "value_processor.h"

#ifdef NDEBUG
#else
#endif

Collection::RowArray::Row::Row(uint8_t *rowImage, RowArray *rowArrayCursor)
	: rowArrayCursor_(rowArrayCursor), binary_(rowImage) {
	nullsOffset_ = (rowArrayCursor_->container_->getVariableColumnNum() > 0)
					   ? sizeof(OId)
					   : 0;
}

/*!
	@brief Initialize the area in Row
*/
void Collection::RowArray::Row::initialize() {
	memset(binary_, 0, rowArrayCursor_->getContainer().getRowSize());
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		setVariableArray(UNDEF_OID);
	}
}

/*!
	@brief Free Objects related to Row
*/
void Collection::RowArray::Row::finalize(TransactionContext &txn) {
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		if (getVariableArray() != UNDEF_OID) {
			ObjectManager &objectManager =
				*(rowArrayCursor_->getContainer().getObjectManager());
			VariableArrayCursor cursor(
				txn, objectManager, getVariableArray(), true);
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool nextFound = cursor.nextElement();
					UNUSED_VARIABLE(nextFound);
					assert(nextFound);
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemCount;
						uint8_t *elemData = cursor.getElement(
							elemSize, elemCount);  
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						}
					}
				}
			}
			cursor.finalize();
			setVariableArray(UNDEF_OID);
		}
	}
}

/*!
	@brief Calculate Object's size of variable field values
*/
void Collection::RowArray::Row::checkVarDataSize(TransactionContext &txn,
	uint32_t columnNum, uint32_t variableColumnNum, uint8_t *varTopAddr,
	util::XArray<uint32_t> &varColumnIdList,
	util::XArray<uint32_t> &varDataObjectSizeList,
	util::XArray<uint32_t> &varDataObjectPosList) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	VariableArrayCursor variableArrayCursor(varTopAddr);
	uint32_t currentObjectSize = ValueProcessor::getEncodedVarSize(
		variableColumnNum);  
	varDataObjectSizeList.push_back(currentObjectSize);  
	uint32_t varColumnObjectCount = 0;
	for (uint32_t columnId = 0; columnId < columnNum; columnId++) {
		ColumnInfo &columnInfo =
			rowArrayCursor_->getContainer().getColumnInfo(columnId);
		if (columnInfo.isVariable()) {
			varColumnIdList.push_back(columnId);
			bool nextFound = variableArrayCursor.nextElement();
			UNUSED_VARIABLE(nextFound);
			assert(nextFound);
			uint32_t elemSize;
			uint32_t elemNth;
			variableArrayCursor.getElement(elemSize, elemNth);
			if (columnInfo.isSpecialVariable()) {
				elemSize = LINK_VARIABLE_COLUMN_DATA_SIZE;
			}
			if ((currentObjectSize + elemSize +
					ValueProcessor::getEncodedVarSize(elemSize) +
					NEXT_OBJECT_LINK_INFO_SIZE) >
				static_cast<uint32_t>(objectManager.getMaxObjectSize())) {
				varDataObjectPosList.push_back(
					static_cast<uint32_t>(elemNth - 1));
				varDataObjectSizeList[varColumnObjectCount] = currentObjectSize;
				++varColumnObjectCount;
				currentObjectSize = 0;								 
				varDataObjectSizeList.push_back(currentObjectSize);  
			}
			currentObjectSize +=
				elemSize + ValueProcessor::getEncodedVarSize(elemSize);
		}
	}
	varDataObjectSizeList[varColumnObjectCount] = currentObjectSize;
	varDataObjectPosList.push_back(
		static_cast<uint32_t>(variableColumnNum - 1));
}
/*!
	@brief Set field values to RowArray Object
*/
void Collection::RowArray::Row::setFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	const void *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();

	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
		const AllocateStrategy allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(txn.getDefaultAllocator());
		varColumnIdList.reserve(variableColumnNum);

		util::XArray<uint32_t> varDataObjectSizeList(
			txn.getDefaultAllocator());  
		util::XArray<uint32_t> varDataObjectPosList(
			txn.getDefaultAllocator());  

		uint8_t *varTopAddr = messageRowStore->getRowVariablePart();

		checkVarDataSize(txn, rowArrayCursor_->getContainer().getColumnNum(),
			variableColumnNum, varTopAddr, varColumnIdList,
			varDataObjectSizeList, varDataObjectPosList);

		OId variableOId = UNDEF_OID;

		VariableArrayCursor variableArrayCursor(varTopAddr);
		uint8_t *copyStartAddr = varTopAddr;
		BaseObject destObj(txn.getPartitionId(), objectManager);
		for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
			BaseObject prevDestObj(txn.getPartitionId(), objectManager);
			prevDestObj.copyReference(destObj);
			prevDestObj.moveCursor(
				destObj.getCursor<uint8_t>() - destObj.getBaseAddr());
			destObj.allocateNeighbor<uint8_t>(varDataObjectSizeList[i],
				allocateStrategy, variableOId, rowArrayCursor_->getBaseOId(),
				OBJECT_TYPE_ROW);  
			if (i == 0) {
				setVariableArray(variableOId);
				uint32_t encodedVariableColumnNum =
					ValueProcessor::encodeVarSize(variableColumnNum);
				uint32_t encodedVariableColumnNumLen =
					ValueProcessor::getEncodedVarSize(variableColumnNum);
				memcpy(destObj.getCursor<uint8_t>(), &encodedVariableColumnNum,
					encodedVariableColumnNumLen);
				copyStartAddr += encodedVariableColumnNumLen;
				destObj.moveCursor(encodedVariableColumnNumLen);
			}
			else {
				uint64_t encodedOId =
					ValueProcessor::encodeVarSizeOId(variableOId);
				memcpy(prevDestObj.getCursor<uint8_t>(), &encodedOId,
					sizeof(uint64_t));
			}
			while (variableArrayCursor.nextElement()) {
				uint32_t elemSize;
				uint32_t elemNth;
				uint8_t *data =
					variableArrayCursor.getElement(elemSize, elemNth);
				uint32_t headerSize =
					ValueProcessor::getEncodedVarSize(elemSize);
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(
						varColumnIdList[elemNth]);
				if (columnInfo.isSpecialVariable()) {
					uint32_t linkHeaderValue = ValueProcessor::encodeVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
					uint32_t linkHeaderSize = ValueProcessor::getEncodedVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
					memcpy(destObj.getCursor<uint8_t>(), &linkHeaderValue,
						linkHeaderSize);
					destObj.moveCursor(linkHeaderSize);
					memcpy(destObj.getCursor<uint8_t>(), &elemSize,
						sizeof(uint32_t));  
					destObj.moveCursor(sizeof(uint32_t));
					OId linkOId = UNDEF_OID;
					if (elemSize > 0) {
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							linkOId = StringArrayProcessor::putToObject(txn,
								objectManager, data, elemSize, allocateStrategy,
								variableOId);  
							break;
						case COLUMN_TYPE_BLOB:
							linkOId = BlobProcessor::putToObject(txn,
								objectManager, data, elemSize, allocateStrategy,
								variableOId);  
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
					}
					memcpy(destObj.getCursor<uint8_t>(), &linkOId, sizeof(OId));
					destObj.moveCursor(sizeof(OId));
					data += headerSize + elemSize;
					copyStartAddr = data;
				}
				else {
					memcpy(destObj.getCursor<uint8_t>(), copyStartAddr,
						(data + headerSize + elemSize - copyStartAddr));
					destObj.moveCursor(
						data + headerSize + elemSize - copyStartAddr);
					copyStartAddr = data + headerSize + elemSize;
				}
				if (elemNth == varDataObjectPosList[i]) {
					break;
				}
			}
		}
	}
}

/*!
	@brief Updates field values on RowArray Object
*/
void Collection::RowArray::Row::updateFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	const uint8_t *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();
	OId oldVarDataOId = UNDEF_OID;
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		oldVarDataOId = *reinterpret_cast<OId *>(getVariableArrayAddr());
	}
	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
		const AllocateStrategy allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		OId destVarTopOId = oldVarDataOId;

		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(txn.getDefaultAllocator());
		varColumnIdList.reserve(
			rowArrayCursor_->getContainer().getVariableColumnNum());

		util::XArray<uint32_t> varDataObjectSizeList(
			txn.getDefaultAllocator());  
		util::XArray<uint32_t> varDataObjectPosList(
			txn.getDefaultAllocator());  

		uint8_t *varTopAddr = messageRowStore->getRowVariablePart();
		checkVarDataSize(txn, rowArrayCursor_->getContainer().getColumnNum(),
			variableColumnNum, varTopAddr, varColumnIdList,
			varDataObjectSizeList, varDataObjectPosList);

		util::XArray<OId> oldVarDataOIdList(txn.getDefaultAllocator());
		{
			VariableArrayCursor srcArrayCursor(
				txn, objectManager, oldVarDataOId, true);
			OId prevOId = UNDEF_OID;
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool srcNext = srcArrayCursor.nextElement();
					UNUSED_VARIABLE(srcNext);
					assert(srcNext);
					if (prevOId != srcArrayCursor.getElementOId()) {
						oldVarDataOIdList.push_back(
							srcArrayCursor.getElementOId());
						prevOId = srcArrayCursor.getElementOId();
					}
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemNth;
						uint8_t *data =
							srcArrayCursor.getElement(elemSize, elemNth);
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
					}
				}
			}
		}

		{
			VariableArrayCursor variableArrayCursor(varTopAddr);
			uint8_t *copyStartAddr = varTopAddr;
			uint8_t *destAddr = NULL;

			OId variableOId = UNDEF_OID;
			BaseObject oldVarObj(txn.getPartitionId(), objectManager);
			Size_t oldVarObjSize = 0;
			uint8_t *nextLinkAddr = NULL;
			for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
				if (i < oldVarDataOIdList.size()) {
					oldVarDataOId = oldVarDataOIdList[i];
					oldVarObj.load(oldVarDataOId, OBJECT_FOR_UPDATE);
					oldVarObjSize =
						objectManager.getSize(oldVarObj.getBaseAddr());
				}
				else {
					oldVarDataOId = UNDEF_OID;
					oldVarObjSize = 0;
				}
				if (oldVarObjSize >= varDataObjectSizeList[i]) {
					destAddr = oldVarObj.getBaseAddr();
					variableOId = oldVarDataOId;
				}
				else {
					if (UNDEF_OID != oldVarDataOId) {
						oldVarObj.finalize();
					}
					destAddr = oldVarObj.allocateNeighbor<uint8_t>(
						varDataObjectSizeList[i], allocateStrategy, variableOId,
						rowArrayCursor_->getBaseOId(),
						OBJECT_TYPE_ROW);  
					assert(destAddr);
				}
				if (i == 0) {
					setVariableArray(variableOId);
					destVarTopOId = variableOId;
					uint32_t encodedVariableColumnNum =
						ValueProcessor::encodeVarSize(variableColumnNum);
					uint32_t encodedVariableColumnNumLen =
						ValueProcessor::getEncodedVarSize(variableColumnNum);
					memcpy(destAddr, &encodedVariableColumnNum,
						encodedVariableColumnNumLen);
					copyStartAddr += encodedVariableColumnNumLen;
					destAddr += encodedVariableColumnNumLen;
				}
				else {
					assert(nextLinkAddr);
					uint64_t encodedOId =
						ValueProcessor::encodeVarSizeOId(variableOId);
					memcpy(nextLinkAddr, &encodedOId, sizeof(uint64_t));
					nextLinkAddr = NULL;
				}
				while (variableArrayCursor.nextElement()) {
					uint32_t elemSize;
					uint32_t elemNth;
					uint8_t *data =
						variableArrayCursor.getElement(elemSize, elemNth);
					uint32_t headerSize =
						ValueProcessor::getEncodedVarSize(elemSize);
					ColumnInfo &columnInfo =
						rowArrayCursor_->getContainer().getColumnInfo(
							varColumnIdList[elemNth]);
					if (columnInfo.isSpecialVariable()) {
						uint32_t linkHeaderValue =
							ValueProcessor::encodeVarSize(
								LINK_VARIABLE_COLUMN_DATA_SIZE);
						uint32_t linkHeaderSize =
							ValueProcessor::getEncodedVarSize(
								LINK_VARIABLE_COLUMN_DATA_SIZE);
						memcpy(destAddr, &linkHeaderValue, linkHeaderSize);
						destAddr += linkHeaderSize;
						memcpy(destAddr, &elemSize,
							sizeof(uint32_t));  
						destAddr += sizeof(uint32_t);
						OId linkOId = UNDEF_OID;
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							if (elemSize > 0) {
								linkOId = StringArrayProcessor::putToObject(txn,
									objectManager, data, elemSize,
									allocateStrategy,
									variableOId);  
							}
							break;
						case COLUMN_TYPE_BLOB:
							if (elemSize > 0) {
								linkOId = BlobProcessor::putToObject(txn,
									objectManager, data, elemSize,
									allocateStrategy,
									variableOId);  
							}
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
						memcpy(destAddr, &linkOId, sizeof(OId));
						destAddr += sizeof(OId);
						copyStartAddr = data + headerSize + elemSize;
					}
					else {
						memcpy(destAddr, copyStartAddr,
							(data + headerSize + elemSize - copyStartAddr));
						destAddr +=
							(data + headerSize + elemSize - copyStartAddr);
						copyStartAddr = data + headerSize + elemSize;
					}
					nextLinkAddr = destAddr;
					if (elemNth == varDataObjectPosList[i]) {
						break;
					}
				}
			}
		}
		for (size_t i = varDataObjectSizeList.size();
			 i < oldVarDataOIdList.size(); ++i) {
			assert(UNDEF_OID != oldVarDataOIdList[i]);
			if (UNDEF_OID != oldVarDataOIdList[i]) {
				objectManager.free(txn.getPartitionId(), oldVarDataOIdList[i]);
			}
		}
	}
}

/*!
	@brief Get field value
*/
void Collection::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, BaseObject &baseObject) {

	if (ValueProcessor::isSimple(columnInfo.getColumnType())) {
		baseObject.copyReference(this->rowArrayCursor_->getBaseOId(),
			this->getFixedAddr() + columnInfo.getColumnOffset());
	}
	else {
		OId variableOId = this->getVariableArray();
		ObjectManager &objectManager =
			*(rowArrayCursor_->getContainer().getObjectManager());
		VariableArrayCursor variableArrayCursor(
			txn, objectManager, variableOId, false);
		variableArrayCursor.getField(columnInfo, baseObject);
	}
}

/*!
	@brief Get field value
*/
void Collection::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, ContainerValue &containerValue) {
	getField(txn, columnInfo, containerValue.getBaseObject());
	containerValue.set(containerValue.getBaseObject().getCursor<uint8_t>(),
		columnInfo.getColumnType());
}

/*!
	@brief Get RowId
*/
void Collection::RowArray::Row::getRowIdField(uint8_t *&data) {
	data = getRowIdAddr();
}

/*!
	@brief Delete this Row
*/
void Collection::RowArray::Row::remove(TransactionContext &txn) {
	finalize(txn);
	setRemoved();
}

/*!
	@brief Move this Row to another RowArray
*/
void Collection::RowArray::Row::move(
	TransactionContext &txn, Collection::RowArray::Row &dest) {
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->rowSize_);
	setVariableArray(UNDEF_OID);
	remove(txn);
}

/*!
	@brief Copy this Row to another RowArray
*/
void Collection::RowArray::Row::copy(
	TransactionContext &txn, Collection::RowArray::Row &dest) {
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->rowSize_);
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0 &&
		getVariableArray() != UNDEF_OID) {
		ObjectManager &objectManager =
			*(rowArrayCursor_->getContainer().getObjectManager());
		const AllocateStrategy &allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		OId srcTopOId = getVariableArray();

		VariableArrayCursor srcCursor(txn, objectManager, srcTopOId, false);
		OId destTopOId = srcCursor.clone(txn, allocateStrategy,
			dest.rowArrayCursor_->getBaseOId());  
		dest.setVariableArray(destTopOId);

		VariableArrayCursor destCursor(txn, objectManager, destTopOId, true);
		for (uint32_t columnId = 0;
			 columnId < rowArrayCursor_->getContainer().getColumnNum();
			 ++columnId) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				bool exist = destCursor.nextElement();
				UNUSED_VARIABLE(exist);
				assert(exist);
				if (columnInfo.isSpecialVariable()) {
					uint32_t elemSize;
					uint32_t elemCount;
					uint8_t *elem = destCursor.getElement(elemSize, elemCount);
					switch (columnInfo.getColumnType()) {
					case COLUMN_TYPE_STRING_ARRAY:
						StringArrayProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), elem, elem,
							allocateStrategy,
							destTopOId);  
						break;
					case COLUMN_TYPE_BLOB:
						BlobProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), elem, elem,
							allocateStrategy,
							destTopOId);  
						break;
					default:
						GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
							"unknown columnType:"
								<< (int32_t)columnInfo.getColumnType());
					}
				}
			}
		}
	}
}

/*!
	@brief Lock this Row
*/
void Collection::RowArray::Row::lock(TransactionContext &txn) {
	if (getTxnId() == txn.getId()) {
	}
	else if (txn.getManager().isActiveTransaction(
				 txn.getPartitionId(), getTxnId())) {
		DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
			"(pId=" << txn.getPartitionId() << ", rowTxnId=" << getTxnId()
					<< ", txnId=" << txn.getId() << ")");
	}
	else {
		setLockTId(txn.getId());
	}
}

/*!
	@brief translate into Message format
*/
void Collection::RowArray::Row::getImage(TransactionContext &txn,
	MessageRowStore *messageRowStore, bool isWithRowId) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	if (isWithRowId) {
		messageRowStore->setRowId(getRowId());
	}
	messageRowStore->setRowFixedPart(
		getFixedAddr(),
		static_cast<uint32_t>(
			rowArrayCursor_->getContainer().getRowFixedDataSize()));
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		messageRowStore->setVarSize(
			rowArrayCursor_->getContainer().getVariableColumnNum());  

		OId variablePartOId = getVariableArray();
		VariableArrayCursor cursor(txn, objectManager, variablePartOId, false);
		for (uint32_t columnId = 0;
			 columnId < rowArrayCursor_->getContainer().getColumnNum();
			 columnId++) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				bool nextFound = cursor.nextElement();
				UNUSED_VARIABLE(nextFound);
				assert(nextFound);

				uint32_t elemSize;
				uint32_t elemCount;
				uint8_t *elemData = cursor.getElement(elemSize, elemCount);
				if (columnInfo.isSpecialVariable()) {
					uint32_t totalSize = 0;
					if (elemSize > 0) {
						assert(elemSize == LINK_VARIABLE_COLUMN_DATA_SIZE);
						elemData += ValueProcessor::getEncodedVarSize(elemSize);
						memcpy(&totalSize, elemData, sizeof(uint32_t));
						elemData += sizeof(uint32_t);
					}
					if (totalSize > 0) {
						OId linkOId;
						memcpy(&linkOId, elemData, sizeof(OId));
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY: {
							messageRowStore->setVarDataHeaderField(
								columnId, totalSize);
							VariableArrayCursor arrayCursor(
								txn, objectManager, linkOId, false);
							messageRowStore->setVarSize(
								arrayCursor.getArrayLength());  
							while (arrayCursor.nextElement()) {
								uint32_t elemSize, elemCount;
								uint8_t *addr =
									arrayCursor.getElement(elemSize, elemCount);
								messageRowStore->addArrayElement(
									addr, elemSize +
											  ValueProcessor::getEncodedVarSize(
												  elemSize));
							}
						} break;
						case COLUMN_TYPE_BLOB: {
							messageRowStore->setVarDataHeaderField(
								columnId, totalSize);
							ArrayObject oIdArrayObject(
								txn, objectManager, linkOId);
							uint32_t num = oIdArrayObject.getArrayLength();
							for (uint32_t blockCount = 0; blockCount < num;
								 blockCount++) {
								const uint8_t *elemData =
									oIdArrayObject.getArrayElement(
										blockCount, COLUMN_TYPE_OID);
								const OId elemOId =
									*reinterpret_cast<const OId *>(elemData);
								if (elemOId != UNDEF_OID) {
									BinaryObject elemObject(
										txn, objectManager, elemOId);
									messageRowStore->addVariableFieldPart(
										elemObject.data(), elemObject.size());
								}
							}
						} break;
						default:
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
							;
						}
					}
					else {
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY: {
							messageRowStore->setVarDataHeaderField(columnId, 1);
							messageRowStore->setVarSize(0);  
						} break;
						case COLUMN_TYPE_BLOB: {
							messageRowStore->setVarDataHeaderField(columnId, 0);
						} break;
						default:
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
							;
						}
					}
				}
				else {
					messageRowStore->setField(columnId, elemData, elemSize);
				}
			}
		}
	}
}

/*!
	@brief translate the field into Message format
*/
void Collection::RowArray::Row::getFieldImage(TransactionContext &txn,
	ColumnInfo &columnInfo, uint32_t newColumnId,
	MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	BaseObject baseFieldObject(txn.getPartitionId(), objectManager);
	getField(txn, columnInfo, baseFieldObject);
	Value value;
	value.set(baseFieldObject.getCursor<uint8_t>(), columnInfo.getColumnType());
	ValueProcessor::getField(
		txn, objectManager, newColumnId, &value, messageRowStore);
}

Collection::RowArray::RowArray(
	TransactionContext &txn, OId oId, Collection *container, uint8_t getOption)
	: BaseObject(txn.getPartitionId(), *(container->getObjectManager())),
	  container_(container),
	  elemCursor_(getElemCursor(oId)) {
	if (getOption != OBJECT_READ_ONLY || getBaseOId() != getBaseOId(oId)) {
		BaseObject::load(oId, getOption);
	}
	rowSize_ = container_->getRowSize();
}

Collection::RowArray::RowArray(TransactionContext &txn, Collection *container)
	: BaseObject(txn.getPartitionId(), *(container->getObjectManager())),
	  container_(container),
	  elemCursor_(0) {
	rowSize_ = container_->getRowSize();
}

/*!
	@brief Get Object from Chunk
*/
void Collection::RowArray::load(TransactionContext &txn, OId oId,
	Collection *container, uint8_t getOption) {
	BaseObject::load(oId, getOption);
	elemCursor_ = getElemCursor(oId);
	container_ = container;
}

/*!
	@brief Allocate RowArray Object
*/
void Collection::RowArray::initialize(
	TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum) {
	OId oId;
	BaseObject::allocate<uint8_t>(getBinarySize(maxRowNum),
		container_->getRowAllcateStrategy(), oId, OBJECT_TYPE_ROW_ARRAY);
	memset(getBaseAddr(), 0, HEADER_SIZE);
	setMaxRowNum(maxRowNum);
	setRowNum(0);
	setRowId(baseRowId);
	for (uint16_t i = 0; i < getMaxRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		row.reset();
	}
	elemCursor_ = 0;
}

/*!
	@brief Free Objects related to RowArray
*/
void Collection::RowArray::finalize(TransactionContext &txn) {
	setDirty();
	for (begin(); !end(); next()) {
		Row row(getRow(), this);
		row.finalize(txn);
	}
	BaseObject::finalize();
}

/*!
	@brief Append Row to current cursor
*/
void Collection::RowArray::append(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	Row row(getNewRow(), this);
	row.initialize();
	row.setRowId(rowId);
	row.setFields(txn, messageRowStore);
	setRowNum(getRowNum() + 1);
}

/*!
	@brief Insert Row to current cursor
*/
void Collection::RowArray::insert(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	Row row(getRow(), this);
	row.initialize();
	row.setRowId(rowId);
	row.setFields(txn, messageRowStore);
	if (elemCursor_ >= getRowNum()) {
		setRowNum(elemCursor_ + 1);
	}
}

/*!
	@brief Update Row on current cursor
*/
void Collection::RowArray::update(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	Row row(getRow(), this);
	row.updateFields(txn, messageRowStore);
}

/*!
	@brief Delete this Row on current cursor
*/
void Collection::RowArray::remove(TransactionContext &txn) {
	Row row(getRow(), this);
	row.remove(txn);
	if (elemCursor_ == getRowNum() - 1) {
		setRowNum(elemCursor_);
	}
}

/*!
	@brief Move Row on current cursor to Another RowArray
*/
void Collection::RowArray::move(TransactionContext &txn, RowArray &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.move(txn, destRow);
	updateCursor();
	dest.updateCursor();
}

/*!
	@brief Copy Row on current cursor to Another RowArray
*/
void Collection::RowArray::copy(TransactionContext &txn, RowArray &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.copy(txn, destRow);
	updateCursor();
	dest.updateCursor();
}

/*!
	@brief Move to next RowArray, and Check if RowArray exists
*/
bool Collection::RowArray::nextRowArray(
	TransactionContext &txn, RowArray &neighbor, uint8_t getOption) {
	uint16_t currentCursor = elemCursor_;
	tail();
	Row row(getRow(), this);
	RowId rowId = row.getRowId();
	BtreeMap::SearchContext sc(
		0, &rowId, 0, false, NULL, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			neighbor.load(txn, *itr, container_, getOption);
			elemCursor_ = currentCursor;
			return true;
		}
	}
	elemCursor_ = currentCursor;
	return false;
}

/*!
	@brief Move to prev RowArray, and Check if RowArray exists
*/
bool Collection::RowArray::prevRowArray(
	TransactionContext &txn, RowArray &neighbor, uint8_t getOption) {
	uint16_t currentCursor = elemCursor_;
	begin();
	Row row(getRow(), this);
	RowId rowId = row.getRowId();
	BtreeMap::SearchContext sc(
		0, NULL, 0, false, &rowId, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			neighbor.load(txn, *itr, container_, getOption);
			elemCursor_ = currentCursor;
			return true;
		}
	}
	elemCursor_ = currentCursor;
	return false;
}

/*!
	@brief Search Row corresponding to RowId
*/
bool Collection::RowArray::searchRowId(RowId rowId) {
	for (uint16_t i = 0; i < getRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		if (!row.isRemoved()) {
			RowId currentRowId = row.getRowId();
			if (currentRowId == rowId) {
				return true;
			}
			else if (currentRowId > rowId) {
				return false;
			}
		}
	}
	elemCursor_ = getRowNum();
	return false;
}

/*!
	@brief Shift Rows to next position
*/
void Collection::RowArray::shift(TransactionContext &txn, bool isForce,
	util::XArray<std::pair<OId, OId> > &moveList) {
	if (isForce) {
		elemCursor_ = getMaxRowNum();
	}
	uint16_t insertPos = elemCursor_;
	uint16_t targetPos = getMaxRowNum();

	for (uint16_t i = insertPos; i < getMaxRowNum(); i++) {
		Row row(getRow(i), this);
		if (row.isRemoved()) {
			targetPos = i;
			break;
		}
	}
	if (targetPos != getMaxRowNum()) {
		for (uint16_t i = targetPos; i > insertPos; i--) {
			elemCursor_ = i - 1;
			Row row(getRow(), this);
			OId oldOId = getOId();
			elemCursor_ = i;
			Row destRow(getRow(), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			if (destRow.isRemoved()) {
				break;
			}
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		elemCursor_ = insertPos;
	}
	else {
		for (uint16_t i = insertPos + 1; i > 0; i--) {
			if (i >= getMaxRowNum()) {
				continue;
			}
			Row row(getRow(i - 1), this);
			if (row.isRemoved()) {
				targetPos = i - 1;
				break;
			}
		}
		for (uint16_t i = targetPos; i < insertPos; i++) {
			elemCursor_ = i;
			Row row(getRow(), this);
			if (!row.isRemoved()) {
				OId oldOId = getOId();
				elemCursor_ = i - 1;
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
		}
		elemCursor_ = insertPos - 1;
	}
	updateCursor();
}

/*!
	@brief Split this RowArray
*/
void Collection::RowArray::split(TransactionContext &txn, RowId insertRowId,
	RowArray &splitRowArray, RowId splitRowId,
	util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t insertPos = elemCursor_;
	uint16_t midPos = getMaxRowNum() / 2;
	if (insertRowId < splitRowId) {
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			elemCursor_ = i;
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		for (uint16_t i = midPos; i > insertPos; i--) {
			elemCursor_ = i - 1;
			Row row(getRow(), this);
			OId oldOId = getOId();
			elemCursor_ = i;
			Row destRow(getRow(i), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		elemCursor_ = insertPos;
	}
	else {
		uint16_t destCursor = getMaxRowNum() - midPos;
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			elemCursor_ = i;
			if (i == insertPos) {
				destCursor = splitRowArray.elemCursor_;
				splitRowArray.updateCursor();
				splitRowArray.next();
			}
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		splitRowArray.elemCursor_ = destCursor;
		elemCursor_ = midPos - 1;
	}
	updateCursor();
	splitRowArray.updateCursor();
}

/*!
	@brief Merge this RowArray and another RowArray
*/
void Collection::RowArray::merge(TransactionContext &txn,
	RowArray &nextRowArray, util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t pos = 0;
	for (uint16_t i = 0; i < getRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		if (!row.isRemoved()) {
			if (pos != i) {
				OId oldOId = getOId();
				elemCursor_ = pos;
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
			pos++;
		}
	}
	for (nextRowArray.begin(); !nextRowArray.end(); nextRowArray.next()) {
		elemCursor_ = pos;
		OId oldOId = nextRowArray.getOId();
		nextRowArray.move(txn, *this);
		OId newOId = getOId();
		moveList.push_back(std::make_pair(oldOId, newOId));
		pos++;
	}
	updateCursor();
	nextRowArray.updateCursor();
}

std::string Collection::RowArray::dump(TransactionContext &txn) {
	uint16_t pos = elemCursor_;
	util::NormalOStringStream strstrm;
	strstrm << "RowId," << getRowId() << ",MaxRowNum," << getMaxRowNum()
			<< ",RowNum," << getActiveRowNum() << std::endl;
	strstrm << "ChunkId,Offset,ElemNum" << std::endl;
	for (begin(); !end(); next()) {
		Row row(getRow(), this);
		OId oId = getOId();
		strstrm << ObjectManager::getChunkId(oId) << ","
				<< ObjectManager::getOffset(oId) << ","
				<< ObjectManager::getChunkId(oId) << "," << getElemCursor(oId)
				<< ",";
		strstrm << row.dump(txn) << std::endl;
	}
	elemCursor_ = pos;
	return strstrm.str();
}

std::string Collection::RowArray::Row::dump(TransactionContext &txn) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	util::NormalOStringStream strstrm;
	strstrm << ", RowId=" << getRowId() << ", TxnId=" << getTxnId() << ", ";
	ContainerValue containerValue(txn, objectManager);
	for (uint32_t i = 0; i < rowArrayCursor_->getContainer().getColumnNum();
		 i++) {
		getField(txn, rowArrayCursor_->getContainer().getColumnInfo(i),
			containerValue);
		strstrm << containerValue.getValue().dump(txn, objectManager) << ", ";
	}
	return strstrm.str();
}


TimeSeries::RowArray::Row::Row(uint8_t *rowImage, RowArray *rowArrayCursor)
	: rowArrayCursor_(rowArrayCursor), binary_(rowImage) {
	nullsOffset_ = (rowArrayCursor_->container_->getVariableColumnNum() > 0)
					   ? sizeof(OId)
					   : 0;
}

/*!
	@brief Initialize the area in Row
*/
void TimeSeries::RowArray::Row::initialize() {
	memset(binary_, 0, rowArrayCursor_->getContainer().getRowSize());
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		setVariableArray(UNDEF_OID);
	}
}

/*!
	@brief Free Objects related to Row
*/
void TimeSeries::RowArray::Row::finalize(TransactionContext &txn) {
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		if (getVariableArray() != UNDEF_OID) {
			ObjectManager &objectManager =
				*(rowArrayCursor_->getContainer().getObjectManager());
			VariableArrayCursor cursor(
				txn, objectManager, getVariableArray(), true);
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool nextFound = cursor.nextElement();
					UNUSED_VARIABLE(nextFound);
					assert(nextFound);
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemCount;
						uint8_t *elemData = cursor.getElement(
							elemSize, elemCount);  
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						}
					}
				}
			}
			cursor.finalize();
			setVariableArray(UNDEF_OID);
		}
	}
}

/*!
	@brief Calculate Object's size of variable field values
*/
void TimeSeries::RowArray::Row::checkVarDataSize(TransactionContext &txn,
	uint32_t columnNum, uint32_t variableColumnNum, uint8_t *varTopAddr,
	util::XArray<uint32_t> &varColumnIdList,
	util::XArray<uint32_t> &varDataObjectSizeList,
	util::XArray<uint32_t> &varDataObjectPosList) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	VariableArrayCursor variableArrayCursor(varTopAddr);
	uint32_t currentObjectSize = ValueProcessor::getEncodedVarSize(
		variableColumnNum);  
	varDataObjectSizeList.push_back(currentObjectSize);  
	uint32_t varColumnObjectCount = 0;
	for (uint32_t columnId = 0; columnId < columnNum; columnId++) {
		ColumnInfo &columnInfo =
			rowArrayCursor_->getContainer().getColumnInfo(columnId);
		if (columnInfo.isVariable()) {
			varColumnIdList.push_back(columnId);
			bool nextFound = variableArrayCursor.nextElement();
			UNUSED_VARIABLE(nextFound);
			assert(nextFound);
			uint32_t elemSize;
			uint32_t elemNth;
			variableArrayCursor.getElement(elemSize, elemNth);
			if (columnInfo.isSpecialVariable()) {
				elemSize = LINK_VARIABLE_COLUMN_DATA_SIZE;
			}
			if ((currentObjectSize + elemSize +
					ValueProcessor::getEncodedVarSize(elemSize) +
					NEXT_OBJECT_LINK_INFO_SIZE) >
				static_cast<uint32_t>(objectManager.getMaxObjectSize())) {
				varDataObjectPosList.push_back(
					static_cast<uint32_t>(elemNth - 1));
				varDataObjectSizeList[varColumnObjectCount] = currentObjectSize;
				++varColumnObjectCount;
				currentObjectSize = 0;								 
				varDataObjectSizeList.push_back(currentObjectSize);  
			}
			currentObjectSize +=
				elemSize + ValueProcessor::getEncodedVarSize(elemSize);
		}
	}
	varDataObjectSizeList[varColumnObjectCount] = currentObjectSize;
	varDataObjectPosList.push_back(
		static_cast<uint32_t>(variableColumnNum - 1));
}
/*!
	@brief Set field values to RowArray Object
*/
void TimeSeries::RowArray::Row::setFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	const void *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();

	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
		const AllocateStrategy allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(txn.getDefaultAllocator());
		varColumnIdList.reserve(variableColumnNum);

		util::XArray<uint32_t> varDataObjectSizeList(
			txn.getDefaultAllocator());  
		util::XArray<uint32_t> varDataObjectPosList(
			txn.getDefaultAllocator());  

		uint8_t *varTopAddr = messageRowStore->getRowVariablePart();

		checkVarDataSize(txn, rowArrayCursor_->getContainer().getColumnNum(),
			variableColumnNum, varTopAddr, varColumnIdList,
			varDataObjectSizeList, varDataObjectPosList);

		OId variableOId = UNDEF_OID;

		VariableArrayCursor variableArrayCursor(varTopAddr);
		uint8_t *copyStartAddr = varTopAddr;
		BaseObject destObj(txn.getPartitionId(), objectManager);
		for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
			BaseObject prevDestObj(txn.getPartitionId(), objectManager);
			prevDestObj.copyReference(destObj);
			prevDestObj.moveCursor(
				destObj.getCursor<uint8_t>() - destObj.getBaseAddr());
			destObj.allocateNeighbor<uint8_t>(varDataObjectSizeList[i],
				allocateStrategy, variableOId, rowArrayCursor_->getBaseOId(),
				OBJECT_TYPE_ROW);  
			if (i == 0) {
				setVariableArray(variableOId);
				uint32_t encodedVariableColumnNum =
					ValueProcessor::encodeVarSize(variableColumnNum);
				uint32_t encodedVariableColumnNumLen =
					ValueProcessor::getEncodedVarSize(variableColumnNum);
				memcpy(destObj.getCursor<uint8_t>(), &encodedVariableColumnNum,
					encodedVariableColumnNumLen);
				copyStartAddr += encodedVariableColumnNumLen;
				destObj.moveCursor(encodedVariableColumnNumLen);
			}
			else {
				uint64_t encodedOId =
					ValueProcessor::encodeVarSizeOId(variableOId);
				memcpy(prevDestObj.getCursor<uint8_t>(), &encodedOId,
					sizeof(uint64_t));
			}
			while (variableArrayCursor.nextElement()) {
				uint32_t elemSize;
				uint32_t elemNth;
				uint8_t *data =
					variableArrayCursor.getElement(elemSize, elemNth);
				uint32_t headerSize =
					ValueProcessor::getEncodedVarSize(elemSize);
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(
						varColumnIdList[elemNth]);
				if (columnInfo.isSpecialVariable()) {
					uint32_t linkHeaderValue = ValueProcessor::encodeVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
					uint32_t linkHeaderSize = ValueProcessor::getEncodedVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
					memcpy(destObj.getCursor<uint8_t>(), &linkHeaderValue,
						linkHeaderSize);
					destObj.moveCursor(linkHeaderSize);
					memcpy(destObj.getCursor<uint8_t>(), &elemSize,
						sizeof(uint32_t));  
					destObj.moveCursor(sizeof(uint32_t));
					OId linkOId = UNDEF_OID;
					if (elemSize > 0) {
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							linkOId = StringArrayProcessor::putToObject(txn,
								objectManager, data, elemSize, allocateStrategy,
								variableOId);  
							break;
						case COLUMN_TYPE_BLOB:
							linkOId = BlobProcessor::putToObject(txn,
								objectManager, data, elemSize, allocateStrategy,
								variableOId);  
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
					}
					memcpy(destObj.getCursor<uint8_t>(), &linkOId, sizeof(OId));
					destObj.moveCursor(sizeof(OId));
					data += headerSize + elemSize;
					copyStartAddr = data;
				}
				else {
					memcpy(destObj.getCursor<uint8_t>(), copyStartAddr,
						(data + headerSize + elemSize - copyStartAddr));
					destObj.moveCursor(
						data + headerSize + elemSize - copyStartAddr);
					copyStartAddr = data + headerSize + elemSize;
				}
				if (elemNth == varDataObjectPosList[i]) {
					break;
				}
			}
		}
	}
}

/*!
	@brief Updates field values on RowArray Object
*/
void TimeSeries::RowArray::Row::updateFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	const uint8_t *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();
	OId oldVarDataOId = UNDEF_OID;
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		oldVarDataOId = *reinterpret_cast<OId *>(getVariableArrayAddr());
	}
	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
		const AllocateStrategy allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		OId destVarTopOId = oldVarDataOId;

		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(txn.getDefaultAllocator());
		varColumnIdList.reserve(
			rowArrayCursor_->getContainer().getVariableColumnNum());

		util::XArray<uint32_t> varDataObjectSizeList(
			txn.getDefaultAllocator());  
		util::XArray<uint32_t> varDataObjectPosList(
			txn.getDefaultAllocator());  

		uint8_t *varTopAddr = messageRowStore->getRowVariablePart();
		checkVarDataSize(txn, rowArrayCursor_->getContainer().getColumnNum(),
			variableColumnNum, varTopAddr, varColumnIdList,
			varDataObjectSizeList, varDataObjectPosList);

		util::XArray<OId> oldVarDataOIdList(txn.getDefaultAllocator());
		{
			VariableArrayCursor srcArrayCursor(
				txn, objectManager, oldVarDataOId, true);
			OId prevOId = UNDEF_OID;
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool srcNext = srcArrayCursor.nextElement();
					UNUSED_VARIABLE(srcNext);
					assert(srcNext);
					if (prevOId != srcArrayCursor.getElementOId()) {
						oldVarDataOIdList.push_back(
							srcArrayCursor.getElementOId());
						prevOId = srcArrayCursor.getElementOId();
					}
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemNth;
						uint8_t *data =
							srcArrayCursor.getElement(elemSize, elemNth);
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
					}
				}
			}
		}

		{
			VariableArrayCursor variableArrayCursor(varTopAddr);
			uint8_t *copyStartAddr = varTopAddr;
			uint8_t *destAddr = NULL;

			OId variableOId = UNDEF_OID;
			BaseObject oldVarObj(txn.getPartitionId(), objectManager);
			Size_t oldVarObjSize = 0;
			uint8_t *nextLinkAddr = NULL;
			for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
				if (i < oldVarDataOIdList.size()) {
					oldVarDataOId = oldVarDataOIdList[i];
					oldVarObj.load(oldVarDataOId, OBJECT_FOR_UPDATE);
					oldVarObjSize =
						objectManager.getSize(oldVarObj.getBaseAddr());
				}
				else {
					oldVarDataOId = UNDEF_OID;
					oldVarObjSize = 0;
				}
				if (oldVarObjSize >= varDataObjectSizeList[i]) {
					destAddr = oldVarObj.getBaseAddr();
					variableOId = oldVarDataOId;
				}
				else {
					if (UNDEF_OID != oldVarDataOId) {
						oldVarObj.finalize();
					}
					destAddr = oldVarObj.allocateNeighbor<uint8_t>(
						varDataObjectSizeList[i], allocateStrategy, variableOId,
						rowArrayCursor_->getBaseOId(),
						OBJECT_TYPE_ROW);  
					assert(destAddr);
				}
				if (i == 0) {
					setVariableArray(variableOId);
					destVarTopOId = variableOId;
					uint32_t encodedVariableColumnNum =
						ValueProcessor::encodeVarSize(variableColumnNum);
					uint32_t encodedVariableColumnNumLen =
						ValueProcessor::getEncodedVarSize(variableColumnNum);
					memcpy(destAddr, &encodedVariableColumnNum,
						encodedVariableColumnNumLen);
					copyStartAddr += encodedVariableColumnNumLen;
					destAddr += encodedVariableColumnNumLen;
				}
				else {
					assert(nextLinkAddr);
					uint64_t encodedOId =
						ValueProcessor::encodeVarSizeOId(variableOId);
					memcpy(nextLinkAddr, &encodedOId, sizeof(uint64_t));
					nextLinkAddr = NULL;
				}
				while (variableArrayCursor.nextElement()) {
					uint32_t elemSize;
					uint32_t elemNth;
					uint8_t *data =
						variableArrayCursor.getElement(elemSize, elemNth);
					uint32_t headerSize =
						ValueProcessor::getEncodedVarSize(elemSize);
					ColumnInfo &columnInfo =
						rowArrayCursor_->getContainer().getColumnInfo(
							varColumnIdList[elemNth]);
					if (columnInfo.isSpecialVariable()) {
						uint32_t linkHeaderValue =
							ValueProcessor::encodeVarSize(
								LINK_VARIABLE_COLUMN_DATA_SIZE);
						uint32_t linkHeaderSize =
							ValueProcessor::getEncodedVarSize(
								LINK_VARIABLE_COLUMN_DATA_SIZE);
						memcpy(destAddr, &linkHeaderValue, linkHeaderSize);
						destAddr += linkHeaderSize;
						memcpy(destAddr, &elemSize,
							sizeof(uint32_t));  
						destAddr += sizeof(uint32_t);
						OId linkOId = UNDEF_OID;
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							if (elemSize > 0) {
								linkOId = StringArrayProcessor::putToObject(txn,
									objectManager, data, elemSize,
									allocateStrategy,
									variableOId);  
							}
							break;
						case COLUMN_TYPE_BLOB:
							if (elemSize > 0) {
								linkOId = BlobProcessor::putToObject(txn,
									objectManager, data, elemSize,
									allocateStrategy,
									variableOId);  
							}
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
						memcpy(destAddr, &linkOId, sizeof(OId));
						destAddr += sizeof(OId);
						copyStartAddr = data + headerSize + elemSize;
					}
					else {
						memcpy(destAddr, copyStartAddr,
							(data + headerSize + elemSize - copyStartAddr));
						destAddr +=
							(data + headerSize + elemSize - copyStartAddr);
						copyStartAddr = data + headerSize + elemSize;
					}
					nextLinkAddr = destAddr;
					if (elemNth == varDataObjectPosList[i]) {
						break;
					}
				}
			}
		}
		for (size_t i = varDataObjectSizeList.size();
			 i < oldVarDataOIdList.size(); ++i) {
			assert(UNDEF_OID != oldVarDataOIdList[i]);
			if (UNDEF_OID != oldVarDataOIdList[i]) {
				objectManager.free(txn.getPartitionId(), oldVarDataOIdList[i]);
			}
		}
	}
}

/*!
	@brief Get field value
*/
void TimeSeries::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, BaseObject &baseObject) {

	if (ValueProcessor::isSimple(columnInfo.getColumnType())) {
		baseObject.copyReference(this->rowArrayCursor_->getBaseOId(),
			this->getFixedAddr() + columnInfo.getColumnOffset());
	}
	else {
		OId variableOId = this->getVariableArray();
		ObjectManager &objectManager =
			*(rowArrayCursor_->getContainer().getObjectManager());
		VariableArrayCursor variableArrayCursor(
			txn, objectManager, variableOId, false);
		variableArrayCursor.getField(columnInfo, baseObject);
	}
}

/*!
	@brief Get field value
*/
void TimeSeries::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, ContainerValue &containerValue) {
	getField(txn, columnInfo, containerValue.getBaseObject());
	containerValue.set(containerValue.getBaseObject().getCursor<uint8_t>(),
		columnInfo.getColumnType());
}

/*!
	@brief Get RowId
*/
void TimeSeries::RowArray::Row::getRowIdField(uint8_t *&data) {
	data = getRowIdAddr();
}

/*!
	@brief Delete this Row
*/
void TimeSeries::RowArray::Row::remove(TransactionContext &txn) {
	finalize(txn);
	setRemoved();
}

/*!
	@brief Move this Row to another RowArray
*/
void TimeSeries::RowArray::Row::move(
	TransactionContext &txn, TimeSeries::RowArray::Row &dest) {
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->rowSize_);
	setVariableArray(UNDEF_OID);
	remove(txn);
}

/*!
	@brief Copy this Row to another RowArray
*/
void TimeSeries::RowArray::Row::copy(
	TransactionContext &txn, TimeSeries::RowArray::Row &dest) {
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->rowSize_);
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0 &&
		getVariableArray() != UNDEF_OID) {
		ObjectManager &objectManager =
			*(rowArrayCursor_->getContainer().getObjectManager());
		const AllocateStrategy &allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		OId srcTopOId = getVariableArray();

		VariableArrayCursor srcCursor(txn, objectManager, srcTopOId, false);
		OId destTopOId = srcCursor.clone(txn, allocateStrategy,
			dest.rowArrayCursor_->getBaseOId());  
		dest.setVariableArray(destTopOId);

		VariableArrayCursor destCursor(txn, objectManager, destTopOId, true);
		for (uint32_t columnId = 0;
			 columnId < rowArrayCursor_->getContainer().getColumnNum();
			 ++columnId) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				bool exist = destCursor.nextElement();
				UNUSED_VARIABLE(exist);
				assert(exist);
				if (columnInfo.isSpecialVariable()) {
					uint32_t elemSize;
					uint32_t elemCount;
					uint8_t *elem = destCursor.getElement(elemSize, elemCount);
					switch (columnInfo.getColumnType()) {
					case COLUMN_TYPE_STRING_ARRAY:
						StringArrayProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), elem, elem,
							allocateStrategy,
							destTopOId);  
						break;
					case COLUMN_TYPE_BLOB:
						BlobProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), elem, elem,
							allocateStrategy,
							destTopOId);  
						break;
					default:
						GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
							"unknown columnType:"
								<< (int32_t)columnInfo.getColumnType());
					}
				}
			}
		}
	}
}

/*!
	@brief translate into Message format
*/
void TimeSeries::RowArray::Row::getImage(TransactionContext &txn,
	MessageRowStore *messageRowStore, bool isWithRowId) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	if (isWithRowId) {
		messageRowStore->setRowId(getRowId());
	}
	messageRowStore->setRowFixedPart(
		getFixedAddr(),
		static_cast<uint32_t>(
			rowArrayCursor_->getContainer().getRowFixedDataSize()));
	if (rowArrayCursor_->getContainer().getVariableColumnNum() > 0) {
		messageRowStore->setVarSize(
			rowArrayCursor_->getContainer().getVariableColumnNum());  

		OId variablePartOId = getVariableArray();
		VariableArrayCursor cursor(txn, objectManager, variablePartOId, false);
		for (uint32_t columnId = 0;
			 columnId < rowArrayCursor_->getContainer().getColumnNum();
			 columnId++) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				bool nextFound = cursor.nextElement();
				UNUSED_VARIABLE(nextFound);
				assert(nextFound);

				uint32_t elemSize;
				uint32_t elemCount;
				uint8_t *elemData = cursor.getElement(elemSize, elemCount);
				if (columnInfo.isSpecialVariable()) {
					uint32_t totalSize = 0;
					if (elemSize > 0) {
						assert(elemSize == LINK_VARIABLE_COLUMN_DATA_SIZE);
						elemData += ValueProcessor::getEncodedVarSize(elemSize);
						memcpy(&totalSize, elemData, sizeof(uint32_t));
						elemData += sizeof(uint32_t);
					}
					if (totalSize > 0) {
						OId linkOId;
						memcpy(&linkOId, elemData, sizeof(OId));
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY: {
							messageRowStore->setVarDataHeaderField(
								columnId, totalSize);
							VariableArrayCursor arrayCursor(
								txn, objectManager, linkOId, false);
							messageRowStore->setVarSize(
								arrayCursor.getArrayLength());  
							while (arrayCursor.nextElement()) {
								uint32_t elemSize, elemCount;
								uint8_t *addr =
									arrayCursor.getElement(elemSize, elemCount);
								messageRowStore->addArrayElement(
									addr, elemSize +
											  ValueProcessor::getEncodedVarSize(
												  elemSize));
							}
						} break;
						case COLUMN_TYPE_BLOB: {
							messageRowStore->setVarDataHeaderField(
								columnId, totalSize);
							ArrayObject oIdArrayObject(
								txn, objectManager, linkOId);
							uint32_t num = oIdArrayObject.getArrayLength();
							for (uint32_t blockCount = 0; blockCount < num;
								 blockCount++) {
								const uint8_t *elemData =
									oIdArrayObject.getArrayElement(
										blockCount, COLUMN_TYPE_OID);
								const OId elemOId =
									*reinterpret_cast<const OId *>(elemData);
								if (elemOId != UNDEF_OID) {
									BinaryObject elemObject(
										txn, objectManager, elemOId);
									messageRowStore->addVariableFieldPart(
										elemObject.data(), elemObject.size());
								}
							}
						} break;
						default:
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
							;
						}
					}
					else {
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY: {
							messageRowStore->setVarDataHeaderField(columnId, 1);
							messageRowStore->setVarSize(0);  
						} break;
						case COLUMN_TYPE_BLOB: {
							messageRowStore->setVarDataHeaderField(columnId, 0);
						} break;
						default:
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
							;
						}
					}
				}
				else {
					messageRowStore->setField(columnId, elemData, elemSize);
				}
			}
		}
	}
}

/*!
	@brief translate the field into Message format
*/
void TimeSeries::RowArray::Row::getFieldImage(TransactionContext &txn,
	ColumnInfo &columnInfo, uint32_t newColumnId,
	MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	BaseObject baseFieldObject(txn.getPartitionId(), objectManager);
	getField(txn, columnInfo, baseFieldObject);
	Value value;
	value.set(baseFieldObject.getCursor<uint8_t>(), columnInfo.getColumnType());
	ValueProcessor::getField(
		txn, objectManager, newColumnId, &value, messageRowStore);
}

TimeSeries::RowArray::RowArray(
	TransactionContext &txn, OId oId, TimeSeries *container, uint8_t getOption)
	: BaseObject(txn.getPartitionId(), *(container->getObjectManager())),
	  container_(container),
	  elemCursor_(getElemCursor(oId)) {
	if (getOption != OBJECT_READ_ONLY || getBaseOId() != getBaseOId(oId)) {
		BaseObject::load(oId, getOption);
	}
	rowSize_ = container_->getRowSize();
}

TimeSeries::RowArray::RowArray(TransactionContext &txn, TimeSeries *container)
	: BaseObject(txn.getPartitionId(), *(container->getObjectManager())),
	  container_(container),
	  elemCursor_(0) {
	rowSize_ = container_->getRowSize();
}

/*!
	@brief Get Object from Chunk
*/
void TimeSeries::RowArray::load(TransactionContext &txn, OId oId,
	TimeSeries *container, uint8_t getOption) {
	BaseObject::load(oId, getOption);
	elemCursor_ = getElemCursor(oId);
	container_ = container;
}

/*!
	@brief Allocate RowArray Object
*/
void TimeSeries::RowArray::initialize(
	TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum) {
	OId oId;
	BaseObject::allocate<uint8_t>(getBinarySize(maxRowNum),
		container_->getRowAllcateStrategy(), oId, OBJECT_TYPE_ROW_ARRAY);
	memset(getBaseAddr(), 0, HEADER_SIZE);
	setMaxRowNum(maxRowNum);
	setRowNum(0);
	setRowId(baseRowId);
	for (uint16_t i = 0; i < getMaxRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		row.reset();
	}
	elemCursor_ = 0;
}

/*!
	@brief Free Objects related to RowArray
*/
void TimeSeries::RowArray::finalize(TransactionContext &txn) {
	setDirty();
	for (begin(); !end(); next()) {
		TimeSeries::RowArray::Row row(getRow(), this);
		row.finalize(txn);
	}
	BaseObject::finalize();
}

/*!
	@brief Append Row to current cursor
*/
void TimeSeries::RowArray::append(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	Row row(getNewRow(), this);
	row.initialize();
	row.setRowId(rowId);
	row.setFields(txn, messageRowStore);
	setRowNum(getRowNum() + 1);
}

/*!
	@brief Insert Row to current cursor
*/
void TimeSeries::RowArray::insert(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	Row row(getRow(), this);
	row.initialize();
	row.setRowId(rowId);
	row.setFields(txn, messageRowStore);
	if (elemCursor_ >= getRowNum()) {
		setRowNum(elemCursor_ + 1);
	}
}

/*!
	@brief Update Row on current cursor
*/
void TimeSeries::RowArray::update(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	Row row(getRow(), this);
	row.updateFields(txn, messageRowStore);
}

/*!
	@brief Delete this Row on current cursor
*/
void TimeSeries::RowArray::remove(TransactionContext &txn) {
	Row row(getRow(), this);
	row.remove(txn);
	if (elemCursor_ == getRowNum() - 1) {
		setRowNum(elemCursor_);
	}
}

/*!
	@brief Move Row on current cursor to Another RowArray
*/
void TimeSeries::RowArray::move(TransactionContext &txn, RowArray &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.move(txn, destRow);
	updateCursor();
	dest.updateCursor();
}

/*!
	@brief Copy Row on current cursor to Another RowArray
*/
void TimeSeries::RowArray::copy(TransactionContext &txn, RowArray &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.copy(txn, destRow);
	updateCursor();
	dest.updateCursor();
}

void TimeSeries::RowArray::copyRowArray(
	TransactionContext &txn, RowArray &dest) {
	uint16_t currentCursor = elemCursor_;
	if (getContainer().getVariableColumnNum() > 0) {
		memcpy(dest.getAddr(), getAddr(), RowArray::HEADER_SIZE);
		for (begin(); !end(); next()) {
			Row row(getRow(), this);
			copy(txn, dest);
			dest.next();
		}
	}
	else {
		memcpy(dest.getAddr(), getAddr(), getBinarySize(getMaxRowNum()));
	}
	elemCursor_ = currentCursor;
}

/*!
	@brief Move to next RowArray, and Check if RowArray exists
*/
bool TimeSeries::RowArray::nextRowArray(
	TransactionContext &txn, RowArray &neighbor, uint8_t getOption) {
	uint16_t currentCursor = elemCursor_;
	tail();
	Row row(getRow(), this);
	RowId rowId = row.getRowId();
	BtreeMap::SearchContext sc(
		0, &rowId, 0, false, NULL, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			neighbor.load(txn, *itr, container_, getOption);
			elemCursor_ = currentCursor;
			return true;
		}
	}
	elemCursor_ = currentCursor;
	return false;
}

/*!
	@brief Move to prev RowArray, and Check if RowArray exists
*/
bool TimeSeries::RowArray::prevRowArray(
	TransactionContext &txn, RowArray &neighbor, uint8_t getOption) {
	uint16_t currentCursor = elemCursor_;
	begin();
	Row row(getRow(), this);
	RowId rowId = row.getRowId();
	BtreeMap::SearchContext sc(
		0, NULL, 0, false, &rowId, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			neighbor.load(txn, *itr, container_, getOption);
			elemCursor_ = currentCursor;
			return true;
		}
	}
	elemCursor_ = currentCursor;
	return false;
}

/*!
	@brief Search Row corresponding to RowId
*/
bool TimeSeries::RowArray::searchRowId(RowId rowId) {
	for (uint16_t i = 0; i < getRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		if (!row.isRemoved()) {
			RowId currentRowId = row.getRowId();
			if (currentRowId == rowId) {
				return true;
			}
			else if (currentRowId > rowId) {
				return false;
			}
		}
	}
	elemCursor_ = getRowNum();
	return false;
}

/*!
	@brief Lock this RowArray
*/
void TimeSeries::RowArray::lock(TransactionContext &txn) {
	if (getTxnId() == txn.getId()) {
	}
	else if (txn.getManager().isActiveTransaction(
				 txn.getPartitionId(), getTxnId())) {
		DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
			"(pId=" << txn.getPartitionId() << ", rowTxnId=" << getTxnId()
					<< ", txnId=" << txn.getId() << ")");
	}
	else {
		setLockTId(txn.getId());
	}
}

/*!
	@brief Shift Rows to next position
*/
void TimeSeries::RowArray::shift(TransactionContext &txn, bool isForce,
	util::XArray<std::pair<OId, OId> > &moveList) {
	if (isForce) {
		elemCursor_ = getMaxRowNum();
	}
	uint16_t insertPos = elemCursor_;
	uint16_t targetPos = getMaxRowNum();

	for (uint16_t i = insertPos; i < getMaxRowNum(); i++) {
		Row row(getRow(i), this);
		if (row.isRemoved()) {
			targetPos = i;
			break;
		}
	}
	if (targetPos != getMaxRowNum()) {
		for (uint16_t i = targetPos; i > insertPos; i--) {
			elemCursor_ = i - 1;
			Row row(getRow(), this);
			OId oldOId = getOId();
			elemCursor_ = i;
			Row destRow(getRow(), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			if (destRow.isRemoved()) {
				break;
			}
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		elemCursor_ = insertPos;
	}
	else {
		for (uint16_t i = insertPos + 1; i > 0; i--) {
			if (i >= getMaxRowNum()) {
				continue;
			}
			Row row(getRow(i - 1), this);
			if (row.isRemoved()) {
				targetPos = i - 1;
				break;
			}
		}
		for (uint16_t i = targetPos; i < insertPos; i++) {
			elemCursor_ = i;
			Row row(getRow(), this);
			if (!row.isRemoved()) {
				OId oldOId = getOId();
				elemCursor_ = i - 1;
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
		}
		elemCursor_ = insertPos - 1;
	}
	updateCursor();
}

/*!
	@brief Split this RowArray
*/
void TimeSeries::RowArray::split(TransactionContext &txn, RowId insertRowId,
	RowArray &splitRowArray, RowId splitRowId,
	util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t insertPos = elemCursor_;
	uint16_t midPos = getMaxRowNum() / 2;
	if (insertRowId < splitRowId) {
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			elemCursor_ = i;
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		for (uint16_t i = midPos; i > insertPos; i--) {
			elemCursor_ = i - 1;
			Row row(getRow(), this);
			OId oldOId = getOId();
			elemCursor_ = i;
			Row destRow(getRow(i), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		elemCursor_ = insertPos;
	}
	else {
		uint16_t destCursor = getMaxRowNum() - midPos;
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			elemCursor_ = i;
			if (i == insertPos) {
				destCursor = splitRowArray.elemCursor_;
				splitRowArray.updateCursor();
				splitRowArray.next();
			}
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		splitRowArray.elemCursor_ = destCursor;
		elemCursor_ = midPos - 1;
	}
	updateCursor();
	splitRowArray.updateCursor();
}

/*!
	@brief Merge this RowArray and another RowArray
*/
void TimeSeries::RowArray::merge(TransactionContext &txn,
	RowArray &nextRowArray, util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t pos = 0;
	for (uint16_t i = 0; i < getRowNum(); i++) {
		elemCursor_ = i;
		Row row(getRow(), this);
		if (!row.isRemoved()) {
			if (pos != i) {
				OId oldOId = getOId();
				elemCursor_ = pos;
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
			pos++;
		}
	}
	for (nextRowArray.begin(); !nextRowArray.end(); nextRowArray.next()) {
		elemCursor_ = pos;
		OId oldOId = nextRowArray.getOId();
		nextRowArray.move(txn, *this);
		OId newOId = getOId();
		moveList.push_back(std::make_pair(oldOId, newOId));
		pos++;
	}
	updateCursor();
	nextRowArray.updateCursor();
}

std::string TimeSeries::RowArray::dump(TransactionContext &txn) {
	uint16_t pos = elemCursor_;
	util::NormalOStringStream strstrm;
	strstrm << "RowId," << getRowId() << ",MaxRowNum," << getMaxRowNum()
			<< ",RowNum," << getActiveRowNum() << ",TxnId," << this->getTxnId()
			<< std::endl;
	strstrm << "ChunkId,Offset,ElemNum" << std::endl;
	for (begin(); !end(); next()) {
		Row row(getRow(), this);
		OId oId = getOId();
		strstrm << ObjectManager::getChunkId(oId) << ","
				<< ObjectManager::getOffset(oId) << ","
				<< ObjectManager::getChunkId(oId) << "," << getElemCursor(oId)
				<< ",";
		strstrm << row.dump(txn) << std::endl;
	}
	elemCursor_ = pos;
	return strstrm.str();
}

std::string TimeSeries::RowArray::Row::dump(TransactionContext &txn) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	util::NormalOStringStream strstrm;
	ContainerValue containerValue(txn, objectManager);
	for (uint32_t i = 0; i < rowArrayCursor_->getContainer().getColumnNum();
		 i++) {
		getField(txn, rowArrayCursor_->getContainer().getColumnInfo(i),
			containerValue);
		strstrm << containerValue.getValue().dump(txn, objectManager) << ", ";
	}
	return strstrm.str();
}
