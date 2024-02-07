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
	@brief Implementation of BtreeMap
*/
#include "base_index.h"
#include "schema.h"

BaseIndex::SearchContextPool::SearchContextPool(
		util::StackAllocator &alloc, util::StackAllocator &settingAlloc) :
		condValuePool_(alloc),
		indexData_(createIndexData(alloc)),
		settingAlloc_(settingAlloc) {
}

IndexData& BaseIndex::SearchContextPool::createIndexData(
		util::StackAllocator &alloc) {
	IndexData *indexData = ALLOC_NEW(alloc) IndexData(alloc);
	return *indexData;
}

void BaseIndex::SearchContextPool::clearIndexData(IndexData &indexData) {
	indexData.clear();
}

void BaseIndex::SearchContext::getConditionList(
	util::Vector<TermCondition> &condList, ConditionType condType) {
	util::Vector<size_t>::iterator itr;
	if (condType == COND_KEY) {
		for (itr = keyList_.begin(); itr != keyList_.end(); itr++) {
			condList.push_back(getCondition(*itr));
		}
	} else {
		for (size_t i = 0; i < conditionList_.size(); i++) {
			if (condType == COND_ALL) {
				condList.push_back(conditionList_[i]);
			} else {
				itr = std::find(keyList_.begin(), keyList_.end(), i);
				if (itr == keyList_.end() && condType == COND_OTHER) {
					condList.push_back(conditionList_[i]);
				}
			}
		}
	}
}

bool BaseIndex::SearchContext::updateCondition(TransactionContext &txn, 
	TermCondition &newCond) {
	bool isNoReplacement = true;
	ConditionStatus status = ConditionStatus::NO_CONTRADICTION;
	util::Vector<TermCondition>::iterator itr;
	for (itr = conditionList_.begin(); itr != conditionList_.end(); itr++) {
		TermCondition &current = *itr;
		status = checkCondition(txn, current, newCond);
		if (status == ConditionStatus::REPLACEMENT) {
			current = newCond;
			isNoReplacement = false;
			break;
		} else if (status == ConditionStatus::CONTRADICTION) {
			conditionList_.push_back(newCond);
			break;
		} else if (status == ConditionStatus::IGNORANCE) {
			break;
		}
	}

	if (isNoReplacement && status == NO_CONTRADICTION) {
		util::Vector<ColumnId>::iterator columnIdItr;
		columnIdItr = std::find(columnIdList_.begin(), columnIdList_.end(), newCond.columnId_);
		if (columnIdItr != columnIdList_.end()) {
			keyList_.push_back(static_cast<ColumnId>(conditionList_.size()));
		}
		conditionList_.push_back(newCond);
	}
	return status == ConditionStatus::REPLACEMENT;
}

void BaseIndex::SearchContext::copy(util::StackAllocator &alloc, SearchContext &dest) {
	util::Vector<TermCondition>::iterator itr;
	dest.setNullCond(nullCond_); 
	for (itr = conditionList_.begin(); itr != conditionList_.end(); itr++) {
		TermCondition destCond;
		itr->copy(alloc, destCond);
		dest.conditionList_.push_back(destCond);
	}
	dest.keyList_.assign(keyList_.begin(), keyList_.end());
	dest.limit_ = limit_;
	dest.isResume_ = isResume_;
	dest.columnIdList_.assign(columnIdList_.begin(), columnIdList_.end());

	dest.setSuspended(isSuspended_);
	dest.setSuspendLimit(suspendLimit_);

	uint8_t *newSuspendKey = NULL;
	if (suspendKey_ != NULL) {
		newSuspendKey = ALLOC_NEW(alloc) uint8_t[suspendKeySize_];
		memcpy(newSuspendKey, suspendKey_, suspendKeySize_);
	}
	dest.setSuspendKey(newSuspendKey);
	dest.setSuspendKeySize(suspendKeySize_);

	uint8_t *newSuspendValue = NULL;
	if (suspendValue_ != NULL) {
		newSuspendValue = ALLOC_NEW(alloc) uint8_t[suspendValueSize_];
		memcpy(newSuspendValue, suspendValue_, suspendValueSize_);
	}
	dest.setSuspendValue(newSuspendValue);
	dest.setSuspendValueSize(suspendValueSize_);

	dest.setNullSuspended(isNullSuspended_);
	dest.setSuspendRowId(suspendRowId_);
}

std::string BaseIndex::SearchContext::dump() {
	util::NormalOStringStream strstrm;
	strstrm << "\"BaseIndex_SearchContext\" : {";

	strstrm << "[";
	for (size_t i = 0; i < conditionList_.size(); i++) {
		if (i != 0) {
			strstrm << ",";
		}
		strstrm << conditionList_[i].dump();
	}
	strstrm << "]";

	strstrm << ",[";
	for (size_t i = 0; i < keyList_.size(); i++) {
		if (i != 0) {
			strstrm << ",";
		}
		strstrm << keyList_[i];
	}
	strstrm << "]";
	strstrm << ",[";
	for (size_t i = 0; i < columnIdList_.size(); i++) {
		if (i != 0) {
			strstrm << ",";
		}
		strstrm << columnIdList_[i];
	}
	strstrm << "]";
	strstrm << ", {\"isResume_\" : " << (int)isResume_ << "}";
	strstrm << ", {\"nullCond_\" : " << nullCond_ << "}";
	strstrm << ", {\"limit_\" : " << limit_ << "}";

	strstrm << ", {\"isSuspended_\" : " << (int)isSuspended_ << "}";
	strstrm << ", {\"isNullSuspended_\" : " << (int)isNullSuspended_ << "}";
	strstrm << ", {\"suspendRowId_\" : " << suspendRowId_ << "}";
	strstrm << ", {\"suspendLimit_\" : " << suspendLimit_ << "}";

	strstrm << ", {\"suspendKey_\" : ";
	strstrm << ", \"no impl\"";
	strstrm << "}";
	strstrm << ", {\"suspendKeySize_\" : " << suspendKeySize_ << "}";

	strstrm << ", {\"suspendValue_\" : ";
	strstrm << ", \"no impl\"";
	strstrm << "}";
	strstrm << ", {\"suspendValueSize_\" : " << suspendValueSize_ << "}";

	strstrm << "}";
	return strstrm.str();
}


BaseIndex::Setting::Setting(ColumnType keyType, bool isCaseSensitive, TreeFuncInfo *funcInfo)
	: keyType_(keyType), isCaseSensitive_(isCaseSensitive), 
	isStartIncluded_(true), isEndIncluded_(true), compareNum_(0), 
	greaterCompareNum_(0), lessCompareNum_(0), funcInfo_(funcInfo),
	startCondition_(NULL), endCondition_(NULL), filterConds_(NULL) {
	ColumnSchema *columnSchema = funcInfo_ != NULL ? funcInfo_->getColumnSchema() : NULL;
	if (columnSchema == NULL || columnSchema->getColumnNum() == 1) {
		compareNum_ = lessCompareNum_ = greaterCompareNum_ = 1;
	} else {
		compareNum_ = lessCompareNum_ = greaterCompareNum_ = columnSchema->getColumnNum();
	}
}

void BaseIndex::Setting::initialize(
		util::StackAllocator &alloc, SearchContext &sc,
		OutputOrder outputOrder, bool *initializedForNext) {
	if (initializedForNext != NULL) {
		*initializedForNext = false;
	}

	const size_t UNDEF_POS = SIZE_MAX;
	ColumnSchema *columnSchema = funcInfo_ != NULL ? funcInfo_->getColumnSchema() : NULL;
	if (columnSchema == NULL || columnSchema->getColumnNum() == 1) {
		lessCompareNum_ = greaterCompareNum_ = 1;
		const util::Vector<size_t> &keyList = sc.getKeyList();
		for (util::Vector<size_t>::const_iterator itr = keyList.begin(); 
			itr != keyList.end(); itr++) {
			if (sc.getCondition(*itr).isStartCondition()) {
				startCondition_ = &(sc.getCondition(*itr));
				isStartIncluded_ = DSExpression::isIncluded(startCondition_->opType_);
			} else
			if (sc.getCondition(*itr).isEndCondition()) {
				endCondition_ = &(sc.getCondition(*itr));
				isEndIncluded_ = DSExpression::isIncluded(endCondition_->opType_);
			} else
			{
				assert(false);
			}
		}
		bool arranged = false;
		if (sc.getSuspendKey() != NULL) {
			if (startCondition_ != NULL && startCondition_->opType_ == DSExpression::EQ) {
			} else {
				if (outputOrder != ORDER_DESCENDING) {
					startCondition_ = ALLOC_NEW(alloc) TermCondition(
						keyType_, keyType_,
						DSExpression::GE, sc.getScColumnId(),
						sc.getSuspendKey(), sc.getSuspendKeySize());
				} else {
					endCondition_ = ALLOC_NEW(alloc) TermCondition(
						keyType_, keyType_, 
						DSExpression::LE, sc.getScColumnId(),
						sc.getSuspendKey(), sc.getSuspendKeySize());
				}
				arranged = true;
			}
		}
		if (!arranged && initializedForNext != NULL) {
			*initializedForNext = true;
		}
	} else {
		TermCondition *restartCond = NULL;
		if (sc.getSuspendKey() != NULL) {
			CompositeInfoObject *suspendKey = funcInfo_->createCompositeInfo(alloc, sc.getSuspendKey(), sc.getSuspendKeySize());
			if (outputOrder != ORDER_DESCENDING) {
				restartCond = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					DSExpression::GE, UNDEF_COLUMNID,
					suspendKey, funcInfo_->getFixedAreaSize());
			} else {
				restartCond = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					DSExpression::LE, UNDEF_COLUMNID,
					suspendKey, funcInfo_->getFixedAreaSize());
			}
		}
		util::Vector<size_t> eqPosList(alloc);
		size_t greaterPos = UNDEF_POS;
		size_t lessPos = UNDEF_POS;
		util::Vector<ColumnId> *orgColumnIds = funcInfo_->getOrgColumnIds();
		for (size_t i = 0; i < orgColumnIds->size(); i++) { 
			size_t eqPos = UNDEF_POS;
			const util::Vector<size_t> &keyList = sc.getKeyList();
			for (util::Vector<size_t>::const_iterator itr = keyList.begin(); 
				itr != keyList.end(); itr++) {
				if (sc.getCondition(*itr).columnId_ == (*orgColumnIds)[i]) {
					if (sc.getCondition(*itr).opType_ == DSExpression::EQ) {
						eqPos = *itr;
						break;
					} else if (sc.getCondition(*itr).isStartCondition()) {
						greaterPos = *itr;
					} else if (sc.getCondition(*itr).isEndCondition()) {
						lessPos = *itr;
					}
				}
			}
			if (eqPos != UNDEF_POS && (greaterPos != UNDEF_POS || lessPos  != UNDEF_POS)) {
				assert(false);
			}
			if (eqPos != UNDEF_POS) {
				eqPosList.push_back(eqPos);
				greaterPos = UNDEF_POS;
				lessPos = UNDEF_POS;
			} else {
				break;
			}
		}


		if (greaterPos == UNDEF_POS && lessPos == UNDEF_POS && !eqPosList.empty()) {
			util::XArray<KeyData> valueList(alloc);
			for (util::Vector<size_t>::iterator itr = eqPosList.begin();
				itr != eqPosList.end(); itr++) {
				valueList.push_back(
					KeyData(sc.getCondition(*itr).value_, 
					sc.getCondition(*itr).valueSize_));
			}
			CompositeInfoObject *object = funcInfo_->createCompositeInfo(
				alloc, valueList);
			if (restartCond != NULL) {
				if (outputOrder != ORDER_DESCENDING) {
					startCondition_ =restartCond;
					endCondition_ = ALLOC_NEW(alloc) TermCondition(
						COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
						DSExpression::LE, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
					greaterCompareNum_ = columnSchema->getColumnNum();
					lessCompareNum_ = eqPosList.size();
				} else {
					startCondition_ = ALLOC_NEW(alloc) TermCondition(
						COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
						DSExpression::GE, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
					endCondition_ = restartCond;
					greaterCompareNum_ = eqPosList.size();
					lessCompareNum_ = columnSchema->getColumnNum();
				}
			} else {
				startCondition_ = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					DSExpression::EQ, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
				lessCompareNum_ = greaterCompareNum_ = eqPosList.size();
			}
		} else {
			bool isGreaterRewrite = false;
			bool isLessRewrite = false;
			if (restartCond != NULL) {
				if (outputOrder != ORDER_DESCENDING) {
					startCondition_ = restartCond;
					greaterCompareNum_ = columnSchema->getColumnNum();
					isGreaterRewrite = true;
				} else {
					endCondition_ = restartCond;
					lessCompareNum_ = columnSchema->getColumnNum();
					isLessRewrite = true;
				}
			}
			if ((greaterPos != UNDEF_POS || !eqPosList.empty()) && !isGreaterRewrite) {
				greaterCompareNum_ = 0;
				util::XArray<KeyData> valueList(alloc);
				for (util::Vector<size_t>::iterator itr = eqPosList.begin();
					itr != eqPosList.end(); itr++) {
					valueList.push_back(
						KeyData(sc.getCondition(*itr).value_, 
						sc.getCondition(*itr).valueSize_));
					greaterCompareNum_++;
				}
				DSExpression::Operation opType = DSExpression::GE;
				if (greaterPos != UNDEF_POS) {
					const TermCondition &lastCond = sc.getCondition(greaterPos);
					opType = lastCond.opType_;
					valueList.push_back(
						KeyData(lastCond.value_, lastCond.valueSize_));
					greaterCompareNum_++;
				}
				CompositeInfoObject *object = funcInfo_->createCompositeInfo(
					alloc, valueList);
				startCondition_ = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					opType, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
			}
			if ((lessPos != UNDEF_POS || !eqPosList.empty()) && !isLessRewrite) {
				lessCompareNum_ = 0;
				util::XArray<KeyData> valueList(alloc);
				for (util::Vector<size_t>::iterator itr = eqPosList.begin();
					itr != eqPosList.end(); itr++) {
					valueList.push_back(
						KeyData(sc.getCondition(*itr).value_, 
						sc.getCondition(*itr).valueSize_));
					lessCompareNum_++;
				}
				DSExpression::Operation opType = DSExpression::LE;
				if (lessPos != UNDEF_POS) {
					const TermCondition &lastCond = sc.getCondition(lessPos);
					opType = lastCond.opType_;
					valueList.push_back(
						KeyData(lastCond.value_, lastCond.valueSize_));
					lessCompareNum_++;
				}
				CompositeInfoObject *object = funcInfo_->createCompositeInfo(
					alloc, valueList);
				endCondition_ = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					opType, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
			}
			if (greaterPos != UNDEF_POS && lessPos == UNDEF_POS && !isLessRewrite) {
				util::XArray<KeyData> valueList(alloc);
				for (util::Vector<size_t>::iterator itr = eqPosList.begin();
					itr != eqPosList.end(); itr++) {
					valueList.push_back(
						KeyData(sc.getCondition(*itr).value_, 
						sc.getCondition(*itr).valueSize_));
				}
				valueList.push_back(
					KeyData(NULL, 0));
				CompositeInfoObject *object = funcInfo_->createCompositeInfo(
					alloc, valueList);
				endCondition_ = ALLOC_NEW(alloc) TermCondition(
					COLUMN_TYPE_COMPOSITE, COLUMN_TYPE_COMPOSITE, 
					DSExpression::LT, UNDEF_COLUMNID, object, funcInfo_->getFixedAreaSize());
				lessCompareNum_ = eqPosList.size() + 1;
			}
		}

		if (startCondition_ != NULL) {
			isStartIncluded_ = DSExpression::isIncluded(startCondition_->opType_);
		}
		if (endCondition_ != NULL) {
			isEndIncluded_ = DSExpression::isIncluded(endCondition_->opType_);
		}

		filterConds_ = ALLOC_NEW(alloc) util::Vector<TermCondition>(alloc);

		const util::Vector<size_t> &keyList = sc.getKeyList();
		for (util::Vector<size_t>::const_iterator itr = keyList.begin(); 
			itr != keyList.end(); itr++) {
			if (greaterPos != *itr && lessPos != *itr && 
				std::find(eqPosList.begin(), eqPosList.end(), *itr) == eqPosList.end()) {
				TermCondition filterCond = sc.getCondition(*itr);
				filterCond.columnId_ = funcInfo_->getColumnId(filterCond.columnId_);
				filterConds_->push_back(filterCond);
			}
		}

	}
}

CompositeInfoObject *TreeFuncInfo::createCompositeInfo(util::StackAllocator &alloc,
	util::XArray<KeyData> &valueList) {
	CompositeInfoObject *compositeInfo = allocateCompositeInfo(alloc);
	ColumnSchema *columnSchema = getColumnSchema();
	assert(valueList.size() <= columnSchema->getColumnNum());

	util::XArray<const void *> varList(alloc);
	util::XArray<uint32_t> varSizeList(alloc);
	uint32_t totalVariableSize = 0;
	for (size_t i = 0; i < valueList.size(); i++) {
		ColumnInfo& columnInfo = columnSchema->getColumnInfo(i);

		const void *fieldValue = valueList[i].data_;
		uint32_t fieldSize = valueList[i].size_;
		if (fieldValue != NULL) {
			if (columnInfo.isVariable()) {
				uint32_t encodeSize = ValueProcessor::getEncodedVarSize(fieldSize);
				totalVariableSize += encodeSize + fieldSize;
				varList.push_back(fieldValue);
				varSizeList.push_back(fieldSize);
			} else {
				compositeInfo->setFixedField(*this, i, fieldValue);
			}
		} else {
			compositeInfo->setNull(i);
			if (columnInfo.isVariable()) {
				const void *defaultValue = Value::getDefaultVariableValue(columnInfo.getColumnType());
				uint32_t encodeSize = ValueProcessor::getEncodedVarSize(defaultValue);
				uint32_t varSize = ValueProcessor::decodeVarSize(defaultValue);
				totalVariableSize += encodeSize + varSize;
				varList.push_back(static_cast<const uint8_t *>(fieldValue) + encodeSize);
				varSizeList.push_back(varSize);
			}
		}
	}

	for (size_t i = valueList.size(); i < columnSchema->getColumnNum(); i++) {
		compositeInfo->setNull(i);
	}

	if (compositeInfo->hasVariable()) {
		uint32_t variableColumnNum = columnSchema->getVariableColumnNum();
		uint64_t encodedVariableColumnNum =
			ValueProcessor::encodeVarSize(variableColumnNum);
		uint32_t encodedVariableColumnNumLen =
			ValueProcessor::getEncodedVarSize(variableColumnNum);

		uint8_t *variableArray = static_cast<uint8_t *>(alloc.allocate(totalVariableSize + encodedVariableColumnNumLen));
		uint8_t *destAddr = variableArray;
		memcpy(destAddr, &encodedVariableColumnNum,
			encodedVariableColumnNumLen);
		destAddr += encodedVariableColumnNumLen;

		for (size_t i = 0; i < varList.size(); i++) {
			const void *addr = varList[i];
			uint32_t varSize = varSizeList[i];
			uint32_t encodeSizeLen = ValueProcessor::getEncodedVarSize(varSize);
			uint32_t encodeSize = ValueProcessor::encodeVarSize(varSize);
			memcpy(destAddr, &encodeSize, encodeSizeLen);
			memcpy(destAddr + encodeSizeLen, addr, varSize);
			destAddr += encodeSizeLen + varSize;
		}
#ifdef WIN32
		uint64_t addr = *reinterpret_cast<uint32_t *>(&variableArray);
		compositeInfo->setVariableArray(&addr); 
#else
		compositeInfo->setVariableArray(&variableArray);
#endif
	}
	return compositeInfo;
}


void TreeFuncInfo::initialize(const util::Vector<ColumnId> &columnIds, const ColumnSchema *srcSchema, bool force) {
	assert(!columnIds.empty());
	orgColumnIds_ = ALLOC_NEW(alloc_) util::Vector<ColumnId>(alloc_);
	orgColumnIds_->assign(columnIds.begin(), columnIds.end());
	UNUSED_VARIABLE(force);
	if (columnIds.size() != 1) {
		columnSchema_ = ALLOC_NEW(alloc_) ColumnSchema();
		columnSchema_ = static_cast<ColumnSchema *>(alloc_.allocate(ColumnSchema::getAllocateSize(columnIds.size(), 0)));

		columnSchema_->initialize(columnIds.size());
		columnSchema_->set(alloc_, srcSchema, columnIds);

		uint32_t fixedColumnsSize = columnSchema_->getRowFixedColumnSize();
		bool hasVariable = columnSchema_->getVariableColumnNum() > 0;
		fixedAreaSize_ = CompositeInfoObject::calcSize(columnSchema_->getColumnNum(), fixedColumnsSize, hasVariable);
	}
}

CompositeInfoObject *TreeFuncInfo::allocateCompositeInfo(util::StackAllocator &alloc) {
	ColumnSchema *columnSchema = getColumnSchema();
	uint32_t fixedColumnsSize = columnSchema->getRowFixedColumnSize();
	bool hasVariable = columnSchema->getVariableColumnNum() > 0;
	CompositeInfoObject *compositeInfo = static_cast<CompositeInfoObject *>(
		alloc.allocate(CompositeInfoObject::calcSize(columnSchema->getColumnNum(), 
		fixedColumnsSize, hasVariable)));
	compositeInfo->initialize(alloc, *this);
	return compositeInfo;
}

CompositeInfoObject *TreeFuncInfo::createCompositeInfo(util::StackAllocator &alloc, void *data, uint32_t size) {
	UNUSED_VARIABLE(size);
	CompositeInfoObject *compositeInfo = allocateCompositeInfo(alloc);

	memcpy(compositeInfo, data, getFixedAreaSize());
	if (compositeInfo->hasVariable()) {
		uint8_t *variableArray = static_cast<uint8_t *>(data) + getFixedAreaSize();
#ifdef WIN32
		uint64_t addr = *reinterpret_cast<uint32_t *>(&variableArray);
		compositeInfo->setVariableArray(&addr); 
#else
		compositeInfo->setVariableArray(&variableArray);
#endif
	}
	return compositeInfo;
}

ColumnId TreeFuncInfo::getColumnId(ColumnId orgColumnId) {
	util::Vector<ColumnId>::iterator itr;
	itr = std::find(getOrgColumnIds()->begin(), getOrgColumnIds()->end(), orgColumnId);
	assert(itr != getOrgColumnIds()->end());
	return static_cast<ColumnId>(itr - getOrgColumnIds()->begin());
}


const uint8_t *CompositeInfoObject::getNullsAddr() const {
	const uint8_t *top = reinterpret_cast<const uint8_t *>(this);
	return (top + HEADER_SIZE);
}

const uint8_t *CompositeInfoObject::getVarAddr() const {
	const uint8_t *top = reinterpret_cast<const uint8_t *>(this);
	return (top + HEADER_SIZE + getNullBitSize());
}

const uint8_t *CompositeInfoObject::getFixedAddr() const {
	const uint8_t *top = reinterpret_cast<const uint8_t *>(this);
	return (top + HEADER_SIZE);
}


uint8_t CompositeInfoObject::getType() const {
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	uint8_t type = ((*header & 0x80) >> 7);
	return type;
}

void CompositeInfoObject::setType(uint8_t type) {
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	*header &= 0x7F; 
	*header |= (type << 7);
}

uint8_t CompositeInfoObject::getNullBitSize() const {
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	uint8_t bitSize = ((*header & 0x60) >> 6);
	return bitSize + 1;
}

void CompositeInfoObject::setNullBitSize(uint16_t columnNum) {
	uint8_t bitSize = static_cast<uint8_t>(ValueProcessor::calcNullsByteSize(columnNum) - 1);
	assert(bitSize < 4); 
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	*header &= 0x9F; 
	*header |= (bitSize << 6);
}

bool CompositeInfoObject::hasVariable() const {
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	uint8_t variable = (*header & 0x10);
	return variable != 0;
}

void CompositeInfoObject::setVariable(bool isVariable) {
	uint8_t *header = reinterpret_cast<uint8_t *>(const_cast<CompositeInfoObject *>(this));
	*header &= 0xEF; 
	if (isVariable) {
		*header |= (0x10);
	}
}

void CompositeInfoObject::setVariableArray(void *value) {
	uint8_t *addr = const_cast<uint8_t *>(getVarAddr());
	memcpy(addr, value, sizeof(OId));
}

void CompositeInfoObject::setKey(TransactionContext &txn, ObjectManagerV4 &objectManager, 
								 AllocateStrategy &allocateStrategy, OId neighborOId,
								 TreeFuncInfo *funcInfo, CompositeInfoObject &src) {
	assert(funcInfo != NULL);
	memcpy(this, &src, funcInfo->getFixedAreaSize());
	if (src.hasVariable()) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		OId variableOId = *reinterpret_cast<const OId *>(src.getVarAddr());
		uint8_t *memoryAddr = reinterpret_cast<uint8_t *>(variableOId);
		VariableArrayCursor cursor(memoryAddr);
		util::XArray< std::pair<uint8_t *, uint32_t> > varList(alloc);
		util::XArray<ColumnType> columnTypeList(alloc);
		while (cursor.nextElement()) {
			uint32_t elemSize, elemCount;
			uint8_t *value = cursor.getElement(elemSize, elemCount);
			varList.push_back(std::make_pair(value, elemSize));
			columnTypeList.push_back(COLUMN_TYPE_STRING);
		}

		bool isConvertSpecialType = true;
		util::XArray<uint32_t> varDataObjectSizeList(
			alloc);  
		util::XArray<uint32_t> varDataObjectPosList(
			alloc);  
		VariableArrayCursor::checkVarDataSize(txn, objectManager,
			varList, columnTypeList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList);
		util::XArray<OId> oldVarDataOIdList(alloc);
		variableOId = VariableArrayCursor::createVariableArrayCursor(
			txn, objectManager, allocateStrategy,
			varList, columnTypeList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList, oldVarDataOIdList, neighborOId);

		setVariableArray(&variableOId);
	}
	setType(KEY_ON_INDEX);
}

void CompositeInfoObject::freeKey(ObjectManagerV4 &objectManager, AllocateStrategy &strategy) {
	if (hasVariable()) {
		OId oId = *reinterpret_cast<const OId *>(getVarAddr());
		VariableArrayCursor cursor(objectManager, strategy, oId, OBJECT_READ_ONLY);
		cursor.finalize();
	}
}

bool CompositeInfoObject::isNull(uint32_t pos) const {
	const uint8_t *addr = getNullsAddr();
	if (addr != NULL) {
		return RowNullBits::isNullValue(addr, pos);
	} else {
		return false;
	}
}

void CompositeInfoObject::setNull(uint32_t pos) {
	uint8_t *addr = const_cast<uint8_t *>(getNullsAddr());
	RowNullBits::setNull(addr, pos);
}

uint8_t *CompositeInfoObject::getField(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnInfo &columnInfo, VariableArrayCursor *&varCursor) const {
	if (columnInfo.isVariable()) {
		if (varCursor == NULL) {
			OId variableOId = *reinterpret_cast<const OId *>(getVarAddr());
			if (getType() == KEY_ON_INDEX) {
				varCursor = ALLOC_NEW(txn.getDefaultAllocator()) VariableArrayCursor(objectManager, strategy, variableOId, OBJECT_READ_ONLY);
			} else {
				uint8_t *memoryAddr = reinterpret_cast<uint8_t *>(variableOId);
				varCursor = ALLOC_NEW(txn.getDefaultAllocator()) VariableArrayCursor(memoryAddr);
			}
		}
		varCursor->moveElement(columnInfo.getColumnOffset());
		uint32_t elemSize, elemCount;
		return varCursor->getElement(elemSize, elemCount);
	} else {
		return const_cast<uint8_t *>(getFixedAddr() + columnInfo.getColumnOffset());
	}
}

uint8_t *CompositeInfoObject::getField(ColumnInfo &columnInfo) const {
	if (columnInfo.isVariable()) {
		OId variableOId = *reinterpret_cast<const OId *>(getVarAddr());
		if (getType() == KEY_ON_INDEX) {
			assert(false);
			return NULL;
		} else {
			uint8_t *memoryAddr = reinterpret_cast<uint8_t *>(variableOId);
			VariableArrayCursor cursor(memoryAddr);
			for (uint32_t i = 0; i < columnInfo.getColumnOffset() + 1; i++) {
				cursor.nextElement();
			}
			uint32_t elemSize, elemCount;
			return cursor.getElement(elemSize, elemCount);
		}
	} else {
		return const_cast<uint8_t *>(getFixedAddr() + columnInfo.getColumnOffset());
	}
}

void CompositeInfoObject::setFixedField(TreeFuncInfo &funcInfo, uint32_t pos, const void *value) {
	ColumnSchema *columnSchema = funcInfo.getColumnSchema();
	ColumnInfo &columnInfo = columnSchema->getColumnInfo(pos);
	uint8_t *addr = const_cast<uint8_t *>(getFixedAddr() + columnInfo.getColumnOffset());
	memcpy(addr, value, columnInfo.getColumnSize());
}

uint32_t CompositeInfoObject::calcSize(uint32_t columnNum, uint32_t fixedColumnsSize, bool hasVariable) {
	uint32_t fixedSize = HEADER_SIZE + ValueProcessor::calcNullsByteSize(columnNum) + fixedColumnsSize;
	if (hasVariable) {
		fixedSize += sizeof(OId);
	}
	if ((fixedSize & 0x7) != 0) {
		fixedSize &= 0xFFFFFFF8;
		fixedSize += 0x8;
	}
	return fixedSize;
}

void CompositeInfoObject::initialize(util::StackAllocator &alloc, TreeFuncInfo &funcInfo) {
	UNUSED_VARIABLE(alloc);
	ColumnSchema *columnSchema = funcInfo.getColumnSchema();
	bool hasVariable = columnSchema->getVariableColumnNum() > 0;

	uint32_t allocSize = funcInfo.getFixedAreaSize();

	memset(this, 0, allocSize);
	setType(KEY_ON_MEMORY);
	setVariable(hasVariable);
	setNullBitSize(columnSchema->getColumnNum());
}

void CompositeInfoObject::serialize(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	TreeFuncInfo &funcInfo, uint8_t *&data, uint32_t &size) const {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	ColumnSchema *columnSchema = funcInfo.getColumnSchema();

	uint32_t fixedColumnsSize = columnSchema->getRowFixedColumnSize();
	uint32_t fixedAllocSize = calcSize(columnSchema->getColumnNum(), fixedColumnsSize, hasVariable());
	if (hasVariable()) {
		uint32_t varColumnNumEncodeSize = ValueProcessor::getEncodedVarSize(columnSchema->getVariableColumnNum());
		uint32_t variableSize = varColumnNumEncodeSize;
		OId variableOId = *reinterpret_cast<const OId *>(getVarAddr());
		StackAllocAutoPtr<VariableArrayCursor> cursor(alloc);
		if (getType() == KEY_ON_INDEX) {
			cursor.set(ALLOC_NEW(alloc) VariableArrayCursor(objectManager, strategy, variableOId, OBJECT_READ_ONLY));
		} else {
			uint8_t *memoryAddr = reinterpret_cast<uint8_t *>(variableOId);
			cursor.set(ALLOC_NEW(alloc) VariableArrayCursor(memoryAddr));
		}
		VariableArrayCursor *cursorPtr = cursor.get();
		while (cursorPtr->nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			cursorPtr->getElement(elemSize, elemNth);
			variableSize += elemSize + ValueProcessor::getEncodedVarSize(elemSize);
		}
		size = fixedAllocSize + variableSize;
		data = static_cast<uint8_t *>(alloc.allocate(size));
		memcpy(data, this, fixedAllocSize);

		uint8_t *dataCursor = data + fixedAllocSize;
		cursorPtr->reset();
		memcpy(dataCursor, cursorPtr->getBaseAddr(), varColumnNumEncodeSize);
		dataCursor += varColumnNumEncodeSize;
		while (cursorPtr->nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			uint8_t *elem = cursorPtr->getElement(elemSize, elemNth);
			uint32_t elemEncodeSize = ValueProcessor::getEncodedVarSize(elemSize);
			memcpy(dataCursor, elem, elemSize + elemEncodeSize);
			dataCursor += elemSize + elemEncodeSize;
		}
	} else {
		size = fixedAllocSize;
		data = static_cast<uint8_t *>(alloc.allocate(size));
		memcpy(data, this, fixedAllocSize);
	}
	reinterpret_cast<CompositeInfoObject *>(data)->setType(KEY_ON_MEMORY);
}

void CompositeInfoObject::dump(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy, TreeFuncInfo &funcInfo, util::NormalOStringStream &output) const {
	output << "(";
	ColumnSchema *columnSchema = funcInfo.getColumnSchema();
	VariableArrayCursor *cursor = NULL;

	for (uint32_t pos = 0; pos < columnSchema->getColumnNum(); pos++) {
		if (pos != 0) {
			output << ",";
		}
		ColumnInfo &columnInfo = columnSchema->getColumnInfo(pos);
		Value val;
		if (isNull(pos)) {
			val.init(COLUMN_TYPE_NULL);
		} else {
			uint8_t *data = getField(txn, objectManager, strategy, columnInfo, cursor);
			val.set(data, columnInfo.getColumnType());
		}
		val.dump(txn, objectManager, strategy, output);
	}
	output << ")";
	if (cursor != NULL) {
		cursor->~VariableArrayCursor();
	}
}

void CompositeInfoObject::dump(TreeFuncInfo &funcInfo, util::NormalOStringStream &output) const {
	output << "(";
	ColumnSchema *columnSchema = funcInfo.getColumnSchema();
	for (uint32_t pos = 0; pos < columnSchema->getColumnNum(); pos++) {
		if (pos != 0) {
			output << ",";
		}
		ColumnInfo &columnInfo = columnSchema->getColumnInfo(pos);
		if (columnInfo.isVariable() && getType() == KEY_ON_INDEX) {
			output << "(This function can not access DB file, call another function)";
			continue;
		}

		Value val;
		if (isNull(pos)) {
			val.init(COLUMN_TYPE_NULL);
		} else {
			uint8_t *data = getField(columnInfo);
			val.set(data, columnInfo.getColumnType());
		}
		ValueProcessor::dumpSimpleValue(output, val.getType(), val.getImage(), val.size(), false);
	}
	output << ")";
}

std::string BaseIndex::Setting::dump()
{
	util::NormalOStringStream strstrm;
	strstrm << "\"BaseIndex_Setting\" : {";
	strstrm << ", {\"isCaseSensitive_\" : " << (int)isCaseSensitive_ << "}";
	strstrm << ", {\"compareNum_\" : " << compareNum_ << "}";
	strstrm << ", {\"greaterCompareNum_\" : " << greaterCompareNum_ << "}";
	strstrm << ", {\"lessCompareNum_\" : " << lessCompareNum_ << "}";

	if (startCondition_ != NULL) {
		strstrm << ", {\"startCondition_\" : " << startCondition_->dump() << "}";
		if (funcInfo_->getColumnSchema() != NULL && funcInfo_->getColumnSchema()->getColumnNum() > 1) {
			const CompositeInfoObject *object = static_cast<const CompositeInfoObject *>(startCondition_->value_);
			strstrm << ", {\"startCondition_composite_value\" : ";
			object->dump(*funcInfo_, strstrm);
			strstrm << "}";
		}
	} else {
		strstrm << ", {\"startCondition_\" : NULL}";
	}
	if (endCondition_ != NULL) {
		strstrm << ", {\"endCondition_\" : " << endCondition_->dump() << "}";
		if (funcInfo_->getColumnSchema() != NULL && funcInfo_->getColumnSchema()->getColumnNum() > 1) {
			const CompositeInfoObject *object = static_cast<const CompositeInfoObject *>(endCondition_->value_);
			strstrm << ", {\"endCondition_composite_value\" : ";
			object->dump(*funcInfo_, strstrm);
			strstrm << "}";
		}
	} else {
		strstrm << ", {\"endCondition_\" : NULL}";
	}

	strstrm << "[";
	if (filterConds_ != NULL) {
		for (size_t i = 0; i < filterConds_->size(); i++) {
			if (i != 0) {
				strstrm << ",";
			}
			strstrm << (*filterConds_)[i].dump();
		}
	}
	strstrm << "]";
	strstrm << "}";
	return strstrm.str();
}

