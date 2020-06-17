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
	@brief Implementation of HashMap
*/
#include "hash_map.h"
#include "util/trace.h"
#include "collection.h"
#include "time_series.h"
#include <iostream>


/*!
	@brief Allocate HashMap Object
*/
int32_t HashMap::initialize(TransactionContext& txn, ColumnType columnType,
	ColumnId columnId, const AllocateStrategy& metaAllocateStrategy,
	bool isUnique) {
	hashMapImage_ = BaseObject::allocate<HashMapImage>(sizeof(HashMapImage),
		metaAllocateStrategy, getBaseOId(), OBJECT_TYPE_HASH_MAP);

	if (columnType == COLUMN_TYPE_STRING) {
		hashMapImage_->thresholdSplit_ = THRESHOLD_SPLIT_STRING;
	}
	else {
		hashMapImage_->thresholdSplit_ = ThresholdSplitNonString;
	}

	hashMapImage_->size_ = 0;
	hashMapImage_->split_ = 0;
	hashMapImage_->front_ = INITIAL_HASH_ARRAY_SIZE;
	hashMapImage_->rear_ = hashMapImage_->front_;
	hashMapImage_->toSplit_ = false;
	hashMapImage_->toMerge_ = false;

	hashMapImage_->keyType_ = columnType;
	hashMapImage_->columnId_ = columnId;
	hashMapImage_->isUnique_ = isUnique ? 1 : 0;

	hashMapImage_->padding1_ = 0;
	hashMapImage_->padding2_ = 0;
	hashMapImage_->padding3_ = 0;
	hashMapImage_->padding4_ = 0;
	hashMapImage_->padding5_ = 0;
	hashMapImage_->padding6_ = 0;
	hashMapImage_->padding7_ = 0;

	hashArray_.initialize(txn, &(hashMapImage_->hashArrayImage_), true);

	while (hashArray_.size() < hashMapImage_->rear_) {
		hashArray_.twiceHashArray(txn);
	}


	return GS_SUCCESS;
}

/*!
	@brief Free Objects related to HashMap
*/
bool HashMap::finalize(TransactionContext& txn) {
	setDirty();

	uint64_t removeNum = NUM_PER_EXEC;
	if (size() > 0) {
		uint32_t counter = 0;
		for (uint64_t i = 0; i < hashMapImage_->rear_; i++) {
			Bucket bucket(txn, *getObjectManager(), maxArraySize_,
				maxCollisionArraySize_);
			hashArray_.get(txn, hashMapImage_->rear_ - i - 1, bucket);
			bucket.clear(txn);
			counter++;
			removeNum--;
			if (removeNum == 0) {
				break;
			}
		}
		hashMapImage_->rear_ -= counter;
	}
	if (removeNum > 0) {
		hashArray_.finalize(txn, removeNum);
	}
	if (removeNum > 0) {
		BaseObject::finalize();
	}
	return removeNum > 0;
}

/*!
	@brief Insert Row Object
*/
int32_t HashMap::insert(
	TransactionContext& txn, const void* constKey, OId oId) {
	assert(oId != UNDEF_OID);
	setDirty();

	void* key = const_cast<void*>(constKey);
	switch (hashMapImage_->keyType_) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t*>(key));
		return insertObject<uint8_t*>(
			txn, stringCusor.str(), stringCusor.stringLength(), oId);
	} break;
	case COLUMN_TYPE_BOOL:
		return insertObject<bool>(txn, *reinterpret_cast<bool*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_BYTE:
		return insertObject<int8_t>(txn, *reinterpret_cast<int8_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_SHORT:
		return insertObject<int16_t>(txn, *reinterpret_cast<int16_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_INT:
		return insertObject<int32_t>(txn, *reinterpret_cast<int32_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_LONG:
		return insertObject<int64_t>(txn, *reinterpret_cast<int64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_OID:
		return insertObject<uint64_t>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_FLOAT:
		return insertObject<float32>(txn, *reinterpret_cast<float32*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_DOUBLE:
		return insertObject<float64>(txn, *reinterpret_cast<float64*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		return insertObject<Timestamp>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
		break;
	}

	return GS_FAIL;  
}

/*!
	@brief Remove Row Object
*/
int32_t HashMap::remove(
	TransactionContext& txn, const void* constKey, OId oId) {
	assert(oId != UNDEF_OID);
	setDirty();

	void* key = const_cast<void*>(constKey);
	switch (hashMapImage_->keyType_) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t*>(key));
		return removeObject<uint8_t*>(
			txn, stringCusor.str(), stringCusor.stringLength(), oId);
	} break;
	case COLUMN_TYPE_BOOL:
		return removeObject<bool>(txn, *reinterpret_cast<bool*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_BYTE:
		return removeObject<int8_t>(txn, *reinterpret_cast<int8_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_SHORT:
		return removeObject<int16_t>(txn, *reinterpret_cast<int16_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_INT:
		return removeObject<int32_t>(txn, *reinterpret_cast<int32_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_LONG:
		return removeObject<int64_t>(txn, *reinterpret_cast<int64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_OID:
		return removeObject<int64_t>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_FLOAT:
		return removeObject<float32>(txn, *reinterpret_cast<float32*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_DOUBLE:
		return removeObject<float64>(txn, *reinterpret_cast<float64*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		return removeObject<Timestamp>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
		break;
	}

	return GS_FAIL;  
}

/*!
	@brief Update Row Object
*/
int32_t HashMap::update(
	TransactionContext& txn, const void* constKey, OId oId, OId newOId) {
	assert(oId != UNDEF_OID);
	setDirty();

	void* key = const_cast<void*>(constKey);
	switch (hashMapImage_->keyType_) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t*>(key));
		return updateObject<uint8_t*>(
			txn, stringCusor.str(), stringCusor.stringLength(), oId, newOId);
	} break;
	case COLUMN_TYPE_BOOL:
		return updateObject<bool>(txn, *reinterpret_cast<bool*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_BYTE:
		return updateObject<int8_t>(txn, *reinterpret_cast<int8_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_SHORT:
		return updateObject<int16_t>(txn, *reinterpret_cast<int16_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_INT:
		return updateObject<int32_t>(txn, *reinterpret_cast<int32_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_LONG:
		return updateObject<int64_t>(txn, *reinterpret_cast<int64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_OID:
		return updateObject<uint64_t>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_FLOAT:
		return updateObject<float32>(txn, *reinterpret_cast<float32*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_DOUBLE:
		return updateObject<float64>(txn, *reinterpret_cast<float64*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		return updateObject<Timestamp>(txn, *reinterpret_cast<uint64_t*>(key),
			FixedSizeOfColumnType[hashMapImage_->keyType_], oId, newOId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
		break;
	}

	return GS_FAIL;  
}

/*!
	@brief Search Row Object
*/
int32_t HashMap::search(
	TransactionContext& txn, const void* constKey, OId& oId) {
	assert(constKey != NULL);

	void* key = const_cast<void*>(constKey);

	switch (hashMapImage_->keyType_) {
	case COLUMN_TYPE_STRING:
		{
			StringCursor stringCusor(reinterpret_cast<uint8_t *>(key));
			oId =
				searchObject<uint8_t*>(txn, stringCusor.str(), stringCusor.stringLength());
		}
		break;
	case COLUMN_TYPE_BOOL:
		oId = searchObject<bool>(txn, *reinterpret_cast<bool*>(key), sizeof(bool));
		break;
	case COLUMN_TYPE_BYTE:
		oId = searchObject<int8_t>(txn, *reinterpret_cast<int8_t*>(key), sizeof(int8_t));
		break;
	case COLUMN_TYPE_SHORT:
		oId =
			searchObject<int16_t>(txn, *reinterpret_cast<int16_t*>(key), sizeof(int16_t));
		break;
	case COLUMN_TYPE_INT:
		oId =
			searchObject<int32_t>(txn, *reinterpret_cast<int32_t*>(key), sizeof(int32_t));
		break;
	case COLUMN_TYPE_LONG:
		oId =
			searchObject<int64_t>(txn, *reinterpret_cast<int64_t*>(key), sizeof(int64_t));
		break;
	case COLUMN_TYPE_OID:
		oId = searchObject<uint64_t>(
			txn, *reinterpret_cast<uint64_t*>(key), sizeof(uint64_t));
		break;
	case COLUMN_TYPE_FLOAT:
		oId =
			searchObject<float32>(txn, *reinterpret_cast<float32*>(key), sizeof(float32));
		break;
	case COLUMN_TYPE_DOUBLE:
		oId =
			searchObject<float64>(txn, *reinterpret_cast<float64*>(key), sizeof(float64));
		break;
	case COLUMN_TYPE_TIMESTAMP:
		oId = searchObject<Timestamp>(
			txn, *reinterpret_cast<uint64_t*>(key), sizeof(Timestamp));
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
		break;
	}
	return GS_FAIL;  
}

/*!
	@brief Search Row Objects
*/
int32_t HashMap::search(TransactionContext &txn, SearchContext &sc,
	util::XArray<OId> &idList, OutputOrder outputOrder) {
	UNUSED_VARIABLE(outputOrder);
	ResultSize limit = sc.getLimit();
	if (limit == 0) {
		return GS_SUCCESS;
	}
	TermCondition *cond = sc.getKeyCondition();
	if (cond == NULL) {
		getAll(txn, limit, idList);
		return GS_SUCCESS;
	}
	const void* constKey = cond->value_;
	uint32_t size = cond->valueSize_;

	void* key = const_cast<void*>(constKey);

	switch (hashMapImage_->keyType_) {
	case COLUMN_TYPE_STRING:
		return searchObject<uint8_t*>(
			txn, reinterpret_cast<uint8_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_BOOL:
		return searchObject<bool>(
			txn, *reinterpret_cast<bool*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_BYTE:
		return searchObject<int8_t>(
			txn, *reinterpret_cast<int8_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_SHORT:
		return searchObject<int16_t>(
			txn, *reinterpret_cast<int16_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_INT:
		return searchObject<int32_t>(
			txn, *reinterpret_cast<int32_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_LONG:
		return searchObject<int64_t>(
			txn, *reinterpret_cast<int64_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_OID:
		return searchObject<uint64_t>(
			txn, *reinterpret_cast<uint64_t*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_FLOAT:
		return searchObject<float32>(
			txn, *reinterpret_cast<float32*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_DOUBLE:
		return searchObject<float64>(
			txn, *reinterpret_cast<float64*>(key), size, limit, idList);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		return searchObject<Timestamp>(
			txn, *reinterpret_cast<uint64_t*>(key), size, limit, idList);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
		break;
	}
	return GS_FAIL;  
}

/*!
	@brief Get all Row Objects all at once
*/
int32_t HashMap::getAll(
	TransactionContext& txn, ResultSize limit, util::XArray<OId>& idList) {
	if (limit == 0) {
		return GS_SUCCESS;
	}

	uint8_t readOnly = OBJECT_READ_ONLY;
	for (uint32_t i = 0; i < hashMapImage_->rear_; i++) {
		Bucket bucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);

		hashArray_.get(txn, i, bucket, readOnly);

		Bucket::Cursor cursor(txn, *getObjectManager());
		bucket.set(txn, cursor);

		while (bucket.next(txn, cursor)) {
			idList.push_back(resetModuleFlagOId(bucket.getCurrentOId(cursor)));
			if (idList.size() >= limit) {
				return GS_SUCCESS;
			}
		}
	}

	return GS_SUCCESS;
}

/*!
	@brief Get all Row Objects
*/
int32_t HashMap::getAll(TransactionContext& txn, ResultSize limit,
	util::XArray<OId>& idList, HashCursor& hashCursor) {
	if (limit == 0) {
		return GS_SUCCESS;
	}

	uint8_t readOnly = OBJECT_READ_ONLY;
	for (uint64_t i = hashCursor.rearCursor_; i < hashMapImage_->rear_; i++) {
		Bucket bucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, i, bucket, readOnly);

		Bucket::Cursor cursor(txn, *getObjectManager());
		if (hashCursor.isContinue_) {
			hashCursor.set(txn, cursor, readOnly);
			hashCursor.isContinue_ = false;
		}
		else {
			bucket.set(txn, cursor, readOnly);
		}

		bool isNext = bucket.next(txn, cursor);
		while (isNext) {
			idList.push_back(bucket.getCurrentOId(cursor));
			if (idList.size() >= limit) {
				hashCursor.currentArrayOId_ = cursor.array_.getBaseOId();
				hashCursor.currentArraysize_ = cursor.size_;
				hashCursor.bucketIterator_ = cursor.iterator_;
				hashCursor.bucketSize_ = cursor.bucketSize_;

				hashCursor.rearCursor_ = i;
				hashCursor.isContinue_ = true;
				return GS_FAIL;  
			}
			isNext = bucket.next(txn, cursor);
		}
	}

	return GS_SUCCESS;
}

template <class T>
int32_t HashMap::insertObject(
	TransactionContext& txn, const T key, uint32_t size, OId oId) {
	if (hashMapImage_->toSplit_) {
		split<T>(txn);
	}

	{
		uint32_t addr = hashBucketAddr<T>(key, size);
		Bucket bucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, addr, bucket);
		size_t arraySize = bucket.append(txn, oId);
		hashMapImage_->size_++;  

		if (arraySize > hashMapImage_->thresholdSplit_ &&
			hashMapImage_->toMerge_ == false) {
			hashMapImage_->toSplit_ = true;
		}
	}


	return GS_SUCCESS;
}

template <class T>
int32_t HashMap::removeObject(
	TransactionContext& txn, const T key, uint32_t size, OId oId) {
	size_t bucketSize;
	{
		uint32_t addr = hashBucketAddr<T>(key, size);
		Bucket bucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, addr, bucket);
		if (bucket.remove(txn, oId, bucketSize) == true) {
			hashMapImage_->size_--;
		}
	}

	if (hashMapImage_->toMerge_) {
		merge<T>(txn);
	}

	if (static_cast<uint32_t>(bucketSize) < THRESHOLD_MERGE &&
		hashMapImage_->toSplit_ == false &&
		hashMapImage_->front_ > INITIAL_HASH_ARRAY_SIZE) {
		hashMapImage_->toMerge_ = true;  
	}

	return GS_SUCCESS;
}

template <class T>
int32_t HashMap::updateObject(
	TransactionContext& txn, const T key, uint32_t size, OId oId, OId newOId) {
	HashArrayObject topHashArrayObject(txn, *getObjectManager());
	hashArray_.getTopArray(txn, topHashArrayObject);

	uint32_t addr = hashBucketAddr<T>(key, size);
	Bucket bucket(
		txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
	hashArray_.get(txn, addr, bucket);
	Bucket::Cursor cursor(txn, *getObjectManager());
	bucket.set(txn, cursor);

	bool updated = false;
	while (bucket.next(txn, cursor)) {
		if (bucket.getCurrentOId(cursor) == oId) {
			bucket.setCurrentOId(cursor, newOId);
			updated = true;
			break;
		}
	}

	if (updated == false) {
		return GS_FAIL;
	}


	return GS_SUCCESS;
}

template <class T>
OId HashMap::searchObject(TransactionContext& txn, const T key, uint32_t size) {
	uint32_t addr = hashBucketAddr<T>(key, size);
	Bucket bucket(
		txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
	hashArray_.get(txn, addr, bucket, OBJECT_READ_ONLY);
	Bucket::Cursor cursor(txn, *getObjectManager());
	bucket.set(txn, cursor, OBJECT_READ_ONLY);

	while (bucket.next(txn, cursor, OBJECT_READ_ONLY)) {
		if (isEquals<T>(txn, key, size, bucket.getCurrentOId(cursor)) == true) {
			return resetModuleFlagOId(bucket.getCurrentOId(cursor));
		}
	}

	return UNDEF_OID;
}

template <class T>
int32_t HashMap::searchObject(TransactionContext& txn, const T key,
	uint32_t size, ResultSize limit, util::XArray<OId>& idList) {
	uint32_t addr = hashBucketAddr<T>(key, size);
	Bucket bucket(
		txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
	hashArray_.get(txn, addr, bucket, OBJECT_READ_ONLY);
	Bucket::Cursor cursor(txn, *getObjectManager());
	bucket.set(txn, cursor, OBJECT_READ_ONLY);

	uint32_t hit = 0;
	while (bucket.next(txn, cursor, OBJECT_READ_ONLY)) {
		if (isEquals<T>(txn, key, size, bucket.getCurrentOId(cursor)) == true) {
			idList.push_back(resetModuleFlagOId(bucket.getCurrentOId(cursor)));
			if (++hit >= limit) {
				break;
			}
		}
	}

	return GS_SUCCESS;
}

template <class T>
void HashMap::split(TransactionContext& txn) {
	if (hashMapImage_->split_ == hashMapImage_->front_) {
		hashMapImage_->split_ = 0;
		hashMapImage_->front_ = hashMapImage_->front_ << 1;
		hashMapImage_->toSplit_ = false;
	}

	uint64_t bucketAddr0 = hashMapImage_->split_;
	uint64_t bucketAddr1 = hashMapImage_->rear_;
	hashMapImage_->split_++;
	hashMapImage_->rear_++;

	if (hashMapImage_->rear_ > hashArray_.size()) {
		hashArray_.twiceHashArray(txn);

		if (hashMapImage_->rear_ > hashArray_.size()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_MAX_ARRY_SIZE_OVER, "");
		}
	}

	Bucket splitBucket(
		txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
	hashArray_.get(txn, bucketAddr0, splitBucket);
	Bucket::Cursor cursor(txn, *getObjectManager());
	splitBucket.set(txn, cursor);
	if (cursor.size_ == 0) {
		;  
	}
	else {
		util::XArray<OId> remainOIdList(txn.getDefaultAllocator());
		Bucket newBucket1(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, bucketAddr1, newBucket1);
		size_t bucket0Size = 0;
		size_t bucket1Size = 0;

		bool isCurrent = splitBucket.next(txn, cursor);
		while (isCurrent) {
			OId oId = splitBucket.getCurrentOId(cursor);
			uint32_t addr = hashBucketAddrFromObject<T>(txn, oId);
			if (addr == bucketAddr0) {
				bucket0Size++;
				remainOIdList.push_back(oId);
			}
			else {
				bucket1Size++;
				newBucket1.append(txn, oId);
			}
			isCurrent = splitBucket.next(txn, cursor);
		}
		cursor.array_.reset();

		if (bucket1Size > 0) {
			splitBucket.clear(txn);
			for (size_t i = 0; i < remainOIdList.size(); i++) {
				splitBucket.append(txn, remainOIdList[i]);
			}
		}
		else if (bucket0Size > 1) {
			remainOIdList.clear();
		}
	}

	return;
}

template <class T>
void HashMap::merge(TransactionContext& txn) {
	if (hashMapImage_->front_ > INITIAL_HASH_ARRAY_SIZE) {
		if (hashMapImage_->split_ == 0) {
			hashMapImage_->front_ = hashMapImage_->front_ >> 1;
			hashMapImage_->split_ = hashMapImage_->front_;
			hashMapImage_->toMerge_ = false;
		}

		hashMapImage_->split_--;
		hashMapImage_->rear_--;
		uint64_t sourceBucketId = hashMapImage_->rear_;

		Bucket sourceBucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, sourceBucketId, sourceBucket);

		Bucket::Cursor cursor(txn, *getObjectManager());
		sourceBucket.set(txn, cursor);

		bool isNext = sourceBucket.next(txn, cursor);
		if (!isNext) {
			return;
		}

		uint32_t addr = hashBucketAddrFromObject<T>(
			txn, sourceBucket.getCurrentOId(cursor));
		cursor.array_.reset();

		Bucket targetBucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, addr, targetBucket);

		if (targetBucket.merge(txn, sourceBucket, ThresholdMergeLimit_) == 0) {
			hashMapImage_->toMerge_ = false;
			hashMapImage_->split_++;
			hashMapImage_->rear_++;
			return;
		}
	}

	uint64_t hashArraySize = hashArray_.size();
	if (hashMapImage_->rear_ < hashArray_.getHalfSize()) {
		hashArray_.halfHashArray(txn);
		hashArraySize = hashArray_.size();

		if (hashMapImage_->front_ << 1 <= hashArraySize >> 1 &&
			hashArraySize >> 1 >= maxArraySize_) {
			hashMapImage_->front_ = hashMapImage_->front_ << 1;
			hashMapImage_->split_ = 0;
			hashMapImage_->rear_++;
			hashMapImage_->toMerge_ = false;
			return;
		}
	}
	return;
}

template <class T>
T* HashMap::getField(TransactionContext& txn, OId oId) {
	OId validOId = resetModuleFlagOId(oId);
	BaseContainer::RowArray *rowArray = container_->getCacheRowArray(txn);
	rowArray->load(txn, validOId, container_, OBJECT_READ_ONLY);
	BaseContainer::RowArray::Row row(rowArray->getRow(), rowArray);
	BaseObject baseFieldObject(txn.getPartitionId(), *getObjectManager());
	row.getField(txn, container_->getColumnInfo(hashMapImage_->columnId_),
		baseFieldObject);
	rowArray->reset();
	return reinterpret_cast<T*>(baseFieldObject.getCursor<uint8_t>());
}

std::string HashMap::dump(TransactionContext& txn, uint8_t mode) {
	util::NormalOStringStream strstrm;
	switch (mode) {
	case PARAM_DUMP: {
		strstrm << dumpParameters();
	} break;
	case SUMMARY_HEADER_DUMP: {
		strstrm << "totalRequest, "
				<< "currentStatus(real), "
				<< "*bucketSize, "
				<< "activeBucket, "
				<< "allocateNum, "
				<< "useNum, "
				<< "useRatio, "
				<< "allocateMaxBucket, "
				<< "allocateMinBucket, "
				<< "useMaxBucket, "
				<< "useMinBucket, "
				<< "useAvgBucket, "
				<< "theoretical memsize, "
				<< "real/theoretical, ";
	} break;
	case SUMMARY_DUMP: {
		MapStat mapStat;

		mapStat.bucketNum_ = hashArray_.size();
		mapStat.activeBucket_ = hashMapImage_->rear_;

		HashArrayObject topHashArrayObject(txn, *getObjectManager());
		hashArray_.getTopArray(txn, topHashArrayObject, OBJECT_READ_ONLY);


		int64_t totalRequest = 0, currentStatus = 0, theoreticalStatus = 0;
		uint64_t ratio = mapStat.allocateNum_ == 0
							 ? 0
							 : mapStat.useNum_ * 100 / mapStat.allocateNum_;
		theoreticalStatus =
			(mapStat.allocateNum_ + mapStat.activeBucket_) *
			8;  
		strstrm << ", " << totalRequest << ", " << currentStatus << ", "
				<< mapStat.bucketNum_ << ", " << mapStat.activeBucket_ << ", "
				<< mapStat.allocateNum_ << ", " << mapStat.useNum_ << ", "
				<< ratio << "% "
				<< ", " << mapStat.allocateMaxBucket_ << ", "
				<< mapStat.allocateMinBucket_ << ", " << mapStat.useMaxBucket_
				<< ", " << mapStat.useMinBucket_ << ", "
				<< mapStat.useAvgBucket_ << ", " << theoreticalStatus << ", "
				<< static_cast<float64>(currentStatus) /
					   static_cast<float64>(theoreticalStatus)
				<< ", ";

		assert(mapStat.useNum_ == size());
	} break;
	case ALL_DUMP: {
		HashArrayObject topHashArrayObject(txn, *getObjectManager());
		hashArray_.getTopArray(txn, topHashArrayObject, OBJECT_READ_ONLY);

		hashArray_.dump(txn);

		uint64_t maxBucketSize = hashArray_.size();

		strstrm << std::endl;
		strstrm << "dump HashMap[" << hashArray_.size() << "], ";
		strstrm << dumpParameters();
		strstrm << "columnType, " << (uint32_t)hashMapImage_->keyType_ << ", "
				<< "columnId, " << hashMapImage_->columnId_ << ", "
				<< "size, " << size() << ", "
				<< "bucketsSize, " << hashMapImage_->rear_ << ", "
				<< "maxBucketsSize, " << maxBucketSize << std::endl;

		int64_t bucketNum = 0;
		int64_t objectNum = 0;
		uint64_t hashArrayElementSize = hashArray_.size();
		uint64_t rear = hashMapImage_->rear_;

		for (uint64_t i = 0; i < hashArrayElementSize; i++) {
			if (i >= rear) {
				if (i == rear || i == hashArrayElementSize - 1) {
					strstrm << "[" << i << ", "
							<< "( -1 )], ";
				}
				continue;
			}

			if (hashMapImage_->split_ == i) {
				strstrm << "s";
			}
			else {
				strstrm << " ";
			}
			if (hashMapImage_->front_ == i) {
				strstrm << "f";
			}
			else {
				strstrm << " ";
			}
			if (hashMapImage_->rear_ == i) {
				strstrm << "r";
			}
			else {
				strstrm << " ";
			}

			Bucket bucket(txn, *getObjectManager(), maxArraySize_,
				maxCollisionArraySize_);
			hashArray_.get(txn, (uint32_t)i, bucket, OBJECT_READ_ONLY);
			int64_t currentBucketNum = 0;
			int64_t currentObjectNum = 0;
			strstrm << bucket.dump(txn, i, currentBucketNum, currentObjectNum);
			bucketNum += currentBucketNum;
			objectNum += currentObjectNum;
		}

		strstrm << std::endl;

		int64_t emptyNum = bucketNum - objectNum;
		int64_t emptyRatio = (bucketNum == 0) ? 0 : emptyNum * 100 / bucketNum;
		strstrm << "hashArraySize, " << hashArrayElementSize << ", "
				<< "BucketSize, " << bucketNum << ", "
				<< "totalNum, " << objectNum << ", "
				<< "emptyNum, " << emptyNum << ", "
				<< "emptyRatio, " << emptyRatio << "% " << std::endl;

	} break;
	}
	return strstrm.str();
}

/*!
	@brief Validate the map
*/
std::string HashMap::validate(TransactionContext& txn) {
	util::NormalOStringStream strstrm;

	uint64_t count = 0;
	for (uint32_t i = 0; i < hashMapImage_->rear_; i++) {
		Bucket bucket(
			txn, *getObjectManager(), maxArraySize_, maxCollisionArraySize_);
		hashArray_.get(txn, i, bucket);

		if (bucket.getBucketOId() == UNDEF_OID) {
			continue;
		}
		else if (isModuleFlagOId(bucket.getBucketOId()) == false) {
			count++;  
		}
		else {
			HashArrayObject hashArrayObject(txn, *getObjectManager());
			getArrayObject(txn, bucket.getBucketOId(), hashArrayObject,
				OBJECT_READ_ONLY);  
			size_t bucketSize = bucket.getBucketSize(txn);

			if (bucketSize <= 1) {
				std::cout << "invalid bucket[" << i << "] size : should be >2, "
						  << bucketSize << std::endl;
				std::cout << dump(txn, 2);
			}


			checkAddr(txn, i, bucket);


			count += bucketSize;
		}
	}
	if (size() != count) {
		std::cout << "invalid hashMap size, expected=" << size()
				  << ", actual=" << count << std::endl;
	}

	return strstrm.str();
}
