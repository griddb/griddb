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
	@brief Definition of HashMap
*/
#ifndef HASH_MAP_H_
#define HASH_MAP_H_

#include "util/trace.h"
#include "base_index.h"
#include "data_type.h"
#include "gs_error.h"
#include "object_manager.h"
#include "value.h"  
#include "value_operator.h"
#include <iostream>
#include <stdio.h>
#include <vector>

UTIL_TRACER_DECLARE(HASH_MAP);

class TransactionContext;
class BaseContainer;
struct TermCondition;
struct HashMapImage;


/*!
	@brief Hash Map Index
*/
class HashMap : public BaseIndex {
private:
	/*!
		@brief Object for HashArray
	*/
	class HashArrayObject : public BaseObject {
	public:
		HashArrayObject(TransactionContext& txn, ObjectManager& objectManager)
			: BaseObject(txn.getPartitionId(), objectManager) {}

		HashArrayObject(
			TransactionContext& txn, ObjectManager& objectManager, OId oId)
			: BaseObject(txn.getPartitionId(), objectManager, oId) {
			moveCursor(ARRAY_OBJECT_HEADER_SIZE);
		}

		void load(OId oId) {
			BaseObject::load(oId);
			moveCursor(ARRAY_OBJECT_HEADER_SIZE);
		}
		void load(OId oId, uint8_t forUpdate) {
			BaseObject::load(oId, forUpdate);
			moveCursor(ARRAY_OBJECT_HEADER_SIZE);
		}

		OId& getElemOId(uint64_t pos) {
			return getCursor()[pos];
		}

		const OId& getElemOId(uint64_t pos) const {
			return getCursor()[pos];
		}

		void setElemOId(uint64_t pos, OId oId) {
			getElemOId(pos) = oId;
		}

		void setArraySize(uint32_t size) {
			*(reinterpret_cast<uint32_t*>(getCursor()) - 1) = size;
		}

		uint32_t getArraySize() const {
			return *(reinterpret_cast<uint32_t*>(getCursor()) - 1);
		}

		void setArrayMaxSize(uint32_t size) {
			*(reinterpret_cast<uint32_t*>(getCursor()) - 2) = size;
		}

		uint32_t getArrayMaxSize() const {
			return *(reinterpret_cast<uint32_t*>(getCursor()) - 2);
		}

		void copyArray(const HashArrayObject& srcObject, uint64_t srcOffset,
			uint64_t destOffset, uint64_t maxSize) {
			memcpy(getCursor() + destOffset, srcObject.getCursor() + srcOffset,
				maxSize * sizeof(OId));
		}

	private:
		HashArrayObject(const HashArrayObject&);  
		HashArrayObject& operator=(
			const HashArrayObject&);  
		OId* getCursor() {
			return BaseObject::getCursor<OId>();
		}
		OId* getCursor() const {
			return BaseObject::getCursor<OId>();
		}
	};

public:
	/*!
		@brief Represents the type(s) of dump
	*/
	enum {
		SUMMARY_HEADER_DUMP = 0,
		SUMMARY_DUMP = 1,
		ALL_DUMP = 2,
		PARAM_DUMP = 3
	};

	/*!
		@brief Statistics of Hash Map
	*/
	struct MapStat {
		uint64_t allocateNum_;		  
		uint64_t useNum_;			  
		uint64_t bucketNum_;		  
		uint64_t activeBucket_;		  
		uint64_t allocateMaxBucket_;  
		uint64_t allocateMinBucket_;  
		uint64_t useMaxBucket_;		  
		uint64_t useMinBucket_;		  
		uint64_t useAvgBucket_;
		MapStat() {
			allocateNum_ = 0;
			useNum_ = 0;
			bucketNum_ = 0;
			activeBucket_ = 0;
			allocateMaxBucket_ = UINT64_MAX;
			allocateMinBucket_ = UINT64_MAX;
			useMaxBucket_ = UINT64_MAX;
			useMinBucket_ = UINT64_MAX;
			useAvgBucket_ = UINT64_MAX;
		}
	};

private:
	class Bucket;

	static const uint32_t ARRAY_OBJECT_HEADER_SIZE = sizeof(uint32_t) << 1;
	static const uint32_t OID_BIT_SIZE = 3;  

	static const uint32_t INITIAL_HASH_ARRAY_SIZE = 4;
	static const uint32_t INITIAL_BUCKET_SIZE = 2;  

	static const uint32_t THRESHOLD_SPLIT_STRING = 8;
	static const uint32_t THRESHOLD_SPLIT_META_MAP = 1;
	static const uint32_t THRESHOLD_MERGE = 1;  

	const uint32_t maxObjectSize_;
	const uint32_t maxArrayObjectSize_;
	const uint32_t maxArraySize_;
	const uint64_t maxHashArraySize_;
	const uint32_t maxCollisionArraySize_;
	const uint32_t ThresholdMergeLimit_;
	const uint32_t ThresholdSplitNonString;

	static uint32_t size2ObjectArraySize(uint32_t size) {
		return ARRAY_OBJECT_HEADER_SIZE + (size << OID_BIT_SIZE);
	}

	static uint32_t arrayObjectSize2Size(uint32_t arrayObjectSize) {
		return (arrayObjectSize - ARRAY_OBJECT_HEADER_SIZE) >> OID_BIT_SIZE;
	}

	static void allocateArrayObject(TransactionContext& txn, uint32_t size,
		OId& oId, uint32_t& actualSize, HashArrayObject& hashArrayObject) {
		uint32_t objectSize = size2ObjectArraySize(size);

		hashArrayObject.allocate<OId>(objectSize,
			AllocateStrategy(ALLOCATE_NO_EXPIRE_MAP), oId,
			OBJECT_TYPE_HASH_MAP);
		actualSize = arrayObjectSize2Size(objectSize);
		assert(size == actualSize);
		memset(hashArrayObject.getBaseAddr(), 0xff, objectSize);
		hashArrayObject.moveCursor(ARRAY_OBJECT_HEADER_SIZE);
		hashArrayObject.setArrayMaxSize(size);
		hashArrayObject.setArraySize(0);
	}

	static void freeArrayObject(
		TransactionContext& txn, ObjectManager& objectManager, OId& oId) {
		assert(oId != UNDEF_OID);
		objectManager.free(txn.getPartitionId(), resetModuleFlagOId(oId));
		oId = UNDEF_OID;
	}

	static void getArrayObject(TransactionContext& txn, const OId oId,
		HashArrayObject& hashArrayObject, uint8_t getOption) {
		hashArrayObject.load(resetModuleFlagOId(oId));
		if (getOption == OBJECT_READ_ONLY) {
		}
		else {
			hashArrayObject.getObjectManager()->setDirty(
				txn.getPartitionId(), resetModuleFlagOId(oId));
		}
		return;
	}

private:
	/*!
		@brief Hash array
	*/
	struct HashArrayImage;
	class HashArray {
	public:
		HashArray(ObjectManager* objectManager, HashArrayImage* arrayImage,
			uint32_t maxArraySize)
			: arrayImage_(arrayImage),
			  maxArraySize_(maxArraySize),
			  objectManager_(objectManager) {
		}

	private:
		HashArrayImage* arrayImage_;
		const uint32_t maxArraySize_;
		ObjectManager* objectManager_;

	public:
		static const uint32_t MIN_ARRAY_SIZE = INITIAL_HASH_ARRAY_SIZE;

		void initialize(
			TransactionContext& txn, HashArrayImage* arrayImage, bool minimum) {
			arrayImage_ = arrayImage;
			arrayImage_->oId_ = UNDEF_OID;
			arrayImage_->dimension_ = 1;
			uint32_t initialSize = maxArraySize_;
			if (minimum == true) {
				initialSize = MIN_ARRAY_SIZE;
			}
			HashArrayObject hashArrayObject(txn, *objectManager_);
			allocateArrayObject(txn, initialSize, arrayImage_->oId_,
				initialSize, hashArrayObject);
			arrayImage_->size_ = static_cast<uint64_t>(initialSize);
		}

		/*!
			@brief Free Objects related to HashArray
		*/
		void finalize(TransactionContext& txn) {
			if (arrayImage_->oId_ == UNDEF_OID) {
				return;
			}

			switch (arrayImage_->dimension_) {
			case 1:
				finalizeArray(txn, arrayImage_->oId_);
				break;
			case 2:
				finalizeMatrix(txn, arrayImage_->oId_);
				break;
			case 3:
				finalizeCube(txn, arrayImage_->oId_);
				break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}
		}

		void twiceHashArray(TransactionContext& txn) {
			switch (arrayImage_->dimension_) {
			case 1:
				twiceArrayElements(txn);
				break;
			case 2:
				twiceMatrixElements(txn);
				break;
			case 3:
				twiceCubicElements(txn);
				break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}
			return;
		}

		void halfHashArray(TransactionContext& txn) {
			switch (arrayImage_->dimension_) {
			case 1:
				return halfArrayElements(txn);
				break;
			case 2:
				return halfMatrixElements(txn);
				break;
			case 3:
				return halfCubicElements(txn);
				break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}
			return;  
		}

		uint64_t getHalfSize() {
			switch (arrayImage_->dimension_) {
			case 1: {
				uint64_t size = (arrayImage_->size_ - 1) >> 1;
				if (size < MIN_ARRAY_SIZE) {
					size = MIN_ARRAY_SIZE;
				}
				return size;
			} break;
			case 2: {
				uint64_t matrixIndex = arrayImage_->size_ / maxArraySize_;
				return arrayImage_->size_ -
					   (((matrixIndex + 1) >> 1) * maxArraySize_);
			} break;
			case 3: {
				uint64_t cubicIndex =
					arrayImage_->size_ / maxArraySize_ / maxArraySize_;
				return arrayImage_->size_ - (((cubicIndex + 1) >> 1) *
												maxArraySize_ * maxArraySize_);
			} break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}
			return 0;  
		}

		void getTopArray(TransactionContext& txn,
			HashArrayObject& topHashArrayObject,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			assert(arrayImage_->oId_ != UNDEF_OID);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, forUpdate);
			return;
		}

		void get(TransactionContext& txn, uint64_t size, Bucket& bucket,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			switch (arrayImage_->dimension_) {
			case 1:
				bucket.load(arrayImage_->oId_, size, forUpdate);
				return;
				break;
			case 2: {
				uint32_t arrayIndex = (uint32_t)(size % maxArraySize_);
				uint32_t matrixIndex = (uint32_t)(size / maxArraySize_);
				HashArrayObject topHashArrayObject(txn, *objectManager_);
				getArrayObject(
					txn, arrayImage_->oId_, topHashArrayObject, forUpdate);

				bucket.load(topHashArrayObject.getElemOId(matrixIndex),
					arrayIndex, forUpdate);
				return;
			} break;
			case 3: {
				uint32_t arrayIndex = (uint32_t)(size % maxArraySize_);
				uint64_t div = (size / maxArraySize_);
				uint32_t matrixIndex = (uint32_t)(div % maxArraySize_);
				uint32_t cubicIndex = (uint32_t)(div / maxArraySize_);
				HashArrayObject topHashArrayObject(txn, *objectManager_);
				getArrayObject(
					txn, arrayImage_->oId_, topHashArrayObject, forUpdate);
				HashArrayObject cubeHashArrayObject(txn, *objectManager_);
				getArrayObject(txn, topHashArrayObject.getElemOId(cubicIndex),
					cubeHashArrayObject, OBJECT_READ_ONLY);

				bucket.load(cubeHashArrayObject.getElemOId(matrixIndex),
					arrayIndex, forUpdate);
			} break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}
			return;  
		}

		uint64_t size() const {
			return arrayImage_->size_;
		}

		std::string dump(TransactionContext& txn) {
			util::NormalOStringStream strstrm;

			switch (arrayImage_->dimension_) {
			case 1:
				strstrm << dumpArray(txn, arrayImage_->oId_) << ", ";
				break;
			case 2:
				strstrm << dumpMatrix(txn, arrayImage_->oId_) << ", ";
				break;
			case 3:
				strstrm << dumpCube(txn, arrayImage_->oId_) << ", ";
				break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
				break;
			}

			return strstrm.str();
		}

	private:
		void finalizeArray(TransactionContext& txn, OId& arrayOId) {
			freeArrayObject(txn, *objectManager_, arrayOId);
		}

		void finalizeMatrix(TransactionContext& txn, OId& matrixOId) {
			HashArrayObject hashArrayObject(txn, *objectManager_);
			getArrayObject(txn, matrixOId, hashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t size = hashArrayObject.getArraySize();

			for (uint32_t i = 0; i < size; i++) {
				finalizeArray(txn, hashArrayObject.getElemOId(i));
			}

			hashArrayObject.finalize();
			matrixOId = UNDEF_OID;
		}

		void finalizeCube(TransactionContext& txn, OId& cubeOId) {
			HashArrayObject hashArrayObject(txn, *objectManager_);
			getArrayObject(txn, cubeOId, hashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t size = hashArrayObject.getArraySize();

			for (uint32_t i = 0; i < size; i++) {
				finalizeMatrix(txn, hashArrayObject.getElemOId(i));
			}

			hashArrayObject.finalize();
			cubeOId = UNDEF_OID;
		}

		void twiceArrayElements(TransactionContext& txn) {
			HashArrayObject pTopHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, pTopHashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t maxSize = pTopHashArrayObject.getArrayMaxSize();
			uint32_t newArraySize = maxSize << 1;  
			if (newArraySize > (maxArraySize_ >> 1)) {
				newArraySize = maxArraySize_;
			}

			HashArrayObject newHashArrayObject(txn, *objectManager_);
			allocateArrayObject(txn, newArraySize, arrayImage_->oId_,
				newArraySize, newHashArrayObject);
			newHashArrayObject.copyArray(pTopHashArrayObject, 0, 0, maxSize);
			newHashArrayObject.setArraySize(pTopHashArrayObject.getArraySize());
			arrayImage_->size_ = newArraySize;
			pTopHashArrayObject.finalize();

			if (newArraySize == maxArraySize_) {
				arrayImage_->dimension_++;
				OId oldOId = arrayImage_->oId_;
				uint32_t tempSize = 0;
				allocateArrayObject(txn, maxArraySize_, arrayImage_->oId_,
					tempSize, newHashArrayObject);
				newHashArrayObject.setElemOId(0, oldOId);
				newHashArrayObject.setArraySize(1);
				GS_TRACE_INFO(HASH_MAP, GS_TRACE_DS_HASH_CHANGE_STATUS,
					"change dimention = " << arrayImage_->dimension_);
			}
		}

		void twiceMatrixElements(TransactionContext& txn) {
			HashArrayObject topHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t arraySize = topHashArrayObject.getArraySize();
			uint32_t newArraySize = arraySize << 1;  
			if (newArraySize > (maxArraySize_ >> 1)) {
				newArraySize = maxArraySize_;
			}

			for (uint32_t i = arraySize; i < newArraySize; i++) {
				uint32_t tempSize = 0;
				HashArrayObject tmpNewHashArrayObject(txn, *objectManager_);
				allocateArrayObject(txn, maxArraySize_,
					topHashArrayObject.getElemOId(i), tempSize,
					tmpNewHashArrayObject);
			}
			topHashArrayObject.setArraySize(newArraySize);
			arrayImage_->size_ += maxArraySize_ * (newArraySize - arraySize);

			if (newArraySize == maxArraySize_) {
				arrayImage_->dimension_++;
				OId oldOId = arrayImage_->oId_;
				uint32_t tempSize = 0;
				allocateArrayObject(txn, maxArraySize_, arrayImage_->oId_,
					tempSize, topHashArrayObject);
				topHashArrayObject.setElemOId(0, oldOId);
				topHashArrayObject.setArraySize(1);
				GS_TRACE_INFO(HASH_MAP, GS_TRACE_DS_HASH_CHANGE_STATUS,
					"change dimention = " << arrayImage_->dimension_);
			}
		}

		void twiceCubicElements(TransactionContext& txn) {
			HashArrayObject topHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, OBJECT_FOR_UPDATE);

			if (arrayImage_->size_ == maxArraySize_) {
				return;
			}

			uint32_t arraySize = topHashArrayObject.getArraySize();
			uint32_t newArraySize = arraySize << 1;  
			if (newArraySize > (maxArraySize_ >> 1)) {
				newArraySize = maxArraySize_;
			}

			for (uint32_t i = arraySize; i < newArraySize; i++) {
				HashArrayObject matrixHashArrayObject(txn, *objectManager_);
				uint32_t matrixSize = 0;
				allocateArrayObject(txn, maxArraySize_,
					topHashArrayObject.getElemOId(i), matrixSize,
					matrixHashArrayObject);
				uint32_t tempSize = 0;
				for (uint32_t j = 0; j < matrixSize; j++) {
					HashArrayObject simpleHashArrayObject(txn, *objectManager_);
					allocateArrayObject(txn, maxArraySize_,
						matrixHashArrayObject.getElemOId(j), tempSize,
						simpleHashArrayObject);
				}
			}
			topHashArrayObject.setArraySize(newArraySize);
			arrayImage_->size_ +=
				maxArraySize_ * maxArraySize_ * (newArraySize - arraySize);
		}

		void halfArrayElements(TransactionContext& txn) {
			HashArrayObject topHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, OBJECT_FOR_UPDATE);

			if (arrayImage_->size_ == MIN_ARRAY_SIZE) {
				return;
			}

			uint32_t maxSize = topHashArrayObject.getArrayMaxSize();
			uint32_t newArraySize = HashMap::align(maxSize >> 1);
			if (newArraySize < MIN_ARRAY_SIZE) {
				newArraySize = MIN_ARRAY_SIZE;
			}

			HashArrayObject newHashArrayObject(txn, *objectManager_);
			allocateArrayObject(txn, newArraySize, arrayImage_->oId_,
				newArraySize, newHashArrayObject);
			newHashArrayObject.copyArray(
				topHashArrayObject, 0, 0, newArraySize);
			newHashArrayObject.setArraySize(topHashArrayObject.getArraySize());
			topHashArrayObject.finalize();
			arrayImage_->size_ = newArraySize;

			return;
		}

		void halfMatrixElements(TransactionContext& txn) {
			HashArrayObject topHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t arraySize = topHashArrayObject.getArraySize();
			uint32_t newArraySize =
				HashMap::align(arraySize >> 1);  

			if (newArraySize == 0) {
				arrayImage_->dimension_--;
				arrayImage_->oId_ = topHashArrayObject.getElemOId(0);
				topHashArrayObject.finalize();
				getArrayObject(txn, arrayImage_->oId_, topHashArrayObject,
					OBJECT_FOR_UPDATE);
				GS_TRACE_INFO(HASH_MAP, GS_TRACE_DS_HASH_CHANGE_STATUS,
					"change dimention = " << arrayImage_->dimension_);
			}
			else {
				for (uint32_t i = newArraySize; i < arraySize; i++) {
					freeArrayObject(
						txn, *objectManager_, topHashArrayObject.getElemOId(i));
				}
				topHashArrayObject.setArraySize(newArraySize);
				arrayImage_->size_ -=
					maxArraySize_ * (arraySize - newArraySize);
			}

			return;
		}

		void halfCubicElements(TransactionContext& txn) {
			HashArrayObject topHashArrayObject(txn, *objectManager_);
			getArrayObject(
				txn, arrayImage_->oId_, topHashArrayObject, OBJECT_FOR_UPDATE);
			uint32_t arraySize = topHashArrayObject.getArraySize();
			uint32_t newArraySize =
				HashMap::align(arraySize >> 1);  
			if (newArraySize == 0) {
				arrayImage_->dimension_--;
				arrayImage_->oId_ = topHashArrayObject.getElemOId(0);
				topHashArrayObject.finalize();
				getArrayObject(txn, arrayImage_->oId_, topHashArrayObject,
					OBJECT_FOR_UPDATE);
				GS_TRACE_INFO(HASH_MAP, GS_TRACE_DS_HASH_CHANGE_STATUS,
					"change dimention = " << arrayImage_->dimension_);
			}
			else {
				for (uint32_t i = newArraySize; i < arraySize; i++) {
					{
						HashArrayObject hashArrayObject(txn, *objectManager_);
						getArrayObject(txn, topHashArrayObject.getElemOId(i),
							hashArrayObject, OBJECT_FOR_UPDATE);
						for (uint32_t j = 0; j < maxArraySize_; j++) {
							freeArrayObject(txn, *objectManager_,
								hashArrayObject.getElemOId(j));
						}
					}
					freeArrayObject(
						txn, *objectManager_, topHashArrayObject.getElemOId(i));
				}
				topHashArrayObject.setArraySize(newArraySize);
				arrayImage_->size_ -=
					maxArraySize_ * maxArraySize_ * (arraySize - newArraySize);
			}

			return;
		}

		std::string dumpArray(TransactionContext& txn, OId oId) {
			util::NormalOStringStream strstrm;

			HashArrayObject hashArrayObject(txn, *objectManager_);
			getArrayObject(txn, oId, hashArrayObject, OBJECT_READ_ONLY);
			uint32_t objectSize =
				objectManager_->getSize(hashArrayObject.getBaseAddr());
			uint32_t arraySize = hashArrayObject.getArraySize();

			strstrm << "Array, [OId, memSize, size]=[" << oId << ", "
					<< objectSize << ", " << arraySize << "], ";

			for (uint32_t i = 0; i < arraySize; i++) {
				Bucket* bucketArray =
					reinterpret_cast<Bucket*>(&(hashArrayObject.getElemOId(i)));
				strstrm << bucketArray->getBucketOId() << ", ";
			}
			strstrm << std::endl;
			return strstrm.str();
		}

		std::string dumpMatrix(TransactionContext& txn, OId oId) {
			util::NormalOStringStream strstrm;

			HashArrayObject hashArrayObject(txn, *objectManager_);
			getArrayObject(txn, oId, hashArrayObject, OBJECT_READ_ONLY);
			uint32_t objectSize =
				objectManager_->getSize(hashArrayObject.getBaseAddr());
			uint32_t arraySize = hashArrayObject.getArraySize();

			strstrm << "Matrix, [OId, max, size]=[" << oId << ", " << objectSize
					<< ", " << arraySize << "], ";

			for (uint32_t i = 0; i < arraySize; i++) {
				strstrm << hashArrayObject.getElemOId(i) << ", ";
			}
			strstrm << std::endl;
			for (uint32_t i = 0; i < arraySize; i++) {
				strstrm << dumpArray(txn, hashArrayObject.getElemOId(i));
			}
			return strstrm.str();
		}

		std::string dumpCube(TransactionContext& txn, OId oId) {
			util::NormalOStringStream strstrm;

			HashArrayObject hashArrayObject(txn, *objectManager_);
			getArrayObject(txn, oId, hashArrayObject, OBJECT_READ_ONLY);
			uint32_t objectSize =
				objectManager_->getSize(hashArrayObject.getBaseAddr());
			uint32_t arraySize = hashArrayObject.getArraySize();

			strstrm << "Cube, [OId, max, size]=[" << oId << ", " << objectSize
					<< ", " << arraySize << "], ";

			for (uint32_t i = 0; i < arraySize; i++) {
				strstrm << hashArrayObject.getElemOId(i) << ", ";
			}
			strstrm << std::endl;
			for (uint32_t i = 0; i < arraySize; i++) {
				strstrm << dumpMatrix(txn, hashArrayObject.getElemOId(i));
			}
			return strstrm.str();
		}
	};

	/*!
		@brief Hash Bucket
	*/
	class Bucket : public BaseObject {
	private:
		const uint32_t maxArraySize_;
		const uint32_t maxCollisionArraySize_;

	public:
		Bucket(TransactionContext& txn, ObjectManager& objectManager,
			uint32_t maxArraySize, uint32_t maxCollisionArraySize)
			: BaseObject(txn.getPartitionId(), objectManager),
			  maxArraySize_(maxArraySize),
			  maxCollisionArraySize_(maxCollisionArraySize) {}
		~Bucket() {}

		void load(OId oId, uint64_t arrayIndex, uint8_t forUpdate) {
			BaseObject::load(oId, forUpdate);
			moveCursor(ARRAY_OBJECT_HEADER_SIZE + sizeof(OId) * arrayIndex);
		}

		OId getBucketOId() {
			if (getBaseOId() == UNDEF_OID) {
				return UNDEF_OID;
			}
			else {
				return *getCursor<OId>();
			}
		}

		void setBucketOId(OId oId) {
			*getCursor<OId>() = oId;
		}

		/*!
			@brief Cursor for Bucket
		*/
		struct Cursor {
			HashArrayObject array_;  
			uint32_t size_;			 
			uint32_t iterator_;
			size_t bucketSize_;
			Cursor(TransactionContext& txn, ObjectManager& objectManager)
				: array_(txn, objectManager),
				  size_(0),
				  iterator_(-1),
				  bucketSize_(0) {}

		private:
			Cursor(const Cursor&);  
			Cursor& operator=(const Cursor&);  
		};

		uint32_t getMaxCollisionArraySize() const {
			return maxArraySize_ - 2;
		}

		void set(TransactionContext& txn, Cursor& cursor,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			OId bucketOId = getBucketOId();
			if (bucketOId == UNDEF_OID) {
				;  
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				cursor.array_.copyReference(
					getBaseOId(), getCursor<uint8_t>());  
				cursor.size_ = 1;
				cursor.iterator_ = -1;
				cursor.bucketSize_ = 1;
			}
			else {
				getNextCollisionArray(txn, cursor, bucketOId, forUpdate);
				cursor.bucketSize_ = getBucketSize(cursor.array_);
			}
		}

		bool next(TransactionContext& txn, Cursor& cursor,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			cursor.iterator_++;
			if (cursor.iterator_ == cursor.size_) {
				if (!nextCollisionArray(txn, cursor, forUpdate)) {
					return false;
				}
				cursor.iterator_++;
			}
			return true;
		}

		OId getCurrentOId(Cursor& cursor) {
			return cursor.array_.getElemOId(cursor.iterator_);
		}
		void setCurrentOId(Cursor& cursor, OId oId) {
			cursor.array_.setElemOId(cursor.iterator_, oId);
		}

		size_t append(TransactionContext& txn, OId oId) {
			OId bucketOId = getBucketOId();
			if (bucketOId == UNDEF_OID) {
				setBucketOId(oId);
				return 1;
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				OId prevOId = bucketOId;
				createCollisionArray(txn, prevOId, oId);
				return 2;
			}
			else {
				return appendCollisionArray(txn, oId);
			}
		}

		bool remove(TransactionContext& txn, OId oId, size_t& bucketSize) {
			OId bucketOId = getBucketOId();
			bucketSize = 0;
			if (bucketOId == UNDEF_OID) {
				;  
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				if (bucketOId == oId) {
					setBucketOId(UNDEF_OID);
					return true;
				}
				else {
					bucketSize = 1;
				}
			}
			else {
				HashArrayObject topHashObject(txn, *getObjectManager());
				getCollisionArray(txn, bucketOId, topHashObject);

				Cursor cursor(txn, *getObjectManager());
				set(txn, cursor);
				bucketSize = cursor.bucketSize_;
				uint32_t topCollisionArraySize = cursor.size_;
				while (next(txn, cursor)) {
					if (getCurrentOId(cursor) == oId) {
						removeCollisionArray(txn, topHashObject,
							topCollisionArraySize, cursor.array_,
							cursor.iterator_, bucketSize);
						bucketSize--;
						return true;
					}
				}
			}
			return false;
		}

		size_t merge(TransactionContext& txn, Bucket& sourceBucket,
			size_t maxBucketSize) {
			if (sourceBucket.getBucketOId() == UNDEF_OID) {
				return 0;
			}
			else if (!isCollisionArrayOId(sourceBucket.getBucketOId())) {
				if (getBucketSize(txn) + 1 >= maxBucketSize) {
					return 0;
				}
				else {
					size_t mergedSize =
						append(txn, sourceBucket.getBucketOId());
					sourceBucket.setBucketOId(UNDEF_OID);
					return mergedSize;
				}
			}
			else {
				size_t sourceBucketSize = sourceBucket.getBucketSize(txn);
				if (getBucketOId() == UNDEF_OID) {
					if (sourceBucketSize >= maxBucketSize) {
						return 0;
					}

					setBucketOId(sourceBucket.getBucketOId());
					sourceBucket.setBucketOId(UNDEF_OID);
					return sourceBucketSize;
				}
				else if (!isCollisionArrayOId(getBucketOId())) {
					if (sourceBucketSize + 1 >= maxBucketSize) {
						return 0;
					}

					OId oId = getBucketOId();
					setBucketOId(sourceBucket.getBucketOId());
					sourceBucket.setBucketOId(UNDEF_OID);
					append(txn, oId);
					return sourceBucketSize + 1;
				}
				else {
					HashArrayObject topHashArrayObject(
						txn, *getObjectManager());
					getCollisionArray(txn, getBucketOId(), topHashArrayObject);
					uint32_t bucketSize = static_cast<uint32_t>(
						getBucketSize(topHashArrayObject) + sourceBucketSize);

					if (bucketSize >= maxBucketSize) {
						return 0;
					}

					uint32_t topOIdListSize =
						getCollisionArraySize(topHashArrayObject);
					return mergeCollisionArray(txn, bucketSize,
						topHashArrayObject, topOIdListSize, sourceBucket);
				}
			}
		}

		void clear(TransactionContext& txn) {
			OId bucketOId = getBucketOId();
			if (bucketOId == UNDEF_OID) {
				;  
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				setBucketOId(UNDEF_OID);
			}
			else {
				HashArrayObject hashArrayObject(txn, *getObjectManager());
				getCollisionArray(txn, bucketOId, hashArrayObject);
				uint32_t maxSize = getCollisionArrayMaxSize(hashArrayObject);
				if (maxSize < maxCollisionArraySize_) {
					hashArrayObject.finalize();
				}
				else {
					OId linkOId = getLinkOId(hashArrayObject);
					hashArrayObject.finalize();
					while (linkOId != UNDEF_OID) {
						HashArrayObject hashArrayObject(
							txn, *getObjectManager());
						getCollisionArray(txn, linkOId, hashArrayObject);
						linkOId = getLinkOId(hashArrayObject);
						hashArrayObject.finalize();
					}
				}
				setBucketOId(UNDEF_OID);
			}
		}

		size_t getBucketSize(TransactionContext& txn) {
			OId bucketOId = getBucketOId();
			if (bucketOId == UNDEF_OID) {
				return 0;
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				return 1;
			}
			else {
				HashArrayObject pTopHashArrayObject(txn, *getObjectManager());
				getCollisionArray(
					txn, getBucketOId(), pTopHashArrayObject, OBJECT_READ_ONLY);
				return getBucketSize(pTopHashArrayObject);
			}
		}

		std::string dump(TransactionContext& txn, int64_t bucketId,
			int64_t& bucketNum, int64_t& objectNum) {
			util::NormalOStringStream strstrm;
			bucketNum = 0;
			objectNum = 0;
			bool isDumpOIdData = false;

			OId bucketOId = getBucketOId();
			if (bucketOId == UNDEF_OID) {
				strstrm << "[" << bucketId << ", "
						<< "( -1 ), 0, 0] ";
				strstrm << std::endl;
			}
			else if (!isCollisionArrayOId(bucketOId)) {
				bucketNum = 0;
				objectNum = 1;
				strstrm << "[" << bucketId << ", "
						<< "( -1 ), " << objectNum << ", " << bucketNum << "] ";
				strstrm << std::endl;
			}
			else {
				util::NormalOStringStream strstrm2;
				Cursor cursor(txn, *getObjectManager());
				set(txn, cursor);
				objectNum = cursor.bucketSize_;
				bucketNum = 1;
				if (objectNum >= maxCollisionArraySize_) {
					bucketNum += getLinkNum(cursor.array_);
				}

				while (next(txn, cursor)) {
					OId oId = getCurrentOId(cursor);
					strstrm2 << ", " << oId;
					if (isDumpOIdData) {
						BaseObject object(txn.getPartitionId(),
							*getObjectManager(), resetModuleFlagOId(oId));
						strstrm2 << "(" << *object.getBaseAddr<int64_t*>()
								 << ")";
					}
				}

				strstrm << "[" << bucketId << ", "
						<< "(" << getBucketOId() << "), " << objectNum << ", "
						<< bucketNum << "] ";

				strstrm << strstrm2.str().c_str();
				strstrm << std::endl;
			}

			return strstrm.str().c_str();
		}


		bool nextCollisionArray(TransactionContext& txn, Cursor& cursor,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			if (cursor.bucketSize_ <= maxCollisionArraySize_) {
				return false;
			}
			OId linkOId = getLinkOId(cursor.array_);
			if (linkOId == UNDEF_OID) {
				return false;
			}
			getNextCollisionArray(txn, cursor, linkOId, forUpdate);
			return true;
		}

		void createCollisionArray(TransactionContext& txn, OId oId0, OId oId1) {
			OId newArrayOId = UNDEF_OID;
			uint32_t actualSize = 0;
			HashArrayObject hashArrayObject(txn, *getObjectManager());
			allocateArrayObject(
				txn, 2, newArrayOId, actualSize, hashArrayObject);
			setBucketOId(setModuleFlagOId(newArrayOId));
			hashArrayObject.setElemOId(0, oId0);
			hashArrayObject.setElemOId(1, oId1);
			hashArrayObject.setArraySize(2);
		}

		size_t appendCollisionArray(TransactionContext& txn, OId oId) {
			HashArrayObject hashArrayObject(txn, *getObjectManager());
			getCollisionArray(txn, getBucketOId(), hashArrayObject);
			size_t bucketSize = getBucketSize(hashArrayObject);
			uint32_t collisionArraySize =
				getCollisionArraySize(hashArrayObject);

			if (bucketSize >= maxCollisionArraySize_ &&
				collisionArraySize == maxCollisionArraySize_) {
				HashArrayObject newHashArrayObject(txn, *getObjectManager());
				linkNewCollisionArray(txn, hashArrayObject, newHashArrayObject);
				collisionArraySize = 0;
				newHashArrayObject.setElemOId(collisionArraySize, oId);
				collisionArraySize++;
				setCollisionArraySize(newHashArrayObject, collisionArraySize);
			}
			else {
				if (bucketSize < maxCollisionArraySize_) {
					size_t collisionArrayMaxSize =
						getCollisionArrayMaxSize(hashArrayObject);
					if (collisionArraySize == collisionArrayMaxSize) {
						twiceCollisionArray(txn, hashArrayObject);
						collisionArraySize =
							getCollisionArraySize(hashArrayObject);
					}
				}
				hashArrayObject.setElemOId(collisionArraySize, oId);
				collisionArraySize++;
				setCollisionArraySize(hashArrayObject, collisionArraySize);
			}
			return bucketSize + 1;
		}

		void removeCollisionArray(TransactionContext& txn,
			HashArrayObject& topHashArrayObject, uint32_t topCollisionArraySize,
			HashArrayObject& hashArrayObject, size_t iterator,
			size_t bucketSize) {
			hashArrayObject.setElemOId(iterator,
				topHashArrayObject.getElemOId(topCollisionArraySize - 1));
			topCollisionArraySize--;
			setCollisionArraySize(topHashArrayObject, topCollisionArraySize);

			if (bucketSize<maxCollisionArraySize_>> 1) {
				if (bucketSize >= 3) {
					size_t collisionArrayMaxSize =
						getCollisionArrayMaxSize(hashArrayObject);
					if (topCollisionArraySize<collisionArrayMaxSize>> 1) {
						topHashArrayObject.reset();  
						halfCollisionArray(txn, hashArrayObject);
					}
				}
				else {
					topHashArrayObject.reset();  
					OId tempOId = hashArrayObject.getElemOId(0);
					hashArrayObject.finalize();
					setBucketOId(tempOId);
				}
			}
			else if (bucketSize <= maxCollisionArraySize_) {
				;  
			}
			else {
				;  

				if (topCollisionArraySize == 0) {
					unlinkCollisionArray(txn, topHashArrayObject);
				}
				else {
					;  
				}
			}
		}

		size_t mergeCollisionArray(TransactionContext& txn, uint32_t bucketSize,
			HashArrayObject& targetHashArrayObject, uint32_t targetSize,
			Bucket& sourceBucket) {
			if (bucketSize < maxCollisionArraySize_) {

				HashArrayObject sourceHashArrayObject(txn, *getObjectManager());
				getCollisionArray(
					txn, sourceBucket.getBucketOId(), sourceHashArrayObject);
				uint32_t sourceBucketSize = static_cast<uint32_t>(
					sourceBucket.getBucketSize(sourceHashArrayObject));

				mergeTopCollisionArray(txn, targetHashArrayObject, targetSize,
					sourceHashArrayObject, sourceBucketSize);

				sourceHashArrayObject.finalize();

				sourceBucket.setBucketOId(UNDEF_OID);

				return getBucketSize(targetHashArrayObject);
			}
			else {
				uint32_t maxSize =
					getCollisionArrayMaxSize(targetHashArrayObject);
				if (maxSize < maxCollisionArraySize_) {
					expandCollisionArray(
						txn, targetHashArrayObject, maxArraySize_);
				}

				HashArrayObject sourceHashArrayObject(txn, *getObjectManager());
				getCollisionArray(
					txn, sourceBucket.getBucketOId(), sourceHashArrayObject);
				size_t srcLinkNum = getLinkNum(sourceHashArrayObject);
				size_t destLinkNum = getLinkNum(targetHashArrayObject);

				Bucket::Cursor cursor(txn, *getObjectManager());
				set(txn, cursor);

				OId linkOId = getLinkOId(cursor.array_);
				while (linkOId != UNDEF_OID) {
					getNextCollisionArray(txn, cursor, linkOId);
					linkOId = getLinkOId(cursor.array_);
				}
				setLinkOId(cursor.array_, sourceBucket.getBucketOId());

				setLinkNum(targetHashArrayObject, srcLinkNum + destLinkNum);

				sourceBucket.setBucketOId(UNDEF_OID);

				return bucketSize;
			}
		}

		void mergeTopCollisionArray(TransactionContext& txn,
			HashArrayObject& targetHashArrayObject, uint32_t targetSize,
			const HashArrayObject& sourceHashArrayObject, uint32_t sourceSize) {
			uint32_t collisionArrayMaxSize =
				getCollisionArrayMaxSize(targetHashArrayObject);
			size_t copySize = sourceSize;

			if (collisionArrayMaxSize - targetSize < copySize) {
				if (collisionArrayMaxSize < maxCollisionArraySize_) {
					uint32_t expandSize = targetSize + sourceSize;
					if (expandSize <= maxCollisionArraySize_) {
						;  
					}
					else {
						expandSize = maxArraySize_;
						copySize = maxCollisionArraySize_ - targetSize;
					}
					expandCollisionArray(
						txn, targetHashArrayObject, expandSize);
				}
				else {
					copySize = collisionArrayMaxSize - targetSize;
				}
			}

			targetHashArrayObject.copyArray(
				sourceHashArrayObject, 0, targetSize, copySize);
			setCollisionArraySize(targetHashArrayObject,
				static_cast<uint32_t>(targetSize + copySize));

			if (copySize < sourceSize) {
				linkNewCollisionArray(
					txn, targetHashArrayObject, targetHashArrayObject);
				targetHashArrayObject.copyArray(sourceHashArrayObject, copySize,
					0, (sourceSize - copySize));
				setCollisionArraySize(targetHashArrayObject,
					static_cast<uint32_t>(sourceSize - copySize));
			}

			return;
		}


		void getCollisionArray(TransactionContext& txn, const OId oId,
			HashArrayObject& pHashArrayObject,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			getArrayObject(txn, oId, pHashArrayObject, forUpdate);
			return;
		}

		void getNextCollisionArray(TransactionContext& txn, Cursor& cursor,
			const OId oId, uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			getCollisionArray(txn, oId, cursor.array_, forUpdate);
			cursor.size_ = getCollisionArraySize(cursor.array_);
			cursor.iterator_ = -1;
		}

		void expandCollisionArray(TransactionContext& txn,
			HashArrayObject& pTopHashArrayObject, uint32_t newSize) {
			assert(newSize <= maxArraySize_);

			uint32_t maxSize = getCollisionArrayMaxSize(pTopHashArrayObject);
			OId newArrayOId = UNDEF_OID;
			uint32_t actualSize = 0;
			HashArrayObject pNewHashArrayObject(txn, *getObjectManager());
			allocateArrayObject(
				txn, newSize, newArrayOId, actualSize, pNewHashArrayObject);
			pNewHashArrayObject.copyArray(pTopHashArrayObject, 0, 0, maxSize);
			pNewHashArrayObject.setArraySize(maxSize);
			if (newSize == maxArraySize_) {
				setLinkNum(pNewHashArrayObject, 0);
				setLinkOId(pNewHashArrayObject, UNDEF_OID);
			}
			pTopHashArrayObject.finalize();
			pTopHashArrayObject.load(pNewHashArrayObject.getBaseOId());
			setBucketOId(setModuleFlagOId(newArrayOId));
			return;
		}

		void twiceCollisionArray(
			TransactionContext& txn, HashArrayObject& pTopHashArrayObject) {
			uint32_t maxSize = getCollisionArrayMaxSize(pTopHashArrayObject);
			uint32_t newSize = maxSize << 1;
			if (newSize > maxCollisionArraySize_) {
				newSize = maxArraySize_;
			}
			expandCollisionArray(txn, pTopHashArrayObject, newSize);
			return;
		}

		void halfCollisionArray(
			TransactionContext& txn, HashArrayObject& pTopHashArrayObject) {
			uint32_t newSize = HashMap::align(
				getCollisionArrayMaxSize(pTopHashArrayObject) >> 1);
			;
			if (newSize == 0) {
				return;
			}

			OId newArrayOId = UNDEF_OID;
			uint32_t actualSize = 0;
			HashArrayObject pNewHashArrayObject(txn, *getObjectManager());
			allocateArrayObject(
				txn, newSize, newArrayOId, actualSize, pNewHashArrayObject);
			pNewHashArrayObject.copyArray(
				pTopHashArrayObject, 0, 0, actualSize);
			pNewHashArrayObject.setArraySize(
				pTopHashArrayObject.getArraySize());
			pTopHashArrayObject.finalize();
			pTopHashArrayObject.load(pNewHashArrayObject.getBaseOId());
			setBucketOId(setModuleFlagOId(newArrayOId));
			return;
		}

		void linkNewCollisionArray(TransactionContext& txn,
			const HashArrayObject& pTopHashArrayObject,
			HashArrayObject& pNewHashArrayObject) {
			OId newArrayOId = UNDEF_OID;
			uint32_t actualSize = 0;
			allocateArrayObject(txn, maxArraySize_, newArrayOId, actualSize,
				pNewHashArrayObject);
			setLinkOId(pNewHashArrayObject, getBucketOId());

			size_t num = getLinkNum(pTopHashArrayObject);
			setLinkNum(pNewHashArrayObject, num + 1);
			setBucketOId(setModuleFlagOId(newArrayOId));
			return;
		}

		void unlinkCollisionArray(
			TransactionContext& txn, HashArrayObject& pTopHashArrayObject) {
			setBucketOId(getLinkOId(pTopHashArrayObject));

			HashArrayObject pPostHashArrayObject(txn, *getObjectManager());
			getArrayObject(
				txn, getBucketOId(), pPostHashArrayObject, OBJECT_FOR_UPDATE);
			size_t num = getLinkNum(pTopHashArrayObject);
			setLinkNum(pPostHashArrayObject, num - 1);
			pTopHashArrayObject.finalize();
			return;
		}

		bool isCollisionArrayOId(OId oId) {
			return (isModuleFlagOId(oId) == true);
		}

		size_t getBucketSize(const HashArrayObject& pTopHashArrayObject) {
			uint32_t maxSize = pTopHashArrayObject.getArrayMaxSize();

			if (maxSize < maxArraySize_) {
				return pTopHashArrayObject.getArraySize();
			}
			else {
				return pTopHashArrayObject.getArraySize() +
					   getLinkNum(pTopHashArrayObject) * maxCollisionArraySize_;
			}
		}

		uint32_t getCollisionArrayMaxSize(
			const HashArrayObject& pHashArrayObject) {
			uint32_t maxSize = pHashArrayObject.getArrayMaxSize();
			if (maxSize < maxArraySize_) {
				return pHashArrayObject.getArrayMaxSize();
			}
			else {
				return maxCollisionArraySize_;
			}
		}

		uint32_t getCollisionArraySize(
			const HashArrayObject& pHashArrayObject) {
			return pHashArrayObject.getArraySize();
		}

		void setCollisionArraySize(
			HashArrayObject& pHashArrayObject, uint32_t size) {
			pHashArrayObject.setArraySize(size);
		}


		OId getLinkOId(const HashArrayObject& pHashArrayObject) {
			return pHashArrayObject.getElemOId(
				pHashArrayObject.getArrayMaxSize() - 1);
		}
		void setLinkOId(HashArrayObject& pHashArrayObject, OId oId) {
			pHashArrayObject.setElemOId(
				pHashArrayObject.getArrayMaxSize() - 1, oId);
		}

		size_t getLinkNum(const HashArrayObject& pHashArrayObject) {
			assert(pHashArrayObject.getArrayMaxSize() == maxArraySize_);
			return static_cast<size_t>(pHashArrayObject.getElemOId(
				pHashArrayObject.getArrayMaxSize() - 2));
		}

		void setLinkNum(HashArrayObject& pHashArrayObject,
			size_t size) {  
			assert(pHashArrayObject.getArrayMaxSize() == maxArraySize_);
			pHashArrayObject.setElemOId(
				pHashArrayObject.getArrayMaxSize() - 2, static_cast<OId>(size));
		}
	};

private:
	/*!
		@brief Hash array format
	*/
	struct HashArrayImage {
		OId oId_;
		uint64_t size_;		 
		uint8_t dimension_;  
	};
	/*!
		@brief Hash Map format
	*/
	struct HashMapImage {
		uint32_t thresholdSplit_;  
		ColumnId columnId_;		   
		ColumnType keyType_;
		uint8_t isUnique_;
		uint8_t toSplit_;  
		uint8_t toMerge_;  
		uint32_t padding1_;
		uint64_t size_;  
		uint64_t split_;  
		uint64_t front_;  
		uint64_t rear_;  
		HashArrayImage hashArrayImage_;  
		uint64_t padding2_;
		uint64_t padding3_;
		uint64_t padding4_;
		uint64_t padding5_;
		uint64_t padding6_;
		uint64_t padding7_;
	};

private:
	HashMapImage* hashMapImage_;
	HashArray hashArray_;

	static const uint64_t MAGIC_NUMBER = 0x0028000000000000ULL;

	static const uint64_t MODULE_NUMBER = 0x0010000000000000ULL;

	static const uint64_t MASK_MAGIC = 0x0038000000000000ULL;

	static const uint64_t RESET_MAGIC_MASK = 0xFFC7FFFFFFFFFFFFULL;

	inline static bool isModuleFlagOId(OId oId) {
		return ((MASK_MAGIC & oId) == MODULE_NUMBER);
	}

	inline static OId setModuleFlagOId(OId oId) {
		OId tmpOId = (RESET_MAGIC_MASK & oId);
		return (MODULE_NUMBER | tmpOId);
	}

	inline static OId resetModuleFlagOId(OId oId) {
		OId tmpOId = (RESET_MAGIC_MASK & oId);
		return (MAGIC_NUMBER | tmpOId);
	}

public:
	/*!
		@brief Information related to a search
	*/
	struct SearchContext : BaseIndex::SearchContext {
		const void* key_;   
		uint32_t keySize_;  

		SearchContext() : BaseIndex::SearchContext(), key_(NULL), keySize_(0) {}

		SearchContext(ColumnId columnId, const void* key, uint32_t keySize,
			uint32_t conditionNum, TermCondition* conditionList,
			ResultSize limit)
			: BaseIndex::SearchContext(
				  columnId, conditionNum, conditionList, limit),
			  key_(key),
			  keySize_(keySize) {}
	};

	/*!
		@brief Cursor for search
	*/
	struct HashCursor {
		OId currentArrayOId_;		 
		uint32_t currentArraysize_;  
		uint32_t bucketIterator_;
		size_t bucketSize_;
		uint64_t rearCursor_;
		bool isContinue_;
		HashCursor() {
			currentArrayOId_ = UNDEF_OID;
			currentArraysize_ = 0;
			bucketIterator_ = 0;
			bucketSize_ = 0;
			rearCursor_ = 0;
			isContinue_ = false;
		}
		void set(TransactionContext& txn, Bucket::Cursor& cursor,
			uint8_t forUpdate = OBJECT_FOR_UPDATE) {
			getArrayObject(txn, currentArrayOId_, cursor.array_, forUpdate);
			cursor.size_ = currentArraysize_;
			cursor.iterator_ = bucketIterator_;
			cursor.bucketSize_ = bucketSize_;
		}
	};

	int32_t initialize(TransactionContext& txn, ColumnType columnType,
		ColumnId columnId, const AllocateStrategy& metaAllocateStrategy,
		bool isUnique = false);

	int32_t finalize(TransactionContext& txn);

	bool isEmpty() {
		return hashMapImage_->size_ == 0;
	}

	uint64_t size() {
		return hashMapImage_->size_;
	}

	MapType mapType() {
		return MAP_TYPE_HASH;
	}

	int32_t insert(TransactionContext& txn, const void* key, OId oId);
	int32_t remove(TransactionContext& txn, const void* key, OId oId);
	int32_t update(
		TransactionContext& txn, const void* key, OId oId, OId newOId);
	int32_t search(
		TransactionContext& txn, const void* key, uint32_t size, OId& oId);
	int32_t search(TransactionContext& txn, const void* key, uint32_t size,
		ResultSize limit, util::XArray<OId>& idList);
	int32_t getAll(
		TransactionContext& txn, ResultSize limit, util::XArray<OId>& idList);
	int32_t getAll(TransactionContext& txn, ResultSize limit,
		util::XArray<OId>& idList, HashCursor& cursor);

	std::string dump(TransactionContext& txn, uint8_t mode);
	std::string validate(TransactionContext& txn);

	HashMap(TransactionContext& txn, ObjectManager& objectManager,
		const AllocateStrategy& strategy, BaseContainer* container)
		: BaseIndex(txn, objectManager, strategy, container),
		  maxObjectSize_(objectManager.getMaxObjectSize()),
		  maxArrayObjectSize_(maxObjectSize_ - ARRAY_OBJECT_HEADER_SIZE),
		  maxArraySize_(maxArrayObjectSize_ >> OID_BIT_SIZE),
		  maxHashArraySize_((uint64_t)maxArraySize_ * (uint64_t)maxArraySize_ *
							(uint64_t)maxArraySize_),
		  maxCollisionArraySize_(maxArraySize_ - 2),
		  ThresholdMergeLimit_(maxObjectSize_ >> 4),
		  ThresholdSplitNonString(
			  (15 < ThresholdMergeLimit_) ? 15 : ThresholdMergeLimit_),
		  hashMapImage_(NULL),
		  hashArray_(&objectManager, NULL, maxArraySize_) {}
	HashMap(TransactionContext& txn, ObjectManager& objectManager, OId oId,
		const AllocateStrategy& strategy, BaseContainer* container)
		: BaseIndex(txn, objectManager, oId, strategy, container),
		  maxObjectSize_(objectManager.getMaxObjectSize()),
		  maxArrayObjectSize_(maxObjectSize_ - ARRAY_OBJECT_HEADER_SIZE),
		  maxArraySize_(maxArrayObjectSize_ >> OID_BIT_SIZE),
		  maxHashArraySize_((uint64_t)maxArraySize_ * (uint64_t)maxArraySize_ *
							(uint64_t)maxArraySize_),
		  maxCollisionArraySize_(maxArraySize_ - 2),
		  ThresholdMergeLimit_(maxObjectSize_ >> 4),
		  ThresholdSplitNonString(
			  (15 < ThresholdMergeLimit_) ? 15 : ThresholdMergeLimit_),
		  hashMapImage_(reinterpret_cast<HashMapImage*>(getBaseAddr())),
		  hashArray_(&objectManager, &(hashMapImage_->hashArrayImage_),
			  maxArraySize_) {}
	~HashMap() {}

private:
	template <class T>
	int32_t insertObject(
		TransactionContext& txn, const T key, uint32_t size, OId oId);
	template <class T>
	int32_t removeObject(
		TransactionContext& txn, const T key, uint32_t size, OId oId);
	template <class T>
	int32_t updateObject(TransactionContext& txn, const T key, uint32_t size,
		OId oId, OId newOId);
	template <class T>
	OId searchObject(TransactionContext& txn, const T key, uint32_t size);
	template <class T>
	int32_t searchObject(TransactionContext& txn, const T key, uint32_t size,
		ResultSize limit, util::XArray<OId>& idList);

	template <class T>
	void split(TransactionContext& txn);
	template <class T>
	void merge(TransactionContext& txn);

	template <class T>
	bool isEquals(TransactionContext& txn, T key, uint32_t, OId oId) {
		T* value = getField<T>(txn, oId);
		return key == *value;
	}

	template <class T>
	uint32_t hashBucketAddr(T key, uint32_t) {
		uint64_t hash = static_cast<uint64_t>(key * 2654435761U);

		if (hash % (hashMapImage_->front_ << 1) >= hashMapImage_->rear_) {
			return static_cast<uint32_t>(hash % hashMapImage_->front_);
		}
		return static_cast<uint32_t>(hash % (hashMapImage_->front_ << 1));
	}

	template <class T>
	uint32_t hashBucketAddrFromObject(TransactionContext& txn, OId oId) {
		return hashBucketAddr<T>(*getField<T>(txn, oId), 0);
	}

	void checkAddr(TransactionContext& txn, uint32_t addr, Bucket& bucket) {
		switch (hashMapImage_->keyType_) {
		case COLUMN_TYPE_STRING:
			return checkHashAddr<uint8_t*>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_BOOL:
			return checkHashAddr<bool>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_BYTE:
			return checkHashAddr<int8_t>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_SHORT:
			return checkHashAddr<int16_t>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_INT:
			return checkHashAddr<int32_t>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_LONG:
			return checkHashAddr<int64_t>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_OID:
			return checkHashAddr<uint64_t>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_FLOAT:
			return checkHashAddr<float32>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_DOUBLE:
			return checkHashAddr<float64>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_TIMESTAMP:
			return checkHashAddr<Timestamp>(txn, addr, bucket);
			break;
		case COLUMN_TYPE_BLOB:
			return checkHashAddr<uint8_t>(txn, addr, bucket);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HM_UNEXPECTED_ERROR, "");
			break;
		}
	}

	template <class T>
	void checkHashAddr(TransactionContext& txn, uint32_t addr, Bucket& bucket) {
		if (bucket.getBucketOId() == UNDEF_OID) {
			;
		}
		else if (isModuleFlagOId(bucket.getBucketOId()) == false) {
			OId oId = bucket.getBucketOId();  
			uint32_t addr2 = hashBucketAddrFromObject<T>(txn, oId);
			if (addr != addr2) {
				std::cout << "invalid bucket addr for oId=" << oId
						  << ". expected=" << addr << ", actual=" << addr2
						  << std::endl;
			}
		}
		else {
			HashArrayObject hashArrayObject(txn, *getObjectManager());
			getArrayObject(
				txn, bucket.getBucketOId(), hashArrayObject, OBJECT_READ_ONLY);
			uint32_t arraySize = hashArrayObject.getArraySize();

			for (uint32_t j = 0; j < arraySize; j++) {
				uint32_t addr2 = hashBucketAddrFromObject<T>(
					txn, hashArrayObject.getElemOId(j));
				if (addr != addr2) {
					std::cout << "invalid bucket addr for oId="
							  << hashArrayObject.getElemOId(j)
							  << ". expected=" << addr << ", actual=" << addr2
							  << std::endl;
				}
			}
		}
	}

	template <class T>
	T* getField(TransactionContext& txn, OId oId);

	bool isUnique() {
		return hashMapImage_->isUnique_ != 0;
	}
	inline static uint32_t align(uint32_t x) {
		x = x - 1;
		x = x | (x >> 1);
		x = x | (x >> 2);
		x = x | (x >> 4);
		x = x | (x >> 8);
		x = x | (x >> 16);

		return x + 1;
	}

	std::string dumpParameters() {
		util::NormalOStringStream strstrm;
		strstrm << "ARRAY_OBJECT_HEADER_SIZE, " << ARRAY_OBJECT_HEADER_SIZE
				<< ", "
				<< "OID_BIT_SIZE, " << OID_BIT_SIZE << ", "
				<< "INITIAL_HASH_ARRAY_SIZE, " << INITIAL_HASH_ARRAY_SIZE
				<< ", "
				<< "INITIAL_BUCKET_SIZE, " << INITIAL_BUCKET_SIZE << ", "
				<< "thresholdSplit_, " << hashMapImage_->thresholdSplit_ << ", "
				<< "THRESHOLD_MERGE, " << THRESHOLD_MERGE << ", "
				<< "ThresholdMergeLimit_, " << ThresholdMergeLimit_ << ", "
				<< "maxObjectSize_, " << maxObjectSize_ << ", "
				<< "maxArrayObjectSize_, " << maxArrayObjectSize_ << ", "
				<< "maxArraySize_, " << maxArraySize_ << ", "
				<< "maxHashArraySize_, " << maxHashArraySize_;

		return strstrm.str();
	}
};

template <>
inline bool HashMap::isEquals<uint8_t*>(
	TransactionContext& txn, uint8_t* key, uint32_t size, OId oId) {
	StringCursor value(getField<uint8_t>(txn, oId));
	return compareStringString(
			   txn, key, size, value.str(), value.stringLength()) == 0;
}

template <>
inline bool HashMap::isEquals<double>(
	TransactionContext& txn, double key, uint32_t, OId oId) {
	double value = *getField<double>(txn, oId);
	if (isnan(key)) {
		if (isnan(value)) {
			return true;
		}
		else {
			return false;
		}
	}
	else if (isnan(value)) {
		return false;
	}
	else {
		return key == value;
	}
}
template <>
inline bool HashMap::isEquals<float>(
	TransactionContext& txn, float key, uint32_t, OId oId) {
	float value = *getField<float>(txn, oId);
	if (isnan(key)) {
		if (isnan(value)) {
			return true;
		}
		else {
			return false;
		}
	}
	else if (isnan(value)) {
		return false;
	}
	else {
		return key == value;
	}
}

template <>
inline uint32_t HashMap::hashBucketAddr<uint8_t*>(
	uint8_t* keyBinary, uint32_t size) {
	uint64_t hash = 0;
	uint8_t* key = keyBinary;
	uint32_t keylen = size;
	for (hash = 0; --keylen != UINT32_MAX; hash = hash * 37 + *(key)++)
		;
	if (hash % (hashMapImage_->front_ << 1) >= hashMapImage_->rear_) {
		return static_cast<uint32_t>(hash % hashMapImage_->front_);
	}
	return static_cast<uint32_t>(hash % (hashMapImage_->front_ << 1));
}

template <>
inline uint32_t HashMap::hashBucketAddrFromObject<uint8_t*>(
	TransactionContext& txn, OId oId) {
	uint8_t* value = getField<uint8_t>(txn, oId);
	StringCursor stringCursor(value);
	return hashBucketAddr<uint8_t*>(
		reinterpret_cast<uint8_t*>(stringCursor.str()),
		stringCursor.stringLength());
}

#endif
