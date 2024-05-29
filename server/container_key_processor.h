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
	@brief Definition of FullContainerKeyCursor
*/

#ifndef FULL_CONTAINER_KEY_PROCESSOR_H_
#define FULL_CONTAINER_KEY_PROCESSOR_H_

#include "util/container.h"
#include "container_key.h"
#include "value_processor.h"

class FullContainerKeyCursor : public BaseObject {
public:
	FullContainerKeyCursor(
		ObjectManagerV4& objectManager, AllocateStrategy& strategy, OId oId) :
		BaseObject(objectManager, strategy, oId),
		body_(NULL), bodySize_(0), key_(NULL) {
		bodySize_ = ValueProcessor::decodeVarSize(getBaseAddr());
		body_ = getBaseAddr() + ValueProcessor::getEncodedVarSize(bodySize_);  
	}
	FullContainerKeyCursor(
		ObjectManagerV4& objectManager) :
		BaseObject(objectManager), body_(NULL), bodySize_(0), key_(NULL) {};
	FullContainerKeyCursor(
		ObjectManagerV4& objectManager, AllocateStrategy& strategy) :
		BaseObject(objectManager, strategy), body_(NULL), bodySize_(0), key_(NULL) {};
	FullContainerKeyCursor(FullContainerKey* key) :
		BaseObject(NULL), body_(NULL), bodySize_(0), key_(key) {
		const void* srcBody;
		size_t srcSize;
		key->toBinary(srcBody, srcSize);
		body_ = const_cast<uint8_t*>(static_cast<const uint8_t*>(srcBody));
		bodySize_ = srcSize;
	}
	void initialize(TransactionContext& txn, const FullContainerKey& src) {
		UNUSED_VARIABLE(txn);
		const void* srcBody;
		size_t srcSize;
		src.toBinary(srcBody, srcSize);
		bodySize_ = srcSize;

		uint32_t headerSize = ValueProcessor::getEncodedVarSize(bodySize_);  

		OId oId;
		allocate<uint8_t>(
				static_cast<DSObjectSize>(headerSize + bodySize_), oId,
				OBJECT_TYPE_VARIANT);

		uint64_t encodedLength = ValueProcessor::encodeVarSize(bodySize_);
		memcpy(getBaseAddr(), &encodedLength, headerSize);
		body_ = getBaseAddr() + headerSize;
		memcpy(body_, static_cast<const uint8_t*>(srcBody), bodySize_);
	}
	void finalize() {
		BaseObject::finalize();
		body_ = NULL;
		bodySize_ = 0;
		key_ = NULL;
	}
	FullContainerKey getKey() const {
		if (key_ == NULL) {
			return FullContainerKey(KeyConstraint(), getBaseAddr());
		}
		else {
			return *key_;
		}
	}
	const uint8_t* getKeyBody() const {
		return body_;
	}


private:
	uint8_t* body_;
	uint64_t bodySize_;
	FullContainerKey* key_;
};

#endif
