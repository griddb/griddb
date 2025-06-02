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
	@brief Definition of BtreeMap
*/
#ifndef BTREE_MAP_H_
#define BTREE_MAP_H_

#include "util/trace.h"
#include "base_index.h"
#include "data_type.h"
#include "gs_error.h"
#include "object_manager_v4.h"
#include "value_processor.h"  
#include "value.h"			   
#include "value_operator.h"
#include "container_key_processor.h"
#include <iomanip>
#include <iostream>

UTIL_TRACER_DECLARE(BTREE_MAP);

class TransactionContext;
class BaseContainer;


/*!
	@brief String-type search key
*/
struct StringObject {
	StringObject(uint8_t *ptr) : ptr_(ptr) {}
	StringObject() : ptr_(NULL) {}
	uint8_t *ptr_;
};

/*!
	@brief For String-type Btree key
*/
struct StringKey {
	OId oId_;
	friend std::ostream &operator<<(std::ostream &output, const StringKey &kv) {
		output << kv.oId_;
		return output;
	}
};

template<typename T>
struct BaseTimestampKey {
	friend std::ostream& operator<<(
			std::ostream &output, const BaseTimestampKey &key) {
		ValueProcessor::dumpRawTimestamp(output, key.base_);
		return output;
	}
	bool operator==(const BaseTimestampKey &key) const {
		return ValueProcessor::compareTimestamp(base_, key.base_) == 0;
	}
	bool operator<(const BaseTimestampKey &key) const {
		return ValueProcessor::compareTimestamp(base_, key.base_) < 0;
	}

	T base_;
};

struct MicroTimestampKey : public BaseTimestampKey<MicroTimestamp> {
};
struct NanoTimestampKey : public BaseTimestampKey<NanoTimestamp> {
};

struct FullContainerKeyObject {
	FullContainerKeyObject(const FullContainerKeyCursor *ptr) : ptr_(reinterpret_cast<const uint8_t *>(ptr)) {}
	FullContainerKeyObject() : ptr_(NULL) {}
	const uint8_t *ptr_;
};



class ColumnSchema;
class CompositeInfoObject;
class BaseIndex;
typedef BaseIndex::Setting Setting;

class CompositeInfoObject8 : public CompositeInfoObject{
	uint8_t data_[8];
};
class CompositeInfoObject16 : public CompositeInfoObject{
	uint8_t data_[16];
};
class CompositeInfoObject24 : public CompositeInfoObject {
	uint8_t data_[24];
};
class CompositeInfoObject32 : public CompositeInfoObject {
	uint8_t data_[32];
};
class CompositeInfoObject40 : public CompositeInfoObject {
	uint8_t data_[40];
};
class CompositeInfoObject48 : public CompositeInfoObject {
	uint8_t data_[48];
};
class CompositeInfoObject56 : public CompositeInfoObject {
	uint8_t data_[56];
};
class CompositeInfoObject64 : public CompositeInfoObject {
	uint8_t data_[64];
};
class CompositeInfoObject72 : public CompositeInfoObject {
	uint8_t data_[72];
};
class CompositeInfoObject80 : public CompositeInfoObject {
	uint8_t data_[80];
};
class CompositeInfoObject88 : public CompositeInfoObject {
	uint8_t data_[88];
};
class CompositeInfoObject96 : public CompositeInfoObject {
	uint8_t data_[96];
};
class CompositeInfoObject104 : public CompositeInfoObject {
	uint8_t data_[104];
};
class CompositeInfoObject112 : public CompositeInfoObject {
	uint8_t data_[112];
};
class CompositeInfoObject120 : public CompositeInfoObject {
	uint8_t data_[120];
};
class CompositeInfoObject128 : public CompositeInfoObject {
	uint8_t data_[128];
};
class CompositeInfoObject136 : public CompositeInfoObject {
	uint8_t data_[136];
};

/*!
	@brief For String-type Btree key
*/
struct FullContainerKeyAddr {
	OId oId_;
	friend std::ostream &operator<<(std::ostream &output, const FullContainerKeyAddr &kv) {
		output << kv.oId_;
		return output;
	}
};

/*!
	@brief a pair of (Key, Value) For Btree
*/
template <typename K, typename V>
struct KeyValue {  
	K key_;
	V value_;

	KeyValue() : key_(), value_() {}
	KeyValue(const K &key, const V &val) : key_(key), value_(val) {}

	friend std::ostream &operator<<(
		std::ostream &output, const KeyValue<K, V> &kv) {
		output << "[" << kv.key_ << ",";
		output << kv.value_;
		output << "]";
		return output;
	}
};


/*!
	@brief Btree Map Index
*/
class BtreeMap : public BaseIndex {
public:
	/*!
		@brief Represents the type(s) of Btree
	*/
	enum BtreeMapType {
		TYPE_SINGLE_KEY,
		TYPE_UNIQUE_RANGE_KEY,
		TYPE_UNDEF_KEY,
	};

	BtreeMap(TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, BaseContainer *container = NULL,
		TreeFuncInfo *funcInfo = NULL)
		: BaseIndex(txn, objectManager, strategy, container, funcInfo, MAP_TYPE_BTREE),
		  nodeMaxSize_(NORMAL_MAX_ITEM_SIZE),
		  nodeMinSize_(NORMAL_MIN_ITEM_SIZE),
		  elemSize_(DEFAULT_ELEM_SIZE)
		  , isUnique_(false), keyType_(COLUMN_TYPE_WITH_BEGIN), btreeMapType_(TYPE_UNDEF_KEY)
	{}
	BtreeMap(TransactionContext &txn, ObjectManagerV4 &objectManager, OId oId,
		AllocateStrategy &strategy, BaseContainer *container = NULL,
		TreeFuncInfo *funcInfo = NULL)
		: BaseIndex(txn, objectManager, oId, strategy, container, funcInfo, MAP_TYPE_BTREE) {
		BNode<char, char> rootNode(this, allocateStrategy_);
		elemSize_ = rootNode.getNodeElemSize();
		nodeMaxSize_ = calcNodeMaxSize(elemSize_);
		nodeMinSize_ = calcNodeMinSize(elemSize_);
		isUnique_ = rootNode.isUnique();
		keyType_ = rootNode.getKeyType();
		btreeMapType_ = static_cast<BtreeMapType>(rootNode.getBtreeMapType());
	}
	~BtreeMap() {}

	static const int32_t ROOT_UPDATE = 0x80000000;  
	static const int32_t TAIL_UPDATE = 0x40000000;  
	static const ResultSize MINIMUM_SUSPEND_SIZE =
		2;  

	/*!
		@brief Represents the type(s) of dump
	*/
	enum {
		SUMMARY_HEADER_DUMP = 0,
		SUMMARY_DUMP = 1,
		ALL_DUMP = 2,
		PARAM_DUMP = 3,
		MVCC_ROW_DUMP = 4,
	};

public:
	class TermConditionUpdator;

	template <typename P, typename K, typename V, typename R>
	class TermConditionRewriter;

	/*!
		@brief Information related to a search
	*/
	struct SearchContext : BaseIndex::SearchContext {
	private:
		bool isEqual_;		  
		bool isCaseSensitive_;
		TermConditionUpdator *condUpdator_;

	public:
		SearchContext(
				util::StackAllocator &alloc,
				const util::Vector<ColumnId> &columnIds) :
				BaseIndex::SearchContext(alloc, columnIds),
				isEqual_(false),
				isCaseSensitive_(true),
				condUpdator_(NULL) {
		}

		SearchContext(util::StackAllocator &alloc, ColumnId columnId) :
				BaseIndex::SearchContext(alloc, columnId),
				isEqual_(false),
				isCaseSensitive_(true),
				condUpdator_(NULL) {
		}

		SearchContext(
				util::StackAllocator &alloc, TermCondition &startCond,
				TermCondition &endCond, ResultSize limit) :
				BaseIndex::SearchContext(alloc, startCond, endCond, limit),
				isEqual_(false),
				isCaseSensitive_(true),
				condUpdator_(NULL) {
		}

		SearchContext(
				util::StackAllocator &alloc, TermCondition &cond,
				ResultSize limit) :
				BaseIndex::SearchContext(alloc, cond, limit),
				isEqual_(true),
				isCaseSensitive_(true),
				condUpdator_(NULL) {
		}

		void clear() {
			BaseIndex::SearchContext::clear();
			isEqual_ = false;
			isCaseSensitive_ = true;
			condUpdator_ = NULL;
		}

		void setCaseSensitive(bool isCaseSensitive) {
			isCaseSensitive_ = isCaseSensitive;
		}

		void copy(util::StackAllocator &alloc, SearchContext &dest) {
			BaseIndex::SearchContext::copy(alloc, dest);

			dest.setEqual(isEqual_);
			dest.setCaseSensitive(isCaseSensitive_);
		}
		bool isCaseSensitive() {
			return isCaseSensitive_;
		}
		bool isEqual() const {
			return isEqual_;
		}
		void setEqual(bool isEqual) {
			isEqual_ = isEqual;
		}
		TermCondition *getStartKeyCondition() {
			assert(columnIdList_.size() == 1);
			for (util::Vector<size_t>::iterator itr = keyList_.begin(); 
				itr != keyList_.end(); itr++) {
				if (conditionList_[*itr].isStartCondition()) {
					return &(conditionList_[*itr]);
				}
			}
			return NULL;
		}
		TermCondition *getEndKeyCondition() {
			assert(columnIdList_.size() == 1);
			for (util::Vector<size_t>::iterator itr = keyList_.begin(); 
				itr != keyList_.end(); itr++) {
				if (conditionList_[*itr].isEndCondition()) {
					return &(conditionList_[*itr]);
				}
			}
			return NULL;
		}

		template< typename T>
		const T *getStartKey() {
			TermCondition *cond = getStartKeyCondition();
			if (cond != NULL) {
				return reinterpret_cast< const T *>(cond->value_);
			}
			return NULL;
		}

		template< typename T>
		const T *getEndKey() {
			TermCondition *cond = getEndKeyCondition();
			if (cond != NULL) {
				return reinterpret_cast< const T *>(cond->value_);
			}
			return NULL;
		}

		uint32_t getRangeConditionNum();
		virtual std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << BaseIndex::SearchContext::dump();

			strstrm << ", {\"isEqual_\" : " << (int)isEqual_ << "}";
			strstrm << ", {\"isCaseSensitive_\" : " << (int)isCaseSensitive_ << "}";

			return strstrm.str();
		}

		void setTermConditionUpdator(TermConditionUpdator *condUpdator) {
			condUpdator_ = condUpdator;
		}

		TermConditionUpdator* getTermConditionUpdator() {
			return condUpdator_;
		}

	};

	/*!
		@brief Cursor for search
	*/
	struct BtreeCursor {
		OId nodeId_;
		int32_t loc_;
		BtreeCursor() {
			nodeId_ = UNDEF_OID;
			loc_ = 0;
		}
	};


	static inline int32_t keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringObject &e1,
		const StringKey &e2, BaseIndex::Setting &);

	static inline int32_t keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringKey &e1,
		const StringObject &e2, BaseIndex::Setting &);

	static inline int32_t keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyObject &e1,
		const FullContainerKeyAddr &e2, BaseIndex::Setting &setting);
	static inline int32_t keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyAddr &e1,
		const FullContainerKeyObject &e2, BaseIndex::Setting &setting);

	template <typename K>
	static inline int32_t keyCmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		const K &e1, const K &e2, BaseIndex::Setting &);


	static int32_t compositeInfoCmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	   const CompositeInfoObject *e1, const CompositeInfoObject *e2, BaseIndex::Setting &setting);


	template <typename S>
	bool compositeInfoMatch(TransactionContext &txn, ObjectManagerV4 &objectManager,
		const S *e, BaseIndex::Setting &setting);


	int32_t initialize(TransactionContext &txn, ColumnType columnType,
		bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize = DEFAULT_ELEM_SIZE);
	template <typename K, typename V>
	int32_t initialize(TransactionContext &txn, ColumnType columnType,
		bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize = DEFAULT_ELEM_SIZE);

	bool finalize(TransactionContext &txn);

	int32_t insert(TransactionContext &txn, const void *key, OId oId);
	int32_t remove(TransactionContext &txn, const void *key, OId oId);
	int32_t update(
		TransactionContext &txn, const void *key, OId oId, OId newOId);

	template <typename K, typename V>
	int32_t insert(TransactionContext &txn, K &key, V &value, bool isCaseSensitive);
	template <typename K, typename V>
	int32_t remove(TransactionContext &txn, K &key, V &value, bool isCaseSensitive );
	template <typename K, typename V>
	int32_t update(TransactionContext &txn, K &key, V &oldValue, V &newValue, bool isCaseSensitive);

	int32_t search(TransactionContext &txn, const void *key, OId &oId);
	int32_t search(TransactionContext &txn, SearchContext &sc,
		util::XArray<OId> &idList, OutputOrder outputOrder = ORDER_UNDEFINED);

	template <typename K, typename V, typename R>
	int32_t search(TransactionContext &txn, K &key, R &retVal, bool isCaseSensitive);
	template <typename K, typename V, typename R>
	int32_t search(TransactionContext &txn, SearchContext &sc,
		util::XArray<R> &idList, OutputOrder outputOrder = ORDER_UNDEFINED);

	int32_t getAll(
		TransactionContext &txn, ResultSize limit, util::XArray<OId> &idList);
	int32_t getAll(TransactionContext &txn, ResultSize limit,
		util::XArray<OId> &idList, BtreeCursor &cursor);
	template <typename K, typename V>
	int32_t getAll(TransactionContext &txn, ResultSize limit,
		util::XArray<std::pair<K, V> > &keyValueList) {
		if (isEmpty()) {
			return GS_SUCCESS;
		}
		BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
		getHeadNode<K, V>(txn, node);

		int32_t loc = 0;
		while (true) {
			keyValueList.push_back(std::make_pair(
				node.getKeyValue(loc).key_, node.getKeyValue(loc).value_));
			if (!nextPos(txn, node, loc) || keyValueList.size() >= limit) {
				break;
			}
		}
		return GS_SUCCESS;
	}

	template <typename K, typename V>
	int32_t getAll(TransactionContext &txn, ResultSize limit,
		util::XArray<std::pair<K, V> > &idList, BtreeMap::BtreeCursor &cursor) {
		if (isEmpty()) {
			return GS_SUCCESS;
		}
		if (limit == 0) {
			return GS_SUCCESS;
		}

		BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
		int32_t loc = 0;
		if (cursor.nodeId_ == UNDEF_OID) {
			getHeadNode<K, V>(txn, node);
			loc = 0;
		}
		else {
			node.load(cursor.nodeId_, false);
			loc = cursor.loc_;
		}
		bool hasNext = false;
		while (true) {
			idList.push_back(std::make_pair(
				node.getKeyValue(loc).key_, node.getKeyValue(loc).value_));
			hasNext = nextPos(txn, node, loc);
			if (!hasNext || idList.size() >= limit) {
				break;
			}
		}
		if (hasNext) {
			cursor.nodeId_ = node.getSelfOId();
			cursor.loc_ = loc;
			return GS_FAIL;
		}
		else {
			return GS_SUCCESS;
		}
	}


	inline OId getTail(TransactionContext &txn) const {
		if (isEmpty()) {
			return UNDEF_OID;
		}
		else {
			BNode<Timestamp, OId> node(
				txn, *getObjectManager(), getTailNodeOId(), *const_cast<AllocateStrategy*>(&allocateStrategy_));
			return node.getKeyValue(node.numkeyValues() - 1).value_;
		}
	}
	static OId getTailDirect(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy& allocateStrategy, OId tailNodeOId) {
		if (tailNodeOId == UNDEF_OID) {
			return UNDEF_OID;
		}
		else {
			BNode<Timestamp, OId> node(
				txn, objectManager, tailNodeOId, allocateStrategy);
			if (node.numkeyValues() == 0) {
				return UNDEF_OID;
			}
			else {
				return node.getKeyValue(node.numkeyValues() - 1).value_;
			}
		}
	}

	inline OId getTailNodeOId() const {
		BNode<char, char> rootNode(this, *const_cast<AllocateStrategy*>(&allocateStrategy_));
		return rootNode.getTailNodeOId();
	}

	uint64_t estimate(TransactionContext &txn, SearchContext &sc);

	inline bool isEmpty() const {
		BNode<char, char> rootNode(this, *const_cast<AllocateStrategy*>(&allocateStrategy_));
		return rootNode.numkeyValues() == 0;
	}

	static uint8_t getDefaultElemSize() {
		return DEFAULT_ELEM_SIZE;
	}

	std::string dump(TransactionContext &txn, uint8_t mode);
	template <typename K, typename V>
	std::string dump(TransactionContext &txn);

	std::string validate(TransactionContext &txn);

private:
	int32_t nodeMaxSize_;	
	int32_t nodeMinSize_;	
	uint8_t elemSize_;		

	bool isUnique_;
	ColumnType keyType_;
	BtreeMapType btreeMapType_;

private:
	static const int32_t INITIAL_DEFAULT_ITEM_SIZE_THRESHOLD =
		5;  
	static const int32_t INITIAL_MVCC_ITEM_SIZE_THRESHOLD =
		2;  

	static const int32_t NORMAL_MIN_ITEM_SIZE =
		30;  
	static const int32_t NORMAL_MAX_ITEM_SIZE =
		2 * NORMAL_MIN_ITEM_SIZE + 1;  
	static const uint8_t DEFAULT_ELEM_SIZE =
		1;  

	enum CompareComponent {
		KEY_COMPONENT,
		KEY_VALUE_COMPONENT
	};

	template<CompareComponent> struct CompareComponentType {};
	typedef CompareComponentType<KEY_COMPONENT> ComponentKey;
	typedef CompareComponentType<KEY_VALUE_COMPONENT> ComponentKeyValue;

	template <typename P, typename K, typename V, CompareComponent C>
	class CmpFunctor {
	public:
		int32_t operator()(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
			const KeyValue<P, V> &e1, 
			const KeyValue<K, V> &e2,
			Setting &setting) {
				typedef typename MapKeyTraits<P>::TYPE S;
				typedef typename MapKeyTraits<K>::TYPE T;
				typedef CompareComponentType<C> Component;

				const S *ptr1 = reinterpret_cast< const S * >(&(e1.key_));
				const T *ptr2 = reinterpret_cast< const T * >(&(e2.key_));
				return cmp(txn, objectManager, strategy, *ptr1, e1.value_, *ptr2, e2.value_, setting, Component());
		}

		template <typename S, typename T, typename U>
		int32_t cmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
			const S &key1, const U &value1, 
			const T &key2, const U &value2, 
			Setting &setting, const ComponentKey & ) {
			UNUSED_VARIABLE(value1);
			UNUSED_VARIABLE(value2);
			return keyCmp(txn, objectManager, strategy, key1, key2, setting);
		}
		template <typename S, typename T, typename U>
		int32_t cmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
			const S &key1, const U &value1, 
			const T &key2, const U &value2, 
			Setting &setting, const ComponentKeyValue & ) {
			int32_t ret = keyCmp(txn, objectManager, strategy, key1, key2, setting);
			if (ret == 0) {
				ret = value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
			}
			return ret;
		}
	};

	template <typename P, typename K, typename V, CompareComponent C>
	class BoundaryCmpFunctor {
	public:
		BoundaryCmpFunctor(bool lower, bool inclusive) :
				eqRet_(resolveEqResult(lower, inclusive)) {
		}

		int32_t operator()(
				TransactionContext &txn, ObjectManagerV4 &objectManager,
				AllocateStrategy &strategy, const KeyValue<P, V> &e1,
				const KeyValue<K, V> &e2, Setting &setting) const {
			CmpFunctor<P, K, V, C> base;
			const int32_t baseRet =
					base(txn, objectManager, strategy, e1, e2, setting);
			if (baseRet == 0) {
				return eqRet_;
			}
			return baseRet;
		}

	private:
		static int32_t resolveEqResult(bool lower, bool inclusive) {
			if (inclusive) {
				return 0;
			}
			else {
				return (lower ? 1 : -1);
			}
		}

		int32_t eqRet_;
	};

	/*!
		@brief Btree Node Header format
	*/
	struct BNodeHeader {  
		OId parent_;	  
		OId self_;		  
		OId children_;	
		int32_t size_;	
		ColumnType keyType_;
		int8_t btreeMapType_;
		uint8_t isUnique_;
		uint8_t elemSize_;
		OId tailNodeOId_;
	};

	/*!
		@brief Btree Node format
	*/
	template <typename K, typename V>
	class BNodeImage {  
	public:
		BNodeHeader &getHeader() {
			return header_;
		}
		KeyValue<K, V> *getKeyValue(int32_t nth) {
			return &(keyValues_[nth]);
		}
		void setKeyValue(int32_t nth, const KeyValue<K, V> *keyValue) {
			keyValues_[nth] = *keyValue;
		}
	private:
		BNodeHeader header_;
		KeyValue<K, V>
			keyValues_[1];  
	};

	/*!
		@brief Object for Btree Node
	*/
	template <typename K, typename V>
	class BNode : public BaseObject {
	public:
		BNode(const BtreeMap *map, AllocateStrategy &allocateStrategy) :
				BaseObject(*(map->getObjectManager()), allocateStrategy),
				allocateStrategy_(allocateStrategy) {
			BaseObject::copyReference(UNDEF_OID, map->getBaseAddr());
		}

		BNode(
				TransactionContext &txn, ObjectManagerV4 &objectManager,
				AllocateStrategy &allocateStrategy) :
				BaseObject(objectManager, allocateStrategy),
				allocateStrategy_(allocateStrategy) {
			UNUSED_VARIABLE(txn);
		}

		BNode(
				TransactionContext &txn, ObjectManagerV4 &objectManager, OId oId,
				AllocateStrategy &allocateStrategy) :
				BaseObject(objectManager, allocateStrategy, oId),
				allocateStrategy_(allocateStrategy) {
			UNUSED_VARIABLE(txn);
		}

		void directLoad(const BtreeMap *map) {
			BaseObject::copyReference(UNDEF_OID, map->getBaseAddr());
		}


		inline void initialize(
				TransactionContext &txn, OId self, bool leaf,
				int32_t nodeMaxSize, uint8_t elemSize) {
			UNUSED_VARIABLE(txn);

			BNodeHeader &header = getImage()->getHeader();
			header.size_ = 0;
			header.parent_ = UNDEF_OID;
			header.self_ = self;
			header.keyType_ = COLUMN_TYPE_WITH_BEGIN;
			header.btreeMapType_ = TYPE_UNDEF_KEY;
			header.isUnique_ = 0;
			header.elemSize_ = elemSize;
			header.tailNodeOId_ = UNDEF_OID;
			if (leaf) {
				header.children_ = UNDEF_OID;
			}
			else {
				BaseObject childrenDirtyObject(
					*getObjectManager(), allocateStrategy_);
				OId *childrenList = childrenDirtyObject.allocate<OId>(
						static_cast<DSObjectSize>(sizeof(OId) * (nodeMaxSize + 1)),
						header.children_, OBJECT_TYPE_BTREE_MAP);
				for (int32_t i = 0; i < nodeMaxSize + 1; ++i) {
					childrenList[i] = UNDEF_OID;
				}
			}
		}
		inline OId getChild(TransactionContext &txn, int32_t nth) const {
			UNUSED_VARIABLE(txn);

			assert(getImage()->getHeader().children_ != UNDEF_OID);

			BaseObject baseObject(*getObjectManager(),
				allocateStrategy_, getImage()->getHeader().children_);
			OId *oIdList = baseObject.getBaseAddr<OId *>();
			return oIdList[nth];
		}
		inline void setChild(TransactionContext &txn, int32_t nth, OId oId) {
			UNUSED_VARIABLE(txn);

			assert(getImage()->getHeader().children_ != UNDEF_OID);

			UpdateBaseObject baseObject(
				*getObjectManager(), allocateStrategy_, getImage()->getHeader().children_);
			OId *oIdListDirtyObject = baseObject.getBaseAddr<OId *>();
			oIdListDirtyObject[nth] = oId;
		}
		inline void insertVal(
			TransactionContext &txn, const KeyValue<K, V> &val,
			BtreeMap::CmpFunctor<K, K, V, KEY_VALUE_COMPONENT> &cmp,
			Setting &setting) {
			BNodeImage<K, V> *image = getImage();
			if (image->getHeader().size_ == 0) {
				image->setKeyValue(0, &val);
			}
			else if (cmp(txn, *getObjectManager(), allocateStrategy_, val,
						 *image->getKeyValue(numkeyValues() - 1), setting) > 0) {
				image->setKeyValue(numkeyValues(), &val);
			}
			else {
				int32_t i, j;
				for (i = 0; i < image->getHeader().size_; ++i) {
					if (cmp(txn, *getObjectManager(), allocateStrategy_, val,
							*image->getKeyValue(i), setting) < 0) {
						for (j = image->getHeader().size_; j > i; --j) {
							image->setKeyValue(j, image->getKeyValue(j - 1));
						}
						image->setKeyValue(i, &val);
						break;
					}
				}
			}
			image->getHeader().size_++;
		}

		inline void removeVal(int32_t m) {
			BNodeImage<K, V> *image = getImage();
			for (int32_t i = m; i < image->getHeader().size_ - 1; ++i) {
				image->setKeyValue(i, image->getKeyValue(i + 1));
			}
			image->getHeader().size_--;
		}

		inline void insertChild(
				TransactionContext &txn, int32_t m, BNode<K, V> &dirtyNode) {
			UNUSED_VARIABLE(txn);

			BNodeImage<K, V> *image = getImage();
			assert(image->getHeader().children_ != UNDEF_OID);

			UpdateBaseObject baseObject(
				*getObjectManager(), allocateStrategy_, image->getHeader().children_);
			OId *oIdListDirtyObject = baseObject.getBaseAddr<OId *>();
			for (int32_t i = numkeyValues(); i >= m; --i) {
				oIdListDirtyObject[i + 1] = oIdListDirtyObject[i];
			}
			oIdListDirtyObject[m] = dirtyNode.getSelfOId();
			dirtyNode.setParentOId(image->getHeader().self_);
		}
		inline void removeChild(TransactionContext &txn, int32_t m) {
			UNUSED_VARIABLE(txn);

			BNodeImage<K, V> *image = getImage();
			assert(image->getHeader().children_ != UNDEF_OID);
			UpdateBaseObject baseObject(
				*getObjectManager(), allocateStrategy_, image->getHeader().children_);
			OId *oIdListDirtyObject = baseObject.getBaseAddr<OId *>();
			for (int32_t i = m; i < image->getHeader().size_; ++i) {
				oIdListDirtyObject[i] = oIdListDirtyObject[i + 1];
			}
			oIdListDirtyObject[image->getHeader().size_] = UNDEF_OID;
		}
		inline bool isLeaf() const {
			return (getImage()->getHeader().children_ == UNDEF_OID ? true : false);
		}
		inline bool isRoot() const {
			return (getImage()->getHeader().parent_ == UNDEF_OID ? true : false);
		}
		inline int32_t numkeyValues() const {
			return getImage()->getHeader().size_;
		}

		inline void allocateVal(
			TransactionContext &txn, int32_t m, KeyValue<K, V> &e, Setting &setting) {
			BNodeImage<K, V> *image = getImage();
			KeyValue<K, V> *dest = image->getKeyValue(m);

			typedef typename MapKeyTraits<K>::TYPE S;
			S *srcKey = reinterpret_cast<S *>(&(e.key_));
			S *destKey = reinterpret_cast<S *>(&(dest->key_));
			setKey(txn, *srcKey, *destKey, setting);

			dest->value_ = e.value_; 
			image->getHeader().size_++;
		}

		inline void allocateVal(
				TransactionContext &txn, int32_t m,
				KeyValue<StringObject, OId> &e, Setting &setting) {
			UNUSED_VARIABLE(txn);
			UNUSED_VARIABLE(setting);

			BNodeImage<K, V> *image = getImage();
			StringCursor *stringCursor =
				reinterpret_cast<StringCursor *>(e.key_.ptr_);
			BaseObject object(*getObjectManager(), allocateStrategy_);
			StringObject *stringObject = object.allocateNeighbor<StringObject>(
				stringCursor->getObjectSize(),
				image->getKeyValue(m)->key_.oId_, image->getHeader().self_,
				OBJECT_TYPE_BTREE_MAP);
			memcpy(stringObject, stringCursor->data(),
				stringCursor->getObjectSize());
			image->getKeyValue(m)->value_ = e.value_;
			image->getHeader().size_++;
		}

		inline void allocateVal(
				TransactionContext &txn, int32_t m,
				KeyValue<FullContainerKeyObject, OId> &e, Setting &setting) {
			UNUSED_VARIABLE(txn);
			UNUSED_VARIABLE(setting);

			BNodeImage<K, V> *image = getImage();
			const FullContainerKeyCursor *keyCursor =
				reinterpret_cast<const FullContainerKeyCursor *>(e.key_.ptr_);
			image->getKeyValue(m)->key_.oId_ = keyCursor->getBaseOId();
			image->getKeyValue(m)->value_ = e.value_;
			image->getHeader().size_++;
		}
		inline void allocateVal(
				TransactionContext& txn, int32_t m,
				KeyValue<FullContainerKeyObject, KeyDataStoreValue>& e, Setting& setting) {
			UNUSED_VARIABLE(txn);
			UNUSED_VARIABLE(setting);

			BNodeImage<K, V>* image = getImage();
			const FullContainerKeyCursor* keyCursor =
				reinterpret_cast<const FullContainerKeyCursor*>(e.key_.ptr_);
			image->getKeyValue(m)->key_.oId_ = keyCursor->getBaseOId();
			image->getKeyValue(m)->value_ = e.value_;
			image->getHeader().size_++;
		}
		inline void setKey(
			TransactionContext &txn, CompositeInfoObject &src, CompositeInfoObject &dest, Setting &setting) {

			BNodeImage<K, V> *image = getImage();
			dest.setKey(txn, *getObjectManager(), 
				allocateStrategy_, image->getHeader().self_, setting.getFuncInfo(), src);
		}
		template <typename S>
		inline void setKey(
			TransactionContext &txn, S &src, S &dest, Setting &setting) {
			UNUSED_VARIABLE(txn);
			UNUSED_VARIABLE(setting);
			UTIL_STATIC_ASSERT((!util::IsSame<K, StringObject>::VALUE));
			UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
			UTIL_STATIC_ASSERT((!util::IsSame<S, CompositeInfoObject>::VALUE));
			dest = src;
		}

		inline void freeVal(int32_t m) {
			KeyValue<K, V> *e = getImage()->getKeyValue(m);
			typedef typename MapKeyTraits<K>::TYPE S;
			S *key = reinterpret_cast<S *>(&(e->key_));
			freeKey(*key);
		}

		inline void freeKey(
			StringKey &key) {
			ChunkAccessor ca;
			getObjectManager()->free(
				ca, allocateStrategy_.getGroupId(), key.oId_);
		}
		inline void freeKey(
			FullContainerKeyAddr &) {
		}
		inline void freeKey(
			CompositeInfoObject &key) {
			key.freeKey(*getObjectManager(), allocateStrategy_);
		}

		template <typename S>
		inline void freeKey(
			S &) {
			UTIL_STATIC_ASSERT((!util::IsSame<K, StringObject>::VALUE));
			UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
			UTIL_STATIC_ASSERT((!util::IsSame<S, CompositeInfoObject>::VALUE));
		}

		inline bool finalize(TransactionContext &txn, ResultSize &removeNum) {
			BNodeImage<K, V> *image = getImage();
			int32_t valueNum = image->getHeader().size_;
			ChunkAccessor ca;
			while (valueNum >= 0) {
				if (!isLeaf()) {
					OId childOId = getChild(txn, valueNum);
					if (childOId != UNDEF_OID) {
						BNode<K, V> dirtyChildNode(
							txn, *getObjectManager(), allocateStrategy_);
						dirtyChildNode.load(childOId, true);
						dirtyChildNode.finalize(txn, removeNum);
						if (removeNum > 0) {
							removeNum--;
						} else {
							break;
						}
					}
				}
				if (valueNum > 0) {
					freeVal(valueNum - 1);
					removeVal(valueNum - 1);
				} else {
					image->getHeader().size_ = -1;
				}
				valueNum--;
			}
			if (removeNum > 0) {
				if (!isLeaf()) {
					getObjectManager()->free(
						ca, allocateStrategy_.getGroupId(), image->getHeader().children_);
				}
				BaseObject::finalize();
			}
			return removeNum > 0;
		}

		inline void remove(TransactionContext &txn) {
			UNUSED_VARIABLE(txn);

			ChunkAccessor ca;
			BNodeImage<K, V> *image = getImage();
			if (image->getHeader().children_ != UNDEF_OID) {
				getObjectManager()->free(
					ca, allocateStrategy_.getGroupId(), image->getHeader().children_);
			}
			BaseObject::finalize();
		}

		inline ColumnType getKeyType() const {
			return getImage()->getHeader().keyType_;
		}
		inline int8_t getBtreeMapType() const {
			return getImage()->getHeader().btreeMapType_;
		}
		inline uint8_t isUnique() const {
			return getImage()->getHeader().isUnique_;
		}
		inline uint8_t getNodeElemSize() const {
			return getImage()->getHeader().elemSize_;
		}
		inline OId getTailNodeOId() const {
			return getImage()->getHeader().tailNodeOId_;
		}
		inline OId getParentOId() const {
			return getImage()->getHeader().parent_;
		}
		inline OId getSelfOId() const {
			return getImage()->getHeader().self_;
		}
		inline const KeyValue<K, V> &getKeyValue(int32_t m) const {
			return *getImage()->getKeyValue(m);
		}

		inline void setTailNodeOId(OId oId) {
			getImage()->getHeader().tailNodeOId_ = oId;
		}
		inline void setKeyValue(int32_t m, const KeyValue<K, V> &keyVal) {
			getImage()->setKeyValue(m, &keyVal);
		}
		inline void setValue(int32_t m, const V &val) {
			getImage()->getKeyValue(m)->value_ = val;
		}
		inline void setNumkeyValues(int32_t num) {
			getImage()->getHeader().size_ = num;
		}
		inline void addNumkeyValues(int32_t num) {
			getImage()->getHeader().size_ += num;
		}
		inline void setParentOId(OId oId) const {
			getImage()->getHeader().parent_ = oId;
		}

		inline void setRootNodeHeader(ColumnType keyType, int8_t btreeMapType,
			uint8_t isUnique, OId oId) {
			BNodeImage<K, V> *image = getImage();
			image->getHeader().keyType_ = keyType;
			image->getHeader().btreeMapType_ = btreeMapType;
			image->getHeader().isUnique_ = isUnique;
			image->getHeader().tailNodeOId_ = oId;
		}
		inline std::string dump(TransactionContext &txn) {
			util::NormalOStringStream out;
			out << "(@@Node@@)";
			for (int32_t i = 0; i < numkeyValues(); ++i) {
				out << getKeyValue(i) << ", ";
			}
			return out.str();
		}
		inline std::string dump(TransactionContext &txn, TreeFuncInfo *funcInfo);

	private:
		AllocateStrategy &allocateStrategy_;
		BNode(const BNode &);  
		BNode &operator=(const BNode &);  
		BNodeImage<K, V> *getImage() const {
			return reinterpret_cast<BNodeImage<K, V> *>(getBaseAddr());
		}
	};

	struct InitializeFunc {
		InitializeFunc(TransactionContext &txn, ColumnType columnType,
			bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize,
			BtreeMap *tree) : txn_(txn), columnType_(columnType),
			isUnique_(isUnique), btreeMapType_(btreeMapType), 
			elemSize_(elemSize), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		ColumnType columnType_;
		bool isUnique_;
		BtreeMapType btreeMapType_;
		uint32_t elemSize_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct FinalizeFunc {
		FinalizeFunc(TransactionContext &txn, BtreeMap *tree) : 
			txn_(txn), tree_(tree), ret_(false) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		BtreeMap *tree_;
		bool ret_;
	};

	struct InsertFunc {
		InsertFunc(TransactionContext &txn, const void *constKey, OId oId,
			BtreeMap *tree) : txn_(txn), key_(const_cast<void *>(constKey)),
			oId_(oId), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		void *key_;
		OId oId_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct RemoveFunc {
		RemoveFunc(TransactionContext &txn, const void *constKey, OId oId,
			BtreeMap *tree) : txn_(txn), key_(const_cast<void *>(constKey)),
			oId_(oId), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		void *key_;
		OId oId_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct UpdateFunc {
		UpdateFunc(TransactionContext &txn, const void *constKey, OId oId,
			OId newOId, BtreeMap *tree) : txn_(txn), key_(const_cast<void *>(constKey)),
			oId_(oId), newOId_(newOId), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		void *key_;
		OId oId_;
		OId newOId_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct UniqueSearchFunc {
		UniqueSearchFunc(TransactionContext &txn, const void *constKey, 
			OId &oId, Setting &setting, BtreeMap *tree) :
			txn_(txn), key_(const_cast<void *>(constKey)),
			oId_(oId), setting_(setting), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		void *key_;
		OId &oId_;
		Setting &setting_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct SearchFunc {
		SearchFunc(TransactionContext &txn, SearchContext &sc, 
			util::XArray<OId> &idList, OutputOrder outputOrder, BtreeMap *tree) :
			txn_(txn), sc_(sc), idList_(idList),
			outputOrder_(outputOrder), tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		SearchContext &sc_;
		util::XArray<OId> &idList_;
		OutputOrder outputOrder_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct GetAllFunc {
		GetAllFunc(TransactionContext &txn, ResultSize limit, 
			util::XArray<OId> &idList, BtreeMap *tree) :
			txn_(txn), limit_(limit), idList_(idList),
			tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		ResultSize limit_;
		util::XArray<OId> &idList_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct GetAllCursorFunc {
		GetAllCursorFunc(TransactionContext &txn, ResultSize limit, 
			util::XArray<OId> &idList, BtreeCursor &cursor, BtreeMap *tree) :
			txn_(txn), limit_(limit), idList_(idList), cursor_(cursor),
			tree_(tree), ret_(GS_SUCCESS) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		ResultSize limit_;
		util::XArray<OId> &idList_;
		BtreeCursor &cursor_;
		BtreeMap *tree_;
		int32_t ret_;
	};

	struct ValidateFunc {
		ValidateFunc(TransactionContext &txn, BtreeMap *tree) :
			txn_(txn), tree_(tree) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		BtreeMap *tree_;
		std::string ret_;
	};

	struct DumpSummaryFunc {
		DumpSummaryFunc(TransactionContext &txn, OId nodeId,
			int32_t nodeNum, int32_t realKeyNum, int32_t keySpaceNum,
			int32_t maxDepth, int32_t minDepth, int32_t maxKeyNode,
			int32_t minKeyNode, int32_t depth, BtreeMap *tree) :
			txn_(txn), nodeId_(nodeId), nodeNum_(nodeNum),
			realKeyNum_(realKeyNum), keySpaceNum_(keySpaceNum),
			maxDepth_(maxDepth), minDepth_(minDepth), maxKeyNode_(maxKeyNode),
			minKeyNode_(minKeyNode), depth_(depth), tree_(tree) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		OId nodeId_;
		int32_t nodeNum_;
		int32_t realKeyNum_;
		int32_t keySpaceNum_;
		int32_t maxDepth_;
		int32_t minDepth_;
		int32_t maxKeyNode_;
		int32_t minKeyNode_;
		int32_t depth_;
		BtreeMap *tree_;
		std::string ret_;
	};

	struct DumpAllFunc {
		DumpAllFunc(TransactionContext &txn, util::NormalOStringStream &out, OId nodeId,
			int32_t depth, TreeFuncInfo *funcInfo, BtreeMap *tree) :
			txn_(txn), out_(out), nodeId_(nodeId),
			depth_(depth), funcInfo_(funcInfo), tree_(tree) {}
		template <typename P, typename K, typename V, typename R>
		void execute();

		TransactionContext &txn_;
		util::NormalOStringStream &out_;
		OId nodeId_;
		int32_t depth_;
		TreeFuncInfo *funcInfo_;
		BtreeMap *tree_;
	};

	template<typename K>
	struct MapKeyTraits {
		typedef K TYPE;
	};

	struct SearchBulkFunc;
	struct EstimateFunc;

	typedef std::pair<int32_t, int32_t> LocationEntry;
	typedef util::Vector<LocationEntry> LocationPath;

	template<typename Action>
	void switchToBasicType(ColumnType type, Action &action);

	template <typename K, typename V>
	BNode<K, V> *getRootNodeAddr() {
		return reinterpret_cast<BNode<K, V> *>(this);
	}

	template <typename K, typename V>
	const BNode<K, V> *getRootNodeAddr() const {
		return reinterpret_cast<const BNode<K, V> *>(this);
	}

	template <typename K, typename V>
	static int32_t getInitialNodeSize(int elemSize) {
		return static_cast<int32_t>(
			sizeof(BNodeHeader) +
			elemSize * getInitialItemSizeThreshold<K, V>());
	}

	template <typename K, typename V>
	static int32_t getInitialItemSizeThreshold();

	template <typename K, typename V>
	inline int32_t getNormalNodeSize(int elemSize) {
		return static_cast<int32_t>(
			sizeof(BNodeHeader) + elemSize * nodeMaxSize_);
	}

	template <typename K, typename V>
	inline int32_t getElemSize() {
		if (util::IsSame<K, CompositeInfoObject>::VALUE && util::IsSame<V, OId>::VALUE) {
			BNode<char, char> rootNode(this, allocateStrategy_);
			return static_cast<int32_t>(
				rootNode.getNodeElemSize());
		} else {
			return static_cast<int32_t>(sizeof(KeyValue<K, V>));
		}
	}

	inline MapType getBtreeMapType() const {
		return btreeMapType_;
	}
	template <typename K, typename V>
	std::string getTreeStatus();

private:
	template<typename P>
	inline bool isComposite() {
		typedef typename MapKeyTraits<P>::TYPE S;
		return (util::IsSame<S, CompositeInfoObject>::VALUE);
	}

	template <typename P, typename V>
	inline bool isMatch(TransactionContext &txn, ObjectManagerV4 &objectManager,
		const KeyValue<P, V> &e, BaseIndex::Setting &setting) {
		typedef typename MapKeyTraits<P>::TYPE S;
		if (isComposite<P>()) {
			const S *ptr = reinterpret_cast< const S * >(&e.key_);
			bool ret = compositeInfoMatch<S>(txn, objectManager, ptr, setting);
			return ret;
		} else {
			return true;
		}
	}

	int32_t calcNodeMaxSize(uint8_t elemSize) {
		int32_t nodeSize;
		if (elemSize == DEFAULT_ELEM_SIZE) {
			nodeSize = NORMAL_MAX_ITEM_SIZE;
		} else {
			int32_t baseNodeSize = 1024;
			if (elemSize >= 72) {
				baseNodeSize = 4 * 1024;
			} else if (elemSize >= 40) {
				baseNodeSize = 2 * 1024;
			}
			int32_t elemAreaSize = static_cast<int32_t>(
					baseNodeSize - sizeof(BNodeHeader) -
					ObjectManagerV4::OBJECT_HEADER_SIZE);
			int32_t maxElemNum = elemAreaSize / elemSize;
			if (maxElemNum % 2 == 0) {
				nodeSize = (maxElemNum - 2) + 1;
			} else {
				nodeSize = (maxElemNum - 1) + 1;
			}
		}
		return nodeSize;
	}
	int32_t calcNodeMinSize(uint8_t elemSize) {
		int32_t nodeSize;
		if (elemSize == DEFAULT_ELEM_SIZE) {
			nodeSize = NORMAL_MIN_ITEM_SIZE;
		} else {
			int32_t baseNodeSize = 1024;
			if (elemSize >= 72) {
				baseNodeSize = 4 * 1024;
			} else if (elemSize >= 40) {
				baseNodeSize = 2 * 1024;
			}
			int32_t elemAreaSize = static_cast<int32_t>(
					baseNodeSize - sizeof(BNodeHeader) -
					ObjectManagerV4::OBJECT_HEADER_SIZE);
			int32_t maxElemNum = elemAreaSize / elemSize;
			if (maxElemNum % 2 == 0) {
				nodeSize = (maxElemNum - 2) / 2;
			} else {
				nodeSize = (maxElemNum - 1) / 2;
			}
		}
		return nodeSize;
	}

	template <typename K, typename V>
	void getHeadNode(TransactionContext &txn, BNode<K, V> &baseHeadNode) {
		getRootBNodeObject<K, V>(baseHeadNode);
		while (1) {
			if (baseHeadNode.isLeaf()) {
				break;
			}
			OId nodeId = baseHeadNode.getChild(txn, 0);
			if (nodeId != UNDEF_OID) {
				baseHeadNode.load(nodeId, false);
			}
			else {
				break;
			}
		}
	}

	template <typename P, typename K, typename V, CompareComponent C>
	bool findNode(TransactionContext &txn, KeyValue<P, V> &val,
		BNode<K, V> &node, int32_t &loc,
		CmpFunctor<P, K, V, C> &cmp,
		Setting &setting) {
		if (isEmpty()) {
			node.reset();  
			loc = 0;
			return false;
		}
		else {
			bool isUniqueKey = isUnique();
			int32_t size;
			getRootBNodeObject<K, V>(node);
			BNode<K, V> *currentNode = &node;
			while (1) {
				OId nodeId;
				size = currentNode->numkeyValues();
				if (cmp(txn, *getObjectManager(), allocateStrategy_, val,
						currentNode->getKeyValue(size - 1), setting) > 0) {
					if (currentNode->isLeaf()) {
						loc = size;
						return false;
					}
					else {
						nodeId = currentNode->getChild(txn, size);
					}
				}
				else {
					int32_t l = 0, r = size;
					while (l < r) {
						int32_t i = (l + r) >> 1;
						int32_t currentCmpResult =
							cmp(txn, *getObjectManager(), allocateStrategy_, val,
								currentNode->getKeyValue(i), setting);
						if (currentCmpResult > 0) {
							l = i + 1;
						}
						else if (isUniqueKey && currentCmpResult == 0) {
							loc = i;
							return true;
						}
						else {
							r = i;
						}
					}
					assert(r == l);
					int32_t cmpResult = cmp(txn, *getObjectManager(),
						allocateStrategy_, val, currentNode->getKeyValue(l), setting);
					if (cmpResult < 0) {
						if (currentNode->isLeaf()) {
							loc = l;
							return false;
						}
						else {
							nodeId = currentNode->getChild(txn, l);
						}
					}
					else if (cmpResult > 0) {
						if (currentNode->isLeaf()) {
							loc = l + 1;
							return false;
						}
						else {
							nodeId = currentNode->getChild(txn, l + 1);
						}
					}
					else {
						if (currentNode->isRoot()) {
							getRootBNodeObject<K, V>(node);
						}
						loc = l;
						return true;
					}
				}
				if (nodeId != UNDEF_OID) {
					node.load(nodeId, false);
					currentNode = &node;
				}
				else {
					break;
				}
			}
			assert(0);
		}
		node.reset();  
		loc = 0;
		return false;
	}

	template <typename K, typename V>
	void findUpNode(
		TransactionContext &txn, const BNode<K, V> &node, int32_t &loc) {
		loc = -1;
		BNode<K, V> baseParentNode(
			txn, *getObjectManager(), node.getParentOId(), allocateStrategy_);
		for (int32_t i = 0; i <= baseParentNode.numkeyValues(); ++i) {
			if (baseParentNode.getChild(txn, i) == node.getSelfOId()) {
				loc = i;
				return;
			}
		}
	}
	template <typename K, typename V>
	void splitNode(TransactionContext &txn, BNode<K, V> &dirtyNode1,
		BNode<K, V> &dirtyNode2, KeyValue<K, V> &val) {
		val = dirtyNode1.getKeyValue(nodeMinSize_);
		OId node2Id;
		dirtyNode2.template allocateNeighbor<BNodeImage<K, V> >(
			getNormalNodeSize<K, V>(getElemSize<K, V>()), node2Id,
			dirtyNode1.getSelfOId(), OBJECT_TYPE_BTREE_MAP);

		dirtyNode2.initialize(
			txn, node2Id, dirtyNode1.isLeaf(), nodeMaxSize_, elemSize_);
		for (int32_t i = nodeMinSize_ + 1, j = 0; i < nodeMaxSize_; i++, j++) {
			dirtyNode2.setKeyValue(j, dirtyNode1.getKeyValue(i));
			dirtyNode2.addNumkeyValues(1);
		}
		dirtyNode2.setParentOId(dirtyNode1.getParentOId());
		if (!dirtyNode1.isLeaf()) {
			for (int32_t i = 0; i < nodeMinSize_ + 1; ++i) {
				OId srcOId = dirtyNode1.getChild(txn, nodeMinSize_ + 1 + i);
				dirtyNode2.setChild(txn, i, srcOId);
				dirtyNode1.setChild(txn, nodeMinSize_ + 1 + i, UNDEF_OID);
				if (srcOId != UNDEF_OID) {
					BNode<K, V> dirtyChildNode(
						txn, *getObjectManager(), allocateStrategy_);
					dirtyChildNode.load(srcOId, true);
					dirtyChildNode.setParentOId(node2Id);
				}
			}
		}
		dirtyNode1.setNumkeyValues(nodeMinSize_);
	}

	template <typename K, typename V>
	bool prevPos(TransactionContext &txn, BNode<K, V> &node, int32_t &loc) {
		BNode<K, V> tmpNode(txn, *getObjectManager(), allocateStrategy_);
		tmpNode.copyReference(node);
		int32_t tmpLoc = loc;
		if (node.isLeaf()) {
			if (loc > 0) {
				loc--;
				return true;
			}
			else {
				while (!node.isRoot()) {
					findUpNode(txn, node, loc);
					node.load(node.getParentOId(), false);
					if (loc > 0) {
						break;
					}
				}
				if (loc > 0) {
					loc--;
					return true;
				}
			}
		}
		else {
			if (loc >= 0) {
				node.load(node.getChild(txn, loc), false);
				while (!node.isLeaf()) {
					node.load(node.getChild(txn, node.numkeyValues()), false);
				}
				loc = node.numkeyValues() - 1;
				return true;
			}
			else {
				while (!node.isRoot()) {
					findUpNode(txn, node, loc);
					node.load(node.getParentOId(), false);
					if (loc > 0) {
						break;
					}
				}
				if (loc > 0) {
					loc--;
					return true;
				}
			}
		}
		node.copyReference(tmpNode);  
		loc = tmpLoc;
		return false;
	}

	template <typename K, typename V>
	bool nextPos(TransactionContext &txn, BNode<K, V> &node, int32_t &loc) {
		BNode<K, V> tmpNode(txn, *getObjectManager(), allocateStrategy_);
		tmpNode.copyReference(node);
		int32_t tmpLoc = loc;
		if (node.isLeaf()) {
			if (node.numkeyValues() - 1 > loc) {
				loc++;
				return true;
			}
			if (!tmpNode.isRoot()) {
				while (!node.isRoot()) {
					findUpNode(txn, node, loc);
					node.load(node.getParentOId(), false);
					if (node.numkeyValues() > loc) {
						break;
					}
				}
				if (node.numkeyValues() > loc) {
					return true;
				}
			}
		}
		else {
			loc++;
			if (node.numkeyValues() >= loc) {
				node.load(node.getChild(txn, loc), false);
				while (!node.isLeaf()) {
					node.load(node.getChild(txn, 0), false);
				}
				loc = 0;
				return true;
			}
			else {
				while (!node.isRoot()) {
					findUpNode(txn, node, loc);
					node.load(node.getParentOId(), false);
					if (node.numkeyValues() >= loc) {
						break;
					}
				}
				if (node.numkeyValues() >= loc) {
					return true;
				}
			}
		}
		node.copyReference(tmpNode);  
		loc = tmpLoc;
		return false;
	}

private:
	template <typename P, typename K, typename V>
	bool insertInternal(
		TransactionContext &txn, P key, V value,
		Setting &setting);
	template <typename P, typename K, typename V>
	bool removeInternal(
		TransactionContext &txn, P key, V value,
		Setting &setting);
	template <typename P, typename K, typename V>
	bool updateInternal(
		TransactionContext &txn, P key, V value, V newValue,
		Setting &setting);

	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findLess(TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded,
		ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findLessByDescending(
		TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findGreater(TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded,
		ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findGreaterByDescending(
		TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findRange(TransactionContext &txn, KeyValue<P, V> &startKeyValue, int32_t isStartIncluded,
		KeyValue<P, V> &endKeyValue, int32_t isEndIncluded, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findRangeByDescending(
		TransactionContext &txn, KeyValue<P, V> &startKeyValue, int32_t isStartIncluded, KeyValue<P, V> &endKeyValue,
		int32_t isEndIncluded, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting);

	template <typename K, typename V>
	void split(TransactionContext &txn, BNode<K, V> &node,
		Setting &setting);
	template <typename K, typename V>
	void merge(TransactionContext &txn, BNode<K, V> &node,
		Setting &setting);
private:
	template <typename P, typename K, typename V>
	int32_t insertInternal(TransactionContext &txn, P &key, V &value, bool isCaseSensitive);

	template <typename P, typename K, typename V>
	int32_t removeInternal(TransactionContext &txn, P &key, V &value, bool isCaseSensitive);

	template <typename P, typename K, typename V>
	int32_t updateInternal(
		TransactionContext &txn, P &key, V &value, V &newValue, bool isCaseSensitive);

	template <typename K, typename V>
	bool finalize(TransactionContext &txn);
	template <typename P, typename K, typename V, BtreeMap::CompareComponent C>
	int32_t find(TransactionContext &txn, P &key, V &value, Setting &setting);

	template <typename P, typename K, typename V, CompareComponent C>
	int32_t find(TransactionContext &txn, P &key, KeyValue<K, V> &keyValue, Setting &setting) {
		KeyValue<P, V> val;
		val.key_ = key;
		val.value_ = V();
		BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
		int32_t loc;
		CmpFunctor<P, K, V, C> cmpFunctor;
		bool ret = findNode<P, K, V>(txn, val, node, loc, cmpFunctor, setting);

		if (ret) {
			keyValue = node.getKeyValue(loc);
			return GS_SUCCESS;
		}
		else {
			if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
				if (prevPos(txn, node, loc)) {
					keyValue = node.getKeyValue(loc);
					return GS_SUCCESS;
				}
				else {
					return GS_FAIL;
				}
			}
			else {
				return GS_FAIL;
			}
		}
	}

	template <typename P, typename K, typename V, typename R, CompareComponent C>
	int32_t find(TransactionContext &txn, SearchContext &sc,
		util::XArray<R> &idList, OutputOrder outputOrder);

	template <typename P, typename K, typename V, typename R>
	bool getAllByAscending(TransactionContext &txn, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, 
		KeyValue<K, V> &suspendKeyValue, Setting &setting) {
		bool isSuspend = false;
		if (isEmpty()) {
			return isSuspend;
		}
		BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
		getHeadNode<K, V>(txn, node);
		int32_t loc = 0;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
		return isSuspend;
	}

	template <typename P, typename K, typename V, typename R>
	int32_t getAllByAscending(
		TransactionContext &txn, ResultSize limit, util::XArray<R> &result);
	template <typename K, typename V, typename R>
	int32_t getAllByAscending(TransactionContext &txn, ResultSize limit,
		util::XArray<R> &result, BtreeMap::BtreeCursor &cursor);

	template <typename P, typename K, typename V, typename R>
	bool getAllByDescending(TransactionContext &txn, ResultSize limit,
		util::XArray<R> &result, ResultSize suspendLimit, 
				KeyValue<K, V> &suspendKeyValue, Setting &setting) {
		bool isSuspend = false;
		if (isEmpty()) {
			return isSuspend;
		}
		BNode<K, V> node(
			txn, *getObjectManager(), getTailNodeOId(), allocateStrategy_);
		int32_t loc = node.numkeyValues() - 1;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
		return isSuspend;
	}
private:
	template <typename K, typename V, typename R>
	void pushResultList(
		const KeyValue<K, V> &keyValue, util::XArray<V> &idList) {
		idList.push_back(keyValue.value_);
	}
	template <typename K, typename V, typename R>
	void pushResultList(
		const KeyValue<K, V> &keyValue, util::XArray<KeyValue<K, V> > &idList) {
		idList.push_back(keyValue);
	}

	template <typename K, typename V>
	bool isChangeNodeSize() {
		BNode<K, V> rootNode(this, allocateStrategy_);
		if (rootNode.isLeaf() &&
			rootNode.numkeyValues() == getInitialItemSizeThreshold<K, V>()) {
			return true;
		}
		else {
			return false;
		}
	}
	template <typename K, typename V>
	bool isRedundantRootNode() {
		BNode<K, V> rootNode(this, allocateStrategy_);
		if (rootNode.numkeyValues() == 0 && !rootNode.isLeaf()) {
			return true;
		}
		else {
			return false;
		}
	}
	template <typename K, typename V>
	int32_t depth(TransactionContext &txn) {
		int32_t d = 0;
		for (OId nodeId = getRootOId(); nodeId != UNDEF_OID;) {
			BNode<K, V> node(txn.getPartitionId(), nodeId);
			if (node.isLeaf()) {
				break;
			}
			nodeId = getChildNode(txn, node.getNode(), 0);
			++d;
		}
		return d;
	}
	template <typename K, typename V>
	friend std::ostream &operator<<(std::ostream &out, BtreeMap &foo) {
		foo.print<K, V>(out, foo.getRootOId());
		return out;
	}
	template <typename K, typename V>
	void getTreeStatus(TransactionContext &txn, OId nodeId, int32_t &nodeNum,
		int32_t &realKeyNum, int32_t &keySpaceNum, int32_t &maxDepth,
		int32_t &minDepth, int32_t &maxKeyNode, int32_t &minKeyNode,
		int32_t depth);

	template <typename K, typename V>
	int32_t print(TransactionContext &txn, std::ostream &out, OId nodeId,
		int32_t depth = 1) {
		int32_t totalSize = 0;
		if (nodeId != UNDEF_OID) {
			BNode<K, V> node(
				txn, *getObjectManager(), nodeId, allocateStrategy_);
			int32_t i;
			int32_t size = node.numkeyValues();
			if (depth >= 1) {
				out << std::setw((depth)*5) << "|-->[ " << size << "] ";
			}
			out << node.dump(txn);
			out << "\n";
			if (!node.isLeaf()) {
				for (i = size; i >= 0; --i) {
					totalSize +=
						print<K, V>(txn, out, node.getChild(txn, i), depth + 1);
				}
			}
			totalSize += size;
		}
		return totalSize;
	}

	template <typename K, typename V>
	int32_t print(TransactionContext &txn, std::ostream &out, OId nodeId,
		int32_t depth, TreeFuncInfo *funcInfo);

	template <typename K, typename V>
	std::string validateInternal(TransactionContext &txn);
	inline bool isUnique() const {
		return isUnique_;
	}
	inline ColumnType getKeyType() const {
		return keyType_;
	}

	void setTailNodeOId(TransactionContext &, OId oId) {
		BNode<char, char> rootNode(this, allocateStrategy_);
		rootNode.setTailNodeOId(oId);
	}
	OId getRootOId() const {
		return getBaseOId();
	}
	void setRootOId(OId oId) {
		setBaseOId(oId);
	}

	template <typename K, typename V>
	void replaceRoot(TransactionContext &txn, BNode<K, V> &replaceNode) {
		UNUSED_VARIABLE(txn);

		BNode<K, V> baseRootNode(this, allocateStrategy_);

		replaceNode.setDirty();
		replaceNode.setRootNodeHeader(baseRootNode.getKeyType(),
			baseRootNode.getBtreeMapType(), baseRootNode.isUnique(),
			baseRootNode.getTailNodeOId());

		this->copyReference(replaceNode);
	}

	template <typename K, typename V>
	void getRootBNodeObject(BNode<K, V> &rootNode) const {
		rootNode.directLoad(this);
	}

	template <typename V>
	V getMaxValue();
	template <typename V>
	V getMinValue();


	int32_t searchBulk(
			TransactionContext &txn, SearchContext &sc, util::XArray<OId> &idList,
			OutputOrder outputOrder);

	template <typename P, typename K, typename V, typename R, CompareComponent C>
	int32_t findBulk(
			TransactionContext &txn, SearchContext &sc, util::XArray<R> &idList,
			OutputOrder outputOrder);

	template <typename P, typename K, typename V, CompareComponent C>
	int32_t findNext(
			TransactionContext &txn, P &key, KeyValue<K, V> &keyValue,
			Setting &setting, BNode<K, V> &node, int32_t &loc);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findNext(
			TransactionContext &txn, SearchContext &sc, util::XArray<R> &idList,
			Setting &setting, BNode<K, V> &node, int32_t &loc);

	template <typename P, typename K, typename V, typename C>
	bool findNodeCustom(
			TransactionContext &txn, KeyValue<P, V> &val, BNode<K, V> &node,
			int32_t &loc, C &cmp, Setting &setting);

	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findGreaterNext(
			TransactionContext &txn, KeyValue<P, V> &keyValue,
			int32_t isIncluded, ResultSize limit, util::XArray<R> &result,
			ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
			Setting &setting, BNode<K, V> &node, int32_t &loc);
	template <typename P, typename K, typename V, typename R, CompareComponent C>
	bool findRangeNext(
			TransactionContext &txn, KeyValue<P, V> &startKeyValue,
			int32_t isStartIncluded, KeyValue<P, V> &endKeyValue,
			int32_t isEndIncluded, ResultSize limit, util::XArray<R> &result,
			ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
			Setting &setting, BNode<K, V> &node, int32_t &loc);

	template <typename P, typename K, typename V, CompareComponent C>
	bool findNodeNext(
			TransactionContext &txn, KeyValue<P, V> &val, BNode<K, V> &node,
			int32_t &loc, CmpFunctor<P, K, V, C> &cmp, Setting &setting);
	template <typename P, typename K, typename V, CompareComponent C>
	bool findTopNodeNext(
			TransactionContext &txn, KeyValue<P, V> &val, BNode<K, V> &node,
			int32_t &loc, CmpFunctor<P, K, V, C> &cmp, Setting &setting);

	template<typename P, typename K, typename V, CompareComponent C>
	uint64_t estimateAt(TransactionContext &txn, SearchContext &sc);

	template<typename K, typename V>
	uint64_t estimatePathRange(
			TransactionContext &txn, const LocationPath &beginPath,
			const LocationPath &endPath);

	uint64_t estimatePathRangeSub(
			const LocationPath &beginPath, const LocationPath &endPath);

	static uint64_t mergePathRangeSize(uint64_t size1, uint64_t size2);

	template<typename P, typename K, typename V, CompareComponent C>
	void findLocationPath(
			TransactionContext &txn, KeyValue<P, V> &keyValue,
			bool inclusive, bool less, Setting &setting, LocationPath &path);

	template<typename K, typename V>
	void getHeadLocationPath(
			TransactionContext &txn, LocationPath &path);
	template<typename K, typename V>
	void getTailLocationPath(
			TransactionContext &txn, LocationPath &path);

	template<typename K, typename V>
	bool findMiddleLocationPath(
			TransactionContext &txn,  const LocationPath &beginPath,
			const LocationPath &endPath, LocationPath &path);

	template<typename K, typename V>
	void nodeToLocationPath(
			TransactionContext &txn, BNode<K, V> &node, int32_t &loc,
			LocationPath &path);

	static void errorInvalidSearchCondition();
};

template <typename K, typename V>
void BtreeMap::split(TransactionContext &txn, BNode<K, V> &dirtyNode1,
	Setting &setting) {
	CmpFunctor<K, K, V, KEY_VALUE_COMPONENT> cmpInternal;
	if (dirtyNode1.numkeyValues() == 2 * nodeMinSize_ + 1) {
		int32_t i, j;
		OId parentOId;
		KeyValue<K, V> middleVal;
		BNode<K, V> dirtyNode2(txn, *getObjectManager(), allocateStrategy_);
		splitNode(txn, dirtyNode1, dirtyNode2, middleVal);

		BNode<K, V> dirtyParentNode(
			txn, *getObjectManager(), allocateStrategy_);
		if (dirtyNode1.isRoot()) {
			dirtyParentNode.template allocateNeighbor<BNodeImage<K, V> >(
				getNormalNodeSize<K, V>(getElemSize<K, V>()), parentOId,
				dirtyNode1.getSelfOId(), OBJECT_TYPE_BTREE_MAP);
			dirtyParentNode.initialize(
				txn, parentOId, false, nodeMaxSize_, elemSize_);

			dirtyParentNode.setKeyValue(0, middleVal);
			dirtyParentNode.setNumkeyValues(1);
			dirtyParentNode.setChild(txn, 0, dirtyNode1.getSelfOId());
			dirtyParentNode.setChild(txn, 1, dirtyNode2.getSelfOId());
			dirtyNode1.setParentOId(dirtyParentNode.getSelfOId());
			dirtyNode2.setParentOId(dirtyParentNode.getSelfOId());
			replaceRoot<K, V>(txn, dirtyParentNode);
		}
		else {
			dirtyParentNode.load(dirtyNode1.getParentOId(), OBJECT_FOR_UPDATE);
			int32_t parentNodeSize = dirtyParentNode.numkeyValues();
			if (cmpInternal(txn, *getObjectManager(), allocateStrategy_, middleVal,
					dirtyParentNode.getKeyValue(parentNodeSize - 1), setting) > 0) {
				dirtyParentNode.setChild(
					txn, parentNodeSize + 1, dirtyNode2.getSelfOId());
				dirtyParentNode.setKeyValue(parentNodeSize, middleVal);
				dirtyParentNode.addNumkeyValues(1);
			}
			else {
				for (i = 0; i < parentNodeSize; ++i) {
					if (cmpInternal(txn, *getObjectManager(), allocateStrategy_, middleVal,
							dirtyParentNode.getKeyValue(i), setting) < 0) {
						for (j = parentNodeSize; j > i; --j) {
							dirtyParentNode.setKeyValue(
								j, dirtyParentNode.getKeyValue(j - 1));
						}
						dirtyParentNode.setKeyValue(i, middleVal);
						dirtyParentNode.addNumkeyValues(1);
						for (j = parentNodeSize + 1; j > i + 1; --j) {
							dirtyParentNode.setChild(
								txn, j, dirtyParentNode.getChild(txn, j - 1));
						}
						dirtyParentNode.setChild(
							txn, i + 1, dirtyNode2.getSelfOId());
						break;
					}
				}
			}
		}
		if (dirtyNode1.getSelfOId() == getTailNodeOId()) {
			setTailNodeOId(txn, dirtyNode2.getSelfOId());
		}

		split(txn, dirtyParentNode, setting);
	}
}

template <typename K, typename V>
void BtreeMap::merge(TransactionContext &txn, BNode<K, V> &node,
	Setting &setting) {
	CmpFunctor<K, K, V, KEY_VALUE_COMPONENT> cmpInternal;
	if (node.numkeyValues() < nodeMinSize_ && !node.isRoot()) {
		int32_t upIndex;
		findUpNode(txn, node, upIndex);
		BNode<K, V> parentNode(txn, *getObjectManager(), node.getParentOId(),
			allocateStrategy_);  
		BNode<K, V> preSibNode(txn, *getObjectManager(),
			allocateStrategy_);  

		if (upIndex > 0) {
			preSibNode.load(parentNode.getChild(txn, upIndex - 1), false);
		}
		if (preSibNode.getBaseOId() != UNDEF_OID &&
			preSibNode.numkeyValues() > nodeMinSize_) {
			node.setDirty();
			preSibNode.setDirty();
			parentNode.setDirty();
			BNode<K, V> *dirtyNode = &node;
			BNode<K, V> *dirtyPreSibNode = &preSibNode;
			BNode<K, V> *dirtyParentNode = &parentNode;
			int32_t sibNodeSize = dirtyPreSibNode->numkeyValues();

			if (!dirtyNode->isLeaf()) {
				BNode<K, V> dirtyChildNode(
					txn, *getObjectManager(), allocateStrategy_);
				dirtyChildNode.load(
					dirtyPreSibNode->getChild(txn, sibNodeSize), true);
				dirtyNode->insertChild(txn, 0, dirtyChildNode);
			}
			dirtyNode->insertVal(
				txn, dirtyParentNode->getKeyValue(upIndex - 1), cmpInternal, setting);

			dirtyParentNode->setKeyValue(
				upIndex - 1, dirtyPreSibNode->getKeyValue(sibNodeSize - 1));

			if (!dirtyNode->isLeaf()) {
				dirtyPreSibNode->removeChild(txn, sibNodeSize);
			}
			dirtyPreSibNode->removeVal(sibNodeSize - 1);
		}
		else {
			BNode<K, V> followSibNode(txn, *getObjectManager(),
				allocateStrategy_);  
			if (upIndex < parentNode.numkeyValues()) {
				followSibNode.load(parentNode.getChild(txn, upIndex + 1), false);
			}
			if (followSibNode.getBaseOId() != UNDEF_OID &&
				followSibNode.numkeyValues() > nodeMinSize_) {
				node.setDirty();
				followSibNode.setDirty();
				parentNode.setDirty();
				BNode<K, V> *dirtyNode = &node;
				BNode<K, V> *dirtyFollowSibNode = &followSibNode;
				BNode<K, V> *dirtyParentNode = &parentNode;
				int32_t nodeObjectSize = dirtyNode->numkeyValues();

				if (!dirtyNode->isLeaf()) {
					BNode<K, V> dirtyChildNode(
						txn, *getObjectManager(), allocateStrategy_);
					dirtyChildNode.load(
						dirtyFollowSibNode->getChild(txn, 0), true);
					dirtyNode->insertChild(
						txn, nodeObjectSize + 1, dirtyChildNode);
				}
				dirtyNode->insertVal(
					txn, dirtyParentNode->getKeyValue(upIndex), cmpInternal, setting);

				dirtyParentNode->setKeyValue(
					upIndex, dirtyFollowSibNode->getKeyValue(0));
				if (!dirtyNode->isLeaf()) {
					dirtyFollowSibNode->removeChild(txn, 0);
				}
				dirtyFollowSibNode->removeVal(0);

			}
			else if (preSibNode.getBaseOId() != UNDEF_OID &&
					 preSibNode.numkeyValues() == nodeMinSize_) {
				node.setDirty();
				preSibNode.setDirty();
				parentNode.setDirty();
				BNode<K, V> *dirtyNode = &node;
				BNode<K, V> *dirtyPreSibNode = &preSibNode;
				BNode<K, V> *dirtyParentNode = &parentNode;
				int32_t sibNodeSize = preSibNode.numkeyValues();

				dirtyPreSibNode->insertVal(txn,
					dirtyParentNode->getKeyValue(upIndex - 1), cmpInternal, setting);

				for (int32_t i = 0; i < dirtyNode->numkeyValues(); ++i) {
					dirtyPreSibNode->setKeyValue(
						sibNodeSize + i + 1, dirtyNode->getKeyValue(i));
				}
				dirtyPreSibNode->addNumkeyValues(dirtyNode->numkeyValues());

				dirtyParentNode->removeChild(txn, upIndex);
				dirtyParentNode->removeVal(upIndex - 1);

				if (!dirtyNode->isLeaf()) {
					for (int32_t i = 0; i <= dirtyNode->numkeyValues(); ++i) {
						OId srcOId = dirtyNode->getChild(txn, i);
						dirtyPreSibNode->setChild(
							txn, sibNodeSize + i + 1, srcOId);
						if (srcOId != UNDEF_OID) {
							BNode<K, V> dirtyChildNode(
								txn, *getObjectManager(), allocateStrategy_);
							dirtyChildNode.load(srcOId, true);
							dirtyChildNode.setParentOId(
								dirtyPreSibNode->getSelfOId());
						}
					}
				}
				if (dirtyNode->getSelfOId() == getTailNodeOId()) {
					setTailNodeOId(txn, dirtyPreSibNode->getSelfOId());
				}
				dirtyNode->remove(txn);
			}
			else if (followSibNode.getBaseOId() != UNDEF_OID &&
					 followSibNode.numkeyValues() == nodeMinSize_) {
				node.setDirty();
				followSibNode.setDirty();
				parentNode.setDirty();
				BNode<K, V> *dirtyNode = &node;
				BNode<K, V> *dirtyFollowSibNode = &followSibNode;
				BNode<K, V> *dirtyParentNode = &parentNode;
				int32_t nodeObjectSize = dirtyNode->numkeyValues();
				int32_t sibNodeSize = dirtyFollowSibNode->numkeyValues();

				dirtyNode->insertVal(
					txn, dirtyParentNode->getKeyValue(upIndex), cmpInternal, setting);

				for (int32_t i = 0; i < sibNodeSize; ++i) {
					dirtyNode->setKeyValue(nodeObjectSize + i + 1,
						dirtyFollowSibNode->getKeyValue(i));
				}
				dirtyNode->addNumkeyValues(sibNodeSize);

				dirtyParentNode->removeChild(txn, upIndex + 1);
				dirtyParentNode->removeVal(upIndex);

				if (!dirtyNode->isLeaf()) {
					for (int32_t i = 0; i <= sibNodeSize; ++i) {
						OId srcOId = dirtyFollowSibNode->getChild(txn, i);
						dirtyNode->setChild(
							txn, nodeObjectSize + i + 1, srcOId);
						if (srcOId != UNDEF_OID) {
							BNode<K, V> dirtyChildNode(
								txn, *getObjectManager(), allocateStrategy_);
							dirtyChildNode.load(srcOId, true);
							dirtyChildNode.setParentOId(
								dirtyNode->getSelfOId());
						}
					}
				}
				if (dirtyFollowSibNode->getSelfOId() == getTailNodeOId()) {
					setTailNodeOId(txn, dirtyNode->getSelfOId());
				}
				dirtyFollowSibNode->remove(txn);
			}
		}
		merge(txn, parentNode, setting);
	}
}

template <typename P, typename K, typename V>
int32_t BtreeMap::insertInternal(TransactionContext &txn, P &key, V &value, bool isCaseSensitive) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	setDirty();

	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	bool isSuccess =
		insertInternal<P, K, V>(txn, key, value, setting);

	int32_t ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;
	assert(ret == GS_SUCCESS);
	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

template <typename P, typename K, typename V>
bool BtreeMap::insertInternal(
	TransactionContext &txn, P key, V value,
	Setting &setting) {

	CmpFunctor<P, K, V, KEY_VALUE_COMPONENT> cmpInput;

	KeyValue<P, V> val;
	val.key_ = key;
	val.value_ = value;
	if (isEmpty()) {
		BNode<K, V> dirtyNode(
			txn, *getObjectManager(), allocateStrategy_);
		getRootBNodeObject<K, V>(dirtyNode);
		dirtyNode.allocateVal(txn, 0, val, setting);
		setTailNodeOId(txn, getRootOId());
	}
	else {
		if (isChangeNodeSize<K, V>()) {
			BNode<K, V> rootNode(
				txn, *getObjectManager(), getRootOId(), allocateStrategy_);
			BNode<K, V> dirtyNewNode(
				txn, *getObjectManager(), allocateStrategy_);
			OId newNodeOId;
			dirtyNewNode.template allocate<BNodeImage<K, V> >(getNormalNodeSize<K, V>(getElemSize<K, V>()),
				newNodeOId, OBJECT_TYPE_BTREE_MAP);
			dirtyNewNode.initialize(
				txn, newNodeOId, true, nodeMaxSize_, elemSize_);

			for (int32_t i = 0; i < rootNode.numkeyValues(); ++i) {
				dirtyNewNode.setKeyValue(i, rootNode.getKeyValue(i));
			}
			dirtyNewNode.setNumkeyValues(rootNode.numkeyValues());

			replaceRoot<K, V>(txn, dirtyNewNode);
			setTailNodeOId(txn, getRootOId());
			rootNode.remove(txn);
		}
		{
			int32_t loc;
			BNode<K, V> dirtyNode1(txn, *getObjectManager(),
				allocateStrategy_);  
			{
				dirtyNode1.load(getTailNodeOId(), false);
				if (cmpInput(txn, *getObjectManager(), allocateStrategy_, val,
						dirtyNode1.getKeyValue(dirtyNode1.numkeyValues() - 1), setting) >
					0) {
					loc = dirtyNode1.numkeyValues();
				}
				else {
					if (findNode<P, K, V, KEY_VALUE_COMPONENT>(
							txn, val, dirtyNode1, loc, cmpInput, setting) &&
						isUnique()) {
						return false;
					}
				}
			}

			dirtyNode1.setDirty();
			for (int32_t i = dirtyNode1.numkeyValues(); i > loc; --i) {
				dirtyNode1.setKeyValue(i, dirtyNode1.getKeyValue(i - 1));
			}

			dirtyNode1.allocateVal(txn, loc, val, setting);

			split<K, V>(txn, dirtyNode1, setting);
		}
	}
	return true;
}

template <typename P, typename K, typename V>
int32_t BtreeMap::removeInternal(TransactionContext &txn, P &key, V &value, bool isCaseSensitive) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	setDirty();
	if (isEmpty()) {
		return GS_FAIL;
	}

	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	bool isSuccess =
		removeInternal<P, K, V>(txn, key, value, setting);

	int32_t ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;
	assert(ret == GS_SUCCESS);
	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

template <typename P, typename K, typename V>
bool BtreeMap::removeInternal(
	TransactionContext &txn, P key, V value,
		Setting &setting) {
	CmpFunctor<P, K, V, KEY_VALUE_COMPONENT> cmpInput;
	{

		KeyValue<P, V> val;
		val.key_ = key;
		val.value_ = value;
		int32_t loc;
		BNode<K, V> dirtyUpdateNode(txn, *getObjectManager(),
			allocateStrategy_);  

		if (!findNode<P, K, V>(txn, val, dirtyUpdateNode, loc, cmpInput, setting)) {
			return false;
		}

		dirtyUpdateNode.setDirty();

		if (!dirtyUpdateNode.isLeaf()) {
			BNode<K, V> dirtyUpdateChildNode(txn, *getObjectManager(),
				dirtyUpdateNode.getChild(txn, loc),
				allocateStrategy_);  

			while (!dirtyUpdateChildNode.isLeaf()) {
				dirtyUpdateChildNode.load(dirtyUpdateChildNode.getChild(
					txn, dirtyUpdateChildNode.numkeyValues()), true);
			}
			dirtyUpdateChildNode.setDirty();

			dirtyUpdateNode.freeVal(loc);
			dirtyUpdateNode.setKeyValue(loc,
				dirtyUpdateChildNode.getKeyValue(
					dirtyUpdateChildNode.numkeyValues() - 1));  
			loc = dirtyUpdateChildNode.numkeyValues() - 1;

			dirtyUpdateNode.copyReference(dirtyUpdateChildNode);
		}
		else {
			dirtyUpdateNode.freeVal(loc);
		}
		if (dirtyUpdateNode.isRoot()) {
			dirtyUpdateNode.removeVal(loc);
		}
		else {
			dirtyUpdateNode.removeVal(loc);
			merge(txn, dirtyUpdateNode, setting);
		}
	}
	{
		if (isRedundantRootNode<K, V>()) {
			BNode<K, V> rootNode(
				txn, *getObjectManager(), getRootOId(), allocateStrategy_);
			OId newrootId = rootNode.getChild(txn, 0);
			BNode<K, V> dirtyNewRootNode(
				txn, *getObjectManager(), allocateStrategy_);
			dirtyNewRootNode.load(newrootId, true);
			dirtyNewRootNode.setParentOId(UNDEF_OID);
			replaceRoot<K, V>(txn, dirtyNewRootNode);
			rootNode.remove(txn);
		}
	}
	{
		if (isChangeNodeSize<K, V>()) {
			BNode<K, V> dirtyNewNode(
				txn, *getObjectManager(), allocateStrategy_);
			OId newNodeOId;
			dirtyNewNode.template allocate<BNodeImage<K, V> >(getInitialNodeSize<K, V>(getElemSize<K, V>()),
				newNodeOId, OBJECT_TYPE_BTREE_MAP);
			dirtyNewNode.initialize(txn, dirtyNewNode.getBaseOId(), true,
				getInitialItemSizeThreshold<K, V>(), elemSize_);

			BNode<K, V> dirtyOldRootNode(
				txn, *getObjectManager(), getRootOId(), allocateStrategy_);
			for (int32_t i = 0; i < dirtyOldRootNode.numkeyValues(); ++i) {
				dirtyNewNode.setKeyValue(i, dirtyOldRootNode.getKeyValue(i));
			}
			dirtyNewNode.setNumkeyValues(dirtyOldRootNode.numkeyValues());

			replaceRoot<K, V>(txn, dirtyNewNode);
			setTailNodeOId(txn, getRootOId());

			dirtyOldRootNode.remove(txn);
		}
	}
	return true;
}

template <typename P, typename K, typename V>
int32_t BtreeMap::updateInternal(
	TransactionContext &txn, P &key, V &oldValue, V &newValue, bool isCaseSensitive) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	setDirty();

	if (isEmpty()) {
		return GS_FAIL;
	}

	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	int32_t ret;
	bool isSuccess;
	if (isUnique()) {
		isSuccess = updateInternal<P, K, V>(txn, key, oldValue, newValue, setting);
	}
	else {
		isSuccess =
			removeInternal<P, K, V>(txn, key, oldValue, setting);
		if (isSuccess) {
			isSuccess = insertInternal<P, K, V>(
				txn, key, newValue, setting);
		}
	}
	ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;

	assert(ret == GS_SUCCESS);
	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

template <typename P, typename K, typename V>
bool BtreeMap::updateInternal(
	TransactionContext &txn, P key, V value, V newValue,
	Setting &setting) {
	CmpFunctor<P, K, V, KEY_VALUE_COMPONENT> cmpInput;

	KeyValue<P, V> val;
	val.key_ = key;
	val.value_ = value;
	int32_t orgLoc;

	BNode<K, V> dirtyOrgNode(txn, *getObjectManager(),
		allocateStrategy_);  
	if (!findNode<P, K, V>(txn, val, dirtyOrgNode, orgLoc, cmpInput, setting)) {
		return false;
	}
	dirtyOrgNode.setDirty();
	dirtyOrgNode.setValue(orgLoc, newValue);

	return true;
}

template <typename P, typename K, typename V, BtreeMap::CompareComponent C>
int32_t BtreeMap::find(TransactionContext &txn, P &key, V &value, Setting &setting) {
	KeyValue<K, V> keyValue;
	int32_t ret = find<P, K, V, C>(txn, key, keyValue, setting);
	if (ret == GS_SUCCESS) {
		value = keyValue.value_;
	}
	return ret;
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
int32_t BtreeMap::find(TransactionContext &txn, SearchContext &sc,
	util::XArray<R> &idList, OutputOrder outputOrder) {
	UTIL_STATIC_ASSERT((!util::IsSame<V, uint32_t>::VALUE));

	Setting setting(getKeyType(), sc.isCaseSensitive(), getFuncInfo());
	setting.initialize(txn.getDefaultAllocator(), sc, outputOrder);

	TermCondition *startCond = setting.getStartKeyCondition();
	TermCondition *endCond = setting.getEndKeyCondition();
	ResultSize suspendLimit = sc.getSuspendLimit();
	KeyValue<K, V> suspendKeyValue;
	bool isSuspend = false;
	if (startCond != NULL && startCond->opType_ == DSExpression::EQ && 
		setting.getGreaterCompareNum() == sc.getKeyColumnNum() && isUnique()) {
		if (startCond == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_UNEXPECTED_ERROR, "internal error");
		}
		P key = *reinterpret_cast<const P *>(startCond->value_);
		KeyValue<K, V> keyValue;
		int32_t ret = find<P, K, V, C>(txn, key, keyValue, setting);
		if (ret == GS_SUCCESS) {
			pushResultList<K, V, R>(keyValue, idList);
		}
	}
	else if (startCond != NULL) {
		KeyValue<P, V> startKeyVal(*reinterpret_cast<const P *>(startCond->value_), V()); 
		if (C == KEY_VALUE_COMPONENT) {
			if (outputOrder != ORDER_DESCENDING) {
				startKeyVal.value_ = *reinterpret_cast<const V *>(sc.getSuspendValue());
			} else {
				startKeyVal.value_ = (setting.isStartIncluded_) ? getMinValue<V>() : getMaxValue<V>();
			}
		}
		if (endCond != NULL) {
			KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(endCond->value_), V()); 
			if (C == KEY_VALUE_COMPONENT) {
				assert(sc.getSuspendValue() != NULL);
				if (outputOrder != ORDER_DESCENDING) {
					endKeyVal.value_ = (setting.isEndIncluded_) ? getMaxValue<V>() : getMinValue<V>();
				} else {
					endKeyVal.value_ = *reinterpret_cast<const V *>(sc.getSuspendValue());
				}
			}
			if (outputOrder != ORDER_DESCENDING) {
				isSuspend = findRange<P, K, V, R, C>(txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, endCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting);
			}
			else {
				isSuspend = findRangeByDescending<P, K, V, R, C>(txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, endCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting);
			}
		}
		else if (startCond->opType_ == DSExpression::EQ) {
			KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(startCond->value_), V()); 
			if (C == KEY_VALUE_COMPONENT) {
				assert(sc.getSuspendValue() != NULL);
				endKeyVal.value_ = (setting.isEndIncluded_) ? getMaxValue<V>() : getMinValue<V>();
			}
			if (outputOrder != ORDER_DESCENDING) {
				isSuspend = findRange<P, K, V, R, C>(txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, startCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting);
			}
			else {
				isSuspend = findRangeByDescending<P, K, V, R, C>(txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, startCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting);
			}
		} 
		else {
			if (outputOrder != ORDER_DESCENDING) {
				isSuspend = findGreater<P, K, V, R, C>(txn, startKeyVal,
					startCond->isIncluded(), sc.getLimit(), idList,
					suspendLimit, suspendKeyValue, setting);
			}
			else {
				isSuspend = findGreaterByDescending<P, K, V, R, C>(txn,
					startKeyVal, startCond->isIncluded(), sc.getLimit(),
					idList, suspendLimit, suspendKeyValue, setting);
			}
		}
	}
	else if (endCond != NULL) {
		KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(endCond->value_), V()); 
		if (C == KEY_VALUE_COMPONENT) {
			assert(sc.getSuspendValue() != NULL);
			endKeyVal.value_ = *reinterpret_cast<const V *>(sc.getSuspendValue());
		}
		if (outputOrder != ORDER_DESCENDING) {
			isSuspend = findLess<P, K, V, R, C>(txn, endKeyVal,
				endCond->isIncluded(), sc.getLimit(), idList,
				suspendLimit, suspendKeyValue, setting);
		}
		else {
			isSuspend = findLessByDescending<P, K, V, R, C>(txn, endKeyVal,
				endCond->isIncluded(), sc.getLimit(), idList,
				suspendLimit, suspendKeyValue, setting);
		}
	}
	else {
		if (outputOrder != ORDER_DESCENDING) {
			isSuspend = getAllByAscending<P, K, V, R>(
				txn, sc.getLimit(), idList, suspendLimit, suspendKeyValue, setting);
		}
		else {
			isSuspend = getAllByDescending<P, K, V, R>(
				txn, sc.getLimit(), idList, suspendLimit, suspendKeyValue, setting);
		}
	}
	if (isSuspend) {
		typedef typename MapKeyTraits<K>::TYPE T;
		sc.setSuspended(true);
		sc.setSuspendPoint<T, V>(txn, *getObjectManager(), allocateStrategy_, getFuncInfo(),
			suspendKeyValue.key_, suspendKeyValue.value_);
	}
	else {
		suspendLimit -= idList.size();
		if (suspendLimit < MINIMUM_SUSPEND_SIZE) {
			suspendLimit = MINIMUM_SUSPEND_SIZE;
		}
		sc.setSuspendLimit(suspendLimit);
	}
	if (idList.empty()) {
		return GS_FAIL;
	}
	else {
		return GS_SUCCESS;
	}
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findLess(TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded,
	ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<K, P, V, C> cmpFuncRight;
	setting.setCompareNum(setting.getLessCompareNum());
	
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;

	if (isEmpty()) {
		return isSuspend;
	}
	else {
		getHeadNode<K, V>(txn, node);
		loc = 0;
	}
	while (true) {
		KeyValue<K, V> currentVal = node.getKeyValue(loc);
		if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, keyValue,
			setting) <	isIncluded) {
			if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
			pushResultList<K, V, R>(currentVal, result);
		}
		else {
			break;
		}
		if (!nextPos(txn, node, loc) || result.size() >= limit) {
			break;
		}
		if (result.size() >= suspendLimit) {
			if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, node.getKeyValue(loc),
					keyValue, setting) < isIncluded) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
			}
			break;
		}
	}
	return isSuspend;
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findLessByDescending(
	TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded, ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	CmpFunctor<K, P, V, C> cmpFuncRight;
	setting.setCompareNum(setting.getLessCompareNum());
	
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;
	findNode(txn, keyValue, node, loc, cmpFuncLeft, setting);
	if (loc >= node.numkeyValues()) {
		if (prevPos(txn, node, loc)) {
			while (true) {
				KeyValue<K, V> currentVal = node.getKeyValue(loc);
				if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
				if (!prevPos(txn, node, loc) || result.size() >= limit) {
					break;
				}
				if (result.size() >= suspendLimit) {
					isSuspend = true;
					suspendKeyValue = node.getKeyValue(loc);
					break;
				}
			}
		}
		return isSuspend;
	}
	else {
		while (isIncluded) {
			if (cmpFuncLeft(
					txn, *getObjectManager(), allocateStrategy_, keyValue, node.getKeyValue(loc), setting) < 0) {
				break;
			}
			if (!nextPos(txn, node, loc)) {
				break;
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
			}
			else {
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, keyValue, setting) <
					isIncluded) {
					if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
					start = true;
				}
			}
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
		return isSuspend;
	}
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findGreater(
	TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded, ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	setting.setCompareNum(setting.getGreaterCompareNum());
	
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;
	bool isEqual = findNode<P, K, V, C>(txn, keyValue, node, loc, cmpFuncLeft, setting);

	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		if (loc >= node.numkeyValues() || !isEqual) {
			prevPos(txn, node, loc);
		}
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			pushResultList<K, V, R>(currentVal, result);
			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	else {
		if (loc >= node.numkeyValues()) {
			prevPos(txn, node, loc);
		}
		else {
			while (isIncluded) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue,
						node.getKeyValue(loc), setting) > 0) {
					break;
				}
				if (!prevPos(txn, node, loc)) {
					break;
				}
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
			}
			else {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue, currentVal,
					setting) < isIncluded) {
					if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
					start = true;
				}
			}
			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	return isSuspend;
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findGreaterByDescending(
	TransactionContext &txn, KeyValue<P, V> &keyValue, int32_t isIncluded, ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	setting.setCompareNum(setting.getGreaterCompareNum());
			
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;

	if (isEmpty()) {
		return isSuspend;
	}
	else {
		node.load(getTailNodeOId(), false);
		loc = node.numkeyValues() - 1;
	}
	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		int32_t ret, prevRet = -1;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			ret = cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue, currentVal,
				setting);
			if (ret < isIncluded) {
				pushResultList<K, V, R>(currentVal, result);
				prevRet = ret;
			}
			else {
				if (ret == 0 || prevRet != 0) {
					pushResultList<K, V, R>(currentVal, result);
				}
				break;
			}
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	else {
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue, currentVal,
				setting) < isIncluded) {
				if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(node.getKeyValue(loc), result);
			}
			else {
				break;
			}
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue,
						node.getKeyValue(loc), setting) < isIncluded) {
					isSuspend = true;
					suspendKeyValue = node.getKeyValue(loc);
				}
				break;
			}
		}
	}
	return isSuspend;
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findRange(
	TransactionContext &txn, KeyValue<P, V> &startKeyValue, int32_t isStartIncluded, KeyValue<P, V> &endKeyValue,
	int32_t isEndIncluded, ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	CmpFunctor<K, P, V, C> cmpFuncRight;
	
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;

	setting.setCompareNum(setting.getGreaterCompareNum());
	bool isEqual = findNode<P, K, V, C>(txn, startKeyValue, node, loc, cmpFuncLeft,
		setting);
	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		if (loc >= node.numkeyValues() || !isEqual) {
			prevPos(txn, node, loc);
		}
		setting.setCompareNum(setting.getLessCompareNum());
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
					setting) < isEndIncluded) {
				pushResultList<K, V, R>(currentVal, result);
			}
			else {
				break;
			}

			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	else {
		if (loc >= node.numkeyValues()) {
			prevPos(txn, node, loc);
		}
		else {
			setting.setCompareNum(setting.getGreaterCompareNum());
			while (isStartIncluded) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue,
						node.getKeyValue(loc), setting) > 0) {
					break;
				}
				if (!prevPos(txn, node, loc)) {
					break;
				}
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
						setting) < isEndIncluded) {
					if (!isComposite<P>() || 
						isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
				}
				else {
					break;
				}
			}
			else {
				setting.setCompareNum(setting.getGreaterCompareNum());
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal,
						setting) < isStartIncluded) {
					setting.setCompareNum(setting.getLessCompareNum());
					if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal,
							endKeyValue, setting) < isEndIncluded) {
						if (!isComposite<P>() || 
							isMatch(txn, *getObjectManager(), currentVal, setting))
						pushResultList<K, V, R>(currentVal, result);
						start = true;
					}
					else {
						break;
					}
				}
			}

			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, node.getKeyValue(loc),
						endKeyValue, setting) < isEndIncluded) {
					isSuspend = true;
					suspendKeyValue = node.getKeyValue(loc);
				}
				break;
			}
		}
	}
	return isSuspend;
}

template <typename P, typename K, typename V, typename R, BtreeMap::CompareComponent C>
bool BtreeMap::findRangeByDescending(
	TransactionContext &txn, KeyValue<P, V> &startKeyValue, int32_t isStartIncluded, KeyValue<P, V> &endKeyValue,
	int32_t isEndIncluded, ResultSize limit,
	util::XArray<R> &result, ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
	Setting &setting) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	CmpFunctor<K, P, V, C> cmpFuncRight;
	
	bool isSuspend = false;
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc;
	setting.setCompareNum(setting.getLessCompareNum());
	bool isEqual = findNode(txn, endKeyValue, node, loc, cmpFuncLeft, setting);

	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		if (loc >= node.numkeyValues() || (isEqual && !isEndIncluded)) {
			prevPos(txn, node, loc);
		}
		int32_t ret, prevRet = -1;
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				setting.setCompareNum(setting.getGreaterCompareNum());
				ret =
					cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal, setting);
				if (ret < isStartIncluded) {
					if (!isComposite<P>() || 
						isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
					prevRet = ret;
				}
				else {
					if (ret == 0 || prevRet != 0) {
						if (!isComposite<P>() || 
							isMatch(txn, *getObjectManager(), currentVal, setting))
						pushResultList<K, V, R>(currentVal, result);
					}
					break;
				}
			}
			else {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
						setting) < isEndIncluded) {
					setting.setCompareNum(setting.getGreaterCompareNum());
					ret =
						cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal, setting);
					if (ret < isStartIncluded) {
						if (!isComposite<P>() || 
							isMatch(txn, *getObjectManager(), currentVal, setting))
						pushResultList<K, V, R>(currentVal, result);
						prevRet = ret;
						start = true;
					}
					else {
						if (ret == 0 || prevRet != 0) {
							if (!isComposite<P>() || 
								isMatch(txn, *getObjectManager(), currentVal, setting))
							pushResultList<K, V, R>(currentVal, result);
						}
						break;
					}
				}
			}
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
		return isSuspend;
	}
	else {
		if (loc >= node.numkeyValues()) {
			prevPos(txn, node, loc);
		}
		else {
			setting.setCompareNum(setting.getLessCompareNum());
			while (isEndIncluded) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, endKeyValue,
						node.getKeyValue(loc), setting) < 0) {
					break;
				}
				if (!nextPos(txn, node, loc)) {
					break;
				}
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				setting.setCompareNum(setting.getGreaterCompareNum());
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal,
						setting) < isStartIncluded) {
					if (!isComposite<P>() || 
						isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
				}
				else {
					break;
				}
			}
			else {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
						setting) < isEndIncluded) {
					setting.setCompareNum(setting.getGreaterCompareNum());
					if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal,
							setting) < isStartIncluded) {
						if (!isComposite<P>() || 
							isMatch(txn, *getObjectManager(), currentVal, setting))
						pushResultList<K, V, R>(currentVal, result);
						start = true;
					}
					else {
						break;
					}
				}
			}
			if (!prevPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				setting.setCompareNum(setting.getGreaterCompareNum());
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue,
						node.getKeyValue(loc), setting) < isStartIncluded) {
					isSuspend = true;
					suspendKeyValue = node.getKeyValue(loc);
				}
				break;
			}
		}
		return isSuspend;
	}
}

template <>
inline std::string BtreeMap::BNode<StringKey, OId>::dump(
		TransactionContext &txn) {
	UNUSED_VARIABLE(txn);
	util::NormalOStringStream out;
	out << "(@@Node@@)";
	for (int32_t i = 0; i < numkeyValues(); ++i) {
		StringCursor obj(*getObjectManager(), allocateStrategy_, getKeyValue(i).key_.oId_);
		util::NormalString tmp(
			reinterpret_cast<const char *>(obj.str()), obj.stringLength());
		out << "[" << tmp << "," << getKeyValue(i).value_ << "], ";
	}
	return out.str();
}

template <>
inline std::string BtreeMap::BNode<FullContainerKeyAddr, OId>::dump(
		TransactionContext &txn) {
	UNUSED_VARIABLE(txn);
	util::NormalOStringStream out;
	out << "(@@Node@@)";
	for (int32_t i = 0; i < numkeyValues(); ++i) {
		StringCursor obj(*getObjectManager(), allocateStrategy_, getKeyValue(i).key_.oId_);
		util::NormalString tmp(
			reinterpret_cast<const char *>(obj.str()), obj.stringLength());
		out << "[" << tmp << "," << getKeyValue(i).value_ << "], ";
	}
	return out.str();
}

template <>
inline std::string BtreeMap::BNode<FullContainerKeyAddr, KeyDataStoreValue>::dump(
	TransactionContext& txn) {
	util::NormalOStringStream out;
	out << "(@@Node@@)";
	for (int32_t i = 0; i < numkeyValues(); ++i) {
		FullContainerKeyCursor obj(*getObjectManager(),
			allocateStrategy_, getKeyValue(i).key_.oId_);
		util::String str(txn.getDefaultAllocator());
		obj.getKey().toString(txn.getDefaultAllocator(), str);
		out << "[" << str.c_str() << "," << getKeyValue(i).value_ << "], ";
	}
	return out.str();
}

template <typename K, typename V>
int32_t BtreeMap::initialize(TransactionContext &txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	UNUSED_VARIABLE(elemSize);
	isUnique_ = isUnique;
	keyType_ = columnType;
	btreeMapType_ = btreeMapType;

	BaseObject::allocate<BNodeImage<K, V> >(getInitialNodeSize<K, V>(sizeof(KeyValue<K, V>)),
		getBaseOId(), OBJECT_TYPE_BTREE_MAP);
	BNode<K, V> rootNode(this, allocateStrategy_);
	rootNode.initialize(txn, getBaseOId(), true,
		getInitialItemSizeThreshold<K, V>(), elemSize_);

	rootNode.setRootNodeHeader(
		columnType, btreeMapType, static_cast<uint8_t>(isUnique), UNDEF_OID);

	return GS_SUCCESS;
}

template <>
inline int32_t BtreeMap::insert
	(TransactionContext &txn, FullContainerKeyCursor &key, OId &value, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return insertInternal<FullContainerKeyObject, FullContainerKeyAddr, OId>(txn, convertKey, value, isCaseSensitive);
}
template <>
inline int32_t BtreeMap::insert
	(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& value, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return insertInternal<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue>(txn, convertKey, value, isCaseSensitive);
}
template <typename K, typename V>
inline int32_t BtreeMap::insert(TransactionContext &txn, K &key, V &value, bool isCaseSensitive) {
	return insertInternal<K, K, V>(txn, key, value, isCaseSensitive);
}
template <>
inline int32_t BtreeMap::remove(TransactionContext &txn, FullContainerKeyCursor &key, OId &value, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return removeInternal<FullContainerKeyObject, FullContainerKeyAddr, OId>(txn, convertKey, value, isCaseSensitive);
}
template <>
inline int32_t BtreeMap::remove(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& value, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return removeInternal<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue>(txn, convertKey, value, isCaseSensitive);
}
template <typename K, typename V>
inline int32_t BtreeMap::remove(TransactionContext &txn, K &key, V &value, bool isCaseSensitive) {
	return removeInternal<K, K, V>(txn, key, value, isCaseSensitive);
}
template <>
inline int32_t BtreeMap::update(TransactionContext &txn, FullContainerKeyCursor &key, OId &oldValue, OId &newValue, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return updateInternal<FullContainerKeyObject, FullContainerKeyAddr, OId>(txn, convertKey, oldValue, newValue, isCaseSensitive);
}

template <>
inline int32_t BtreeMap::update(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& oldValue, KeyDataStoreValue& newValue, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	return updateInternal<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue>(txn, convertKey, oldValue, newValue, isCaseSensitive);
}

template <typename K, typename V>
inline int32_t BtreeMap::update(
	TransactionContext &txn, K &key, V &oldValue, V &newValue, bool isCaseSensitive) {
	return updateInternal<K, K, V>(txn, key, oldValue, newValue, isCaseSensitive);
}

template <>
inline int32_t BtreeMap::search<FullContainerKeyCursor, OId, OId>(TransactionContext &txn, FullContainerKeyCursor &key, OId &retVal, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	if (isEmpty()) {
		return GS_FAIL;
	}
	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	return find<FullContainerKeyObject, FullContainerKeyAddr, OId, KEY_COMPONENT>(txn, convertKey, retVal, setting);
}


template <>
inline int32_t BtreeMap::search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& retVal, bool isCaseSensitive) {
	FullContainerKeyObject convertKey(&key);
	if (isEmpty()) {
		return GS_FAIL;
	}
	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	return find<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue, KEY_COMPONENT>(txn, convertKey, retVal, setting);
}

template <>
inline std::string BtreeMap::dump<FullContainerKeyAddr, KeyDataStoreValue>(TransactionContext& txn) {
	util::NormalOStringStream out;
	print<FullContainerKeyAddr, KeyDataStoreValue>(txn, out, getRootOId());
	return out.str();
}

template <typename K, typename V, typename R>
inline int32_t BtreeMap::search(TransactionContext &txn, K &key, R &retVal, bool isCaseSensitive) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyAddr>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	if (isEmpty()) {
		return GS_FAIL;
	}
	Setting setting(getKeyType(), isCaseSensitive, getFuncInfo());
	return find<K, K, V, KEY_COMPONENT>(txn, key, retVal, setting);	
}

template <>
inline int32_t BtreeMap::search<FullContainerKeyCursor, OId, OId>(TransactionContext &txn, SearchContext &sc,
	util::XArray<OId> &idList, OutputOrder outputOrder) {
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (sc.getSuspendValue() != NULL) {
		return find<FullContainerKeyObject, FullContainerKeyAddr, OId, OId, KEY_VALUE_COMPONENT>(txn, sc, idList, outputOrder);
	} else {
		return find<FullContainerKeyObject, FullContainerKeyAddr, OId, OId, KEY_COMPONENT>(txn, sc, idList, outputOrder);
	}
}

template <>
inline int32_t BtreeMap::search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(TransactionContext& txn, SearchContext& sc,
	util::XArray<KeyDataStoreValue>& idList, OutputOrder outputOrder) {
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (sc.getSuspendValue() != NULL) {
		return find<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue, KeyDataStoreValue, KEY_VALUE_COMPONENT>(txn, sc, idList, outputOrder);
	}
	else {
		return find<FullContainerKeyObject, FullContainerKeyAddr, KeyDataStoreValue, KeyDataStoreValue, KEY_COMPONENT>(txn, sc, idList, outputOrder);
	}
}

template <typename K, typename V, typename R>
inline int32_t BtreeMap::search(TransactionContext &txn, SearchContext &sc,
	util::XArray<R> &idList, OutputOrder outputOrder) {
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyAddr>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyCursor>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (sc.getSuspendValue() != NULL) {
		return find<K, K, V, R, KEY_VALUE_COMPONENT>(txn, sc, idList, outputOrder);
	} else {
		return find<K, K, V, R, KEY_COMPONENT>(txn, sc, idList, outputOrder);
	}
}

struct MvccRowImage;
template<>
MvccRowImage BtreeMap::getMaxValue();
template <typename V>
V BtreeMap::getMaxValue() {
	return std::numeric_limits<V>::max();
}

template<>
MvccRowImage BtreeMap::getMinValue();
template <typename V>
V BtreeMap::getMinValue() {
	return std::numeric_limits<V>::min();
}

template <>
int32_t BtreeMap::initialize<FullContainerKeyCursor, OId>(TransactionContext &txn, ColumnType columnType,
													bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize);
template <>
int32_t BtreeMap::insert< FullContainerKeyCursor, OId>
	(TransactionContext &txn, FullContainerKeyCursor &key, OId &value, bool isCaseSensitive);

template <>
int32_t BtreeMap::remove<FullContainerKeyCursor, OId>
	(TransactionContext &txn, FullContainerKeyCursor &key, OId &value, bool isCaseSensitive);

template <>
int32_t BtreeMap::update<FullContainerKeyCursor, OId>
	(TransactionContext &txn, FullContainerKeyCursor &key, OId &oldValue, OId &newValue, bool isCaseSensitive);

template <>
int32_t BtreeMap::search<FullContainerKeyCursor, OId, OId>(TransactionContext &txn, FullContainerKeyCursor &key, OId &retVal, bool isCaseSensitive);

template <>
int32_t BtreeMap::search<FullContainerKeyCursor, OId, OId>(TransactionContext &txn, SearchContext &sc, util::XArray<OId> &idList, OutputOrder outputOrder);

template <>
int32_t BtreeMap::initialize<FullContainerKeyCursor, KeyDataStoreValue>(TransactionContext& txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize);
template <>
int32_t BtreeMap::insert< FullContainerKeyCursor, KeyDataStoreValue>
(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& value, bool isCaseSensitive);

template <>
int32_t BtreeMap::remove<FullContainerKeyCursor, KeyDataStoreValue>
(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& value, bool isCaseSensitive);

template <>
int32_t BtreeMap::update<FullContainerKeyCursor, KeyDataStoreValue>
(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& oldValue, KeyDataStoreValue& newValue, bool isCaseSensitive);

template <>
int32_t BtreeMap::search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(TransactionContext& txn, FullContainerKeyCursor& key, KeyDataStoreValue& retVal, bool isCaseSensitive);

template <>
int32_t BtreeMap::search<FullContainerKeyCursor, KeyDataStoreValue, KeyDataStoreValue>(TransactionContext& txn, SearchContext& sc, util::XArray<KeyDataStoreValue>& idList, OutputOrder outputOrder);

template <>
std::string BtreeMap::dump<FullContainerKeyCursor, KeyDataStoreValue>(TransactionContext& txn);

template <typename K, typename V>
int32_t BtreeMap::getInitialItemSizeThreshold() {
	return INITIAL_DEFAULT_ITEM_SIZE_THRESHOLD;
}
template <>
int32_t BtreeMap::getInitialItemSizeThreshold<TransactionId, MvccRowImage>();

inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringObject &e1,
	const StringKey &e2, Setting &) {
	StringCursor *obj1 = reinterpret_cast<StringCursor *>(e1.ptr_);
	StringCursor obj2(objectManager, strategy, e2.oId_);
	return compareStringString(txn, obj1->str(), obj1->stringLength(),
		obj2.str(), obj2.stringLength());
}

inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringKey &e1,
	const StringObject &e2, Setting &) {
	StringCursor obj1(objectManager, strategy, e1.oId_);
	StringCursor *obj2 = reinterpret_cast<StringCursor *>(e2.ptr_);
	return compareStringString(txn, obj1.str(), obj1.stringLength(),
		obj2->str(), obj2->stringLength());
}

inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyObject &e1,
	const FullContainerKeyAddr &e2, Setting &setting) {
	const FullContainerKeyCursor *obj1 = reinterpret_cast<const FullContainerKeyCursor *>(e1.ptr_);
	FullContainerKeyCursor obj2(objectManager, strategy, e2.oId_);
	return FullContainerKey::compareTo(txn, obj1->getKeyBody(), obj2.getKeyBody(), setting.isCaseSensitive());
}

inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyAddr &e1,
	const FullContainerKeyObject &e2, Setting &setting) {
	FullContainerKeyCursor obj1(objectManager, strategy, e1.oId_);
	const FullContainerKeyCursor *obj2 = reinterpret_cast<const FullContainerKeyCursor *>(e2.ptr_);

	return FullContainerKey::compareTo(txn, obj1.getKeyBody(), obj2->getKeyBody(), setting.isCaseSensitive());
}

template <typename K>
inline int32_t BtreeMap::keyCmp(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, const K &e1, const K &e2, Setting &) {
	UNUSED_VARIABLE(strategy);

	UTIL_STATIC_ASSERT((!util::IsSame<K, StringKey>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, StringObject>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyAddr>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, FullContainerKeyObject>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, CompositeInfoObject>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, float>::VALUE));
	UTIL_STATIC_ASSERT((!util::IsSame<K, double>::VALUE));
	UNUSED_VARIABLE(txn);
	UNUSED_VARIABLE(objectManager);
	return e1 < e2 ? -1 : (e1 == e2 ? 0 : 1);
}


template <>
inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringKey &e1,
	const StringKey &e2, Setting &) {
	StringCursor obj1(objectManager, strategy, e1.oId_);
	StringCursor obj2(objectManager, strategy, e2.oId_);
	return compareStringString(
		txn, obj1.str(), obj1.stringLength(), obj2.str(), obj2.stringLength());
}


template <>
inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyAddr &e1,
	const FullContainerKeyAddr &e2, Setting &setting) {

	FullContainerKeyCursor obj1(objectManager, strategy, e1.oId_);
	FullContainerKeyCursor obj2(objectManager, strategy, e2.oId_);

	return FullContainerKey::compareTo(txn, obj1.getKeyBody(), obj2.getKeyBody(), setting.isCaseSensitive());
}

template <>
inline int32_t BtreeMap::keyCmp(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &,
	const double &e1, const double &e2, Setting &) {
	if (util::isNaN(e1)) {
		if (util::isNaN(e2)) {
			return 0;
		}
		else {
			return 1;
		}
	}
	else if (util::isNaN(e2)) {
		return -1;
	}
	else {
		return e1 < e2 ? -1 : (e1 == e2 ? 0 : 1);
	}
}

template <>
inline int32_t BtreeMap::keyCmp(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &,
	const float &e1, const float &e2, Setting &) {
	if (util::isNaN(e1)) {
		if (util::isNaN(e2)) {
			return 0;
		}
		else {
			return 1;
		}
	}
	else if (util::isNaN(e2)) {
		return -1;
	}
	else {
		return e1 < e2 ? -1 : (e1 == e2 ? 0 : 1);
	}
}



template <typename S>
inline bool BtreeMap::compositeInfoMatch(TransactionContext &txn, ObjectManagerV4 &objectManager,
	const S *e, BaseIndex::Setting &setting) {
	UNUSED_VARIABLE(txn);
	UNUSED_VARIABLE(objectManager);
	UNUSED_VARIABLE(e);
	UNUSED_VARIABLE(setting);
	return true;
}

template <>
inline int32_t BtreeMap::keyCmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	const CompositeInfoObject &e1, const CompositeInfoObject &e2, Setting &setting) {
	return compositeInfoCmp(txn, objectManager, strategy, &e1, &e2, setting);
}

	template <>
	bool BtreeMap::compositeInfoMatch(TransactionContext &txn, ObjectManagerV4 &objectManager,
		const CompositeInfoObject *e, BaseIndex::Setting &setting);


	template <>
	inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const StringKey &e1,
		const StringKey &e2, BaseIndex::Setting &);
	template <>
	inline int32_t BtreeMap::keyCmp(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, const FullContainerKeyAddr &e1,
		const FullContainerKeyAddr &e2, BaseIndex::Setting &setting);

	template <>
	inline int32_t BtreeMap::keyCmp(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &,
		const double &e1, const double &e2, BaseIndex::Setting &);

	template <>
	inline int32_t BtreeMap::keyCmp(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &,
		const float &e1, const float &e2, BaseIndex::Setting &);

	template <>
	inline int32_t BtreeMap::keyCmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
		const CompositeInfoObject &e1, const CompositeInfoObject &e2, BaseIndex::Setting &setting);

#endif  
