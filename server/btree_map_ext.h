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
	@brief Definition of BtreeMap extension
*/
#ifndef BTREE_MAP_EXT_H_
#define BTREE_MAP_EXT_H_

#include "btree_map.h"

struct BtreeMap::TermConditionUpdator {
public:
	virtual ~TermConditionUpdator();

	virtual void next() = 0;

	void bind(SearchContext &sc) {
		sc_ = &sc;
		update(sc);
	}

	void unbind() {
		sc_ = NULL;
	}

	bool exists() const {
		return exists_;
	}
	void close() {
		exists_ = false;
	}

protected:
	TermConditionUpdator() :
			exists_(true), sc_(NULL) {
	}

	virtual void update(SearchContext &sc) = 0;

	void reset() {
		exists_ = true;
	}

	SearchContext* getActiveSearchContext() {
		return sc_;
	}

private:
	bool exists_;
	SearchContext *sc_;
};

template<typename P, typename K, typename V, typename R>
struct BtreeMap::TermConditionRewriter {
public:
	TermConditionRewriter(TransactionContext&, SearchContext &sc) :
			sc_(sc) {
	}

	void rewrite() {
	}

private:
	SearchContext &sc_;
};

template<>
struct BtreeMap::TermConditionRewriter<StringObject, StringKey, OId, OId> {
public:
	TermConditionRewriter(
			TransactionContext &txn, SearchContext &sc);
	~TermConditionRewriter();

	void rewrite();

private:
	enum {
		ENTRY_COUNT = 2
	};

	typedef util::XArray<uint8_t> Buffer;

	struct Entry {
		Entry() :
				cond_(NULL),
				orgKey_(NULL) {
		}

		util::LocalUniquePtr<Buffer> buf_;
		util::LocalUniquePtr<StringCursor> cursor_;
		StringObject obj_;
		TermCondition *cond_;
		const void *orgKey_;
	};

	Entry entryList_[ENTRY_COUNT];
	SearchContext &sc_;
};

struct BtreeMap::SearchBulkFunc {
	SearchBulkFunc(
			TransactionContext &txn, SearchContext &sc,
			util::XArray<OId> &idList, OutputOrder outputOrder, BtreeMap *tree) :
			txn_(txn), sc_(sc), idList_(idList),
			outputOrder_(outputOrder), tree_(tree), ret_(GS_SUCCESS) {
	}

	template<typename P, typename K, typename V, typename R>
	void execute();

	TransactionContext &txn_;
	SearchContext &sc_;
	util::XArray<OId> &idList_;
	OutputOrder outputOrder_;
	BtreeMap *tree_;
	int32_t ret_;
};

struct BtreeMap::EstimateFunc {
	EstimateFunc(
			TransactionContext &txn, SearchContext &sc, BtreeMap *tree) :
			txn_(txn), sc_(sc), tree_(tree), ret_(0) {
	}

	template<typename P, typename K, typename V, typename R>
	void execute();

	TransactionContext &txn_;
	SearchContext &sc_;
	BtreeMap *tree_;
	uint64_t ret_;
};

#endif 
