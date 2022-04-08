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
#ifndef CONTAINER_MESSAGE_V4_H_
#define CONTAINER_MESSAGE_V4_H_


#include "data_store_log_v4.h"

class ResultSet;
class ResultSetOption;
class ResultSetGuard;


/*!
	@brief Represents time-related condition
*/
struct TimeRelatedCondition {
	TimeRelatedCondition()
		: rowKey_(UNDEF_TIMESTAMP), operator_(TIME_PREV) {}

	Timestamp rowKey_;
	TimeOperator operator_;
};

/*!
	@brief Represents interpolation condition
*/
struct InterpolateCondition {
	InterpolateCondition()
		: rowKey_(UNDEF_TIMESTAMP), columnId_(UNDEF_COLUMNID) {}

	Timestamp rowKey_;
	ColumnId columnId_;
};

/*!
	@brief Represents aggregate condition
*/
struct AggregateQuery {
	AggregateQuery()
		: start_(UNDEF_TIMESTAMP),
		end_(UNDEF_TIMESTAMP),
		columnId_(UNDEF_COLUMNID),
		aggregationType_(AGG_MIN) {}

	Timestamp start_;
	Timestamp end_;
	ColumnId columnId_;
	AggregationType aggregationType_;
};

/*!
	@brief Represents sampling condition
*/
struct SamplingQuery {
	explicit SamplingQuery(util::StackAllocator& alloc)
		: start_(UNDEF_TIMESTAMP),
		end_(UNDEF_TIMESTAMP),
		interval_(0),
		timeUnit_(TIME_UNIT_YEAR),
		interpolatedColumnIdList_(alloc),
		mode_(INTERP_MODE_LINEAR_OR_PREVIOUS) {}

	inline Sampling toSamplingOption() const;

	Timestamp start_;
	Timestamp end_;
	uint32_t interval_;
	TimeUnit timeUnit_;
	util::XArray<uint32_t> interpolatedColumnIdList_;
	InterpolationMode mode_;
};

inline Sampling SamplingQuery::toSamplingOption() const {
	Sampling sampling;
	sampling.interval_ = interval_;
	sampling.timeUnit_ = timeUnit_;

	sampling.interpolatedColumnIdList_.resize(interpolatedColumnIdList_.size());
	std::copy(interpolatedColumnIdList_.begin(),
		interpolatedColumnIdList_.end(),
		sampling.interpolatedColumnIdList_.begin());
	sampling.mode_ = mode_;

	return sampling;
}

/*!
	@brief Represents time range condition
*/
struct RangeQuery {
	RangeQuery()
		: start_(UNDEF_TIMESTAMP),
		end_(UNDEF_TIMESTAMP),
		order_(ORDER_ASCENDING) {}

	Timestamp start_;
	Timestamp end_;
	OutputOrder order_;
};

/*!
	@brief Represents geometry query
*/
struct GeometryQuery {
	explicit GeometryQuery(util::StackAllocator& alloc)
		: columnId_(UNDEF_COLUMNID),
		intersection_(alloc),
		disjoint_(alloc),
		operator_(GEOMETRY_INTERSECT) {}

	ColumnId columnId_;
	util::XArray<uint8_t> intersection_;
	util::XArray<uint8_t> disjoint_;
	GeometryOperator operator_;
};

struct BGTask {
	BGTask() {
		bgId_ = UNDEF_BACKGROUND_ID;
	}
	BackgroundId bgId_;
};

struct DSInputMes : public Serializable {
	struct EmptyMessage : public Serializable {
		EmptyMessage() : Serializable(NULL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};

	struct PutContainer : public Serializable {
		PutContainer(util::StackAllocator& alloc, const FullContainerKey* key,
			ContainerType containerType, util::XArray<uint8_t>* containerInfo,
			bool modifiable, int32_t featureVersion, bool isCaseSensitive) :
			Serializable(&alloc), key_(key), containerType_(containerType),
			containerInfo_(containerInfo),
			modifiable_(modifiable), featureVersion_(featureVersion),
			isCaseSensitive_(isCaseSensitive) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		const FullContainerKey* key_;
		ContainerType containerType_;
		util::XArray<uint8_t>* containerInfo_;
		bool modifiable_;
		int32_t featureVersion_;
		bool isCaseSensitive_; 
	};
	struct UpdateTablePartitioningId : public Serializable {
		UpdateTablePartitioningId(util::StackAllocator& alloc, TablePartitioningVersionId id) :
			Serializable(&alloc), id_(id) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		TablePartitioningVersionId id_;
	};

	struct DropContainer : public Serializable {
		DropContainer(util::StackAllocator& alloc, const FullContainerKey* key,
			ContainerType containerType, bool isCaseSensitive) :
			Serializable(&alloc), key_(key), containerType_(containerType),
			isCaseSensitive_(isCaseSensitive) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		const FullContainerKey* key_;
		ContainerType containerType_;
		bool isCaseSensitive_; 
	};
	struct GetContainer : public Serializable {
		GetContainer(util::StackAllocator& alloc, const FullContainerKey* key,
			ContainerType containerType) :
			Serializable(&alloc), key_(key), containerType_(containerType) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		const FullContainerKey* key_;
		ContainerType containerType_;
	};
	struct CreateIndex : public Serializable {
		CreateIndex(util::StackAllocator& alloc, IndexInfo* info, bool isCaseSensitive,
			CreateDropIndexMode mode, SchemaVersionId verId) :
			Serializable(&alloc), info_(info),
			isCaseSensitive_(isCaseSensitive), mode_(mode), verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		IndexInfo* info_;
		bool isCaseSensitive_; 
		CreateDropIndexMode mode_;
		SchemaVersionId verId_;
	};
	struct DropIndex : public Serializable {
		DropIndex(util::StackAllocator& alloc, IndexInfo* info, bool isCaseSensitive,
			CreateDropIndexMode mode, SchemaVersionId verId) :
			Serializable(&alloc), info_(info),
			isCaseSensitive_(isCaseSensitive), mode_(mode), verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		IndexInfo* info_;
		bool isCaseSensitive_; 
		CreateDropIndexMode mode_;
		SchemaVersionId verId_;
	};
	struct ContinueCreateIndex : public EmptyMessage {
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};
	struct ContinueAlterContainer : public EmptyMessage {
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}
	};
	struct Commit : public Serializable {
		Commit(util::StackAllocator& alloc, bool allowExpiration = true) :
			Serializable(&alloc), allowExpiration_(allowExpiration) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}

		bool allowExpiration_;
	};
	struct Abort : public Serializable {
		Abort(util::StackAllocator& alloc, bool allowExpiration = true) :
			Serializable(&alloc), allowExpiration_(allowExpiration) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {}
		template <typename S>
		void decode(S& in) {}

		bool allowExpiration_;
	};
	struct GetContainerObject : public Serializable {
		GetContainerObject(util::StackAllocator& alloc, ContainerType containerType, bool allowExpiration) :
			Serializable(&alloc), containerType_(containerType), allowExpiration_(allowExpiration) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		ContainerType containerType_;
		bool allowExpiration_;
	};

	struct PutRow : public Serializable {
		typedef util::XArray<uint8_t> RowData;
		PutRow(util::StackAllocator& alloc, RowData* rowData, PutRowOption option, SchemaVersionId verId, bool isEmptyLog = false) :
			Serializable(&alloc), rowData_(rowData), putRowOption_(option), verId_(verId), isEmptyLog_(isEmptyLog){}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RowData* rowData_;
		PutRowOption putRowOption_;
		SchemaVersionId verId_;
		bool isEmptyLog_;
	};


	struct AppendRow : public Serializable {
		typedef util::XArray<uint8_t> RowData;
		AppendRow(util::StackAllocator& alloc, RowData* rowData, SchemaVersionId verId) :
			Serializable(&alloc), rowData_(rowData), verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RowData* rowData_;
		SchemaVersionId verId_;
	};
	struct RemoveRow : public Serializable {
		typedef util::XArray<uint8_t> KeyData;
		RemoveRow(util::StackAllocator& alloc, KeyData* keyData, SchemaVersionId verId) :
			Serializable(&alloc), keyData_(keyData), verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		KeyData* keyData_;
		SchemaVersionId verId_;
	};
	struct UpdateRowById : public Serializable {
		typedef util::XArray<uint8_t> RowData;
		UpdateRowById(util::StackAllocator& alloc, RowId rowId, RowData* rowData, SchemaVersionId verId, bool isEmptyLog = false) :
			Serializable(&alloc), rowId_(rowId), rowData_(rowData), verId_(verId), isEmptyLog_(isEmptyLog) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RowId rowId_;
		RowData* rowData_;
		SchemaVersionId verId_;
		bool isEmptyLog_;
	};
	struct RemoveRowById : public Serializable {
		RemoveRowById(util::StackAllocator& alloc, RowId rowId, SchemaVersionId verId) :
			Serializable(&alloc), rowId_(rowId), verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RowId rowId_;
		SchemaVersionId verId_;
	};
	struct GetRow : public Serializable {
		typedef util::XArray<uint8_t> KeyData;
		GetRow(util::StackAllocator& alloc, KeyData* keyData, bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
			Serializable(&alloc), keyData_(keyData), forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		KeyData* keyData_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct GetRowSet : public Serializable {
		GetRowSet(util::StackAllocator& alloc, RowId position,
			ResultSize limit, ResultSize size, 
			SchemaVersionId verId, EventMonotonicTime emNow) :
			Serializable(&alloc), position_(position),
			limit_(limit), size_(size),
			verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RowId position_;
		ResultSize limit_;
		ResultSize size_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryTQL : public Serializable {
		QueryTQL(util::StackAllocator& alloc,
			const char* dbName, FullContainerKey* containerKey,
			util::String& query, ResultSize limit,
			ResultSize size, ResultSetOption* rsQueryOption,
			bool forUpdate, SchemaVersionId verId,
			EventMonotonicTime emNow) :
			Serializable(&alloc),
			dbName_(dbName), containerKey_(containerKey),
			query_(query), limit_(limit), size_(size),
			rsQueryOption_(rsQueryOption),
			forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		const char* dbName_;
		FullContainerKey* containerKey_;
		util::String& query_;
		ResultSize limit_;
		ResultSize size_;
		ResultSetOption* rsQueryOption_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryGeometryWithExclusion : public Serializable {
		QueryGeometryWithExclusion(util::StackAllocator& alloc,
			GeometryQuery& query, ResultSize limit,
			ResultSize size,
			bool forUpdate, SchemaVersionId verId,
			EventMonotonicTime emNow) :
			Serializable(&alloc),
			query_(query), limit_(limit), size_(size),
			forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		GeometryQuery& query_;
		ResultSize limit_;
		ResultSize size_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryGeometryRelated : public Serializable {
		QueryGeometryRelated(util::StackAllocator& alloc,
			GeometryQuery& query, ResultSize limit,
			ResultSize size,
			bool forUpdate, SchemaVersionId verId,
			EventMonotonicTime emNow) :
			Serializable(&alloc),
			query_(query), limit_(limit), size_(size),
			forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		GeometryQuery& query_;
		ResultSize limit_;
		ResultSize size_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct GetRowTimeRelated : public Serializable {
		GetRowTimeRelated(util::StackAllocator& alloc, TimeRelatedCondition& condition, SchemaVersionId verId, EventMonotonicTime emNow) :
			Serializable(&alloc), condition_(condition), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		TimeRelatedCondition condition_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryInterPolate : public Serializable {
		QueryInterPolate(util::StackAllocator& alloc, InterpolateCondition& condition, SchemaVersionId verId, EventMonotonicTime emNow) :
			Serializable(&alloc), condition_(condition), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		InterpolateCondition condition_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryAggregate : public Serializable {
		QueryAggregate(util::StackAllocator& alloc, AggregateQuery& query, SchemaVersionId verId, EventMonotonicTime emNow) :
			Serializable(&alloc), query_(query), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		AggregateQuery query_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};

	struct QueryTimeRange : public Serializable {
		QueryTimeRange(util::StackAllocator& alloc,
			RangeQuery& query, ResultSize limit,
			ResultSize size,
			bool forUpdate, SchemaVersionId verId,
			EventMonotonicTime emNow) :
			Serializable(&alloc),
			query_(query), limit_(limit), size_(size),
			forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		RangeQuery& query_;
		ResultSize limit_;
		ResultSize size_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryTimeSampling : public Serializable {
		QueryTimeSampling(util::StackAllocator& alloc,
			SamplingQuery& query, ResultSize limit,
			ResultSize size,
			bool forUpdate, SchemaVersionId verId,
			EventMonotonicTime emNow) :
			Serializable(&alloc),
			query_(query), limit_(limit), size_(size),
			forUpdate_(forUpdate), verId_(verId), emNow_(emNow) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		SamplingQuery& query_;
		ResultSize limit_;
		ResultSize size_;
		bool forUpdate_;
		SchemaVersionId verId_;
		EventMonotonicTime emNow_;
	};
	struct QueryFetchResultSet : public Serializable {
		QueryFetchResultSet(util::StackAllocator& alloc,
			ResultSetId rsId,
			ResultSize startPos,
			ResultSize fetchNum,
			SchemaVersionId verId) :
			Serializable(&alloc), rsId_(rsId),
			startPos_(startPos), fetchNum_(fetchNum),
			verId_(verId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		ResultSetId rsId_;
		ResultSize startPos_;
		ResultSize fetchNum_;
		SchemaVersionId verId_;
	};
	struct QueryCloseResultSet : public Serializable {
		QueryCloseResultSet(util::StackAllocator& alloc,
			ResultSetId rsId) :
			Serializable(&alloc), rsId_(rsId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		ResultSetId rsId_;
	};

	struct ScanChunkGroup : public Serializable {
		ScanChunkGroup(util::StackAllocator& alloc, Timestamp currentTime) : Serializable(&alloc), currentTime_(currentTime) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		Timestamp currentTime_;
	};
	struct SearchBackgroundTask : public EmptyMessage {
	};
	struct ExecuteBackgroundTask : public Serializable {
		ExecuteBackgroundTask(util::StackAllocator& alloc, BGTask bgTask) : Serializable(&alloc), bgTask_(bgTask) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		BGTask bgTask_;
	};
	struct CheckTimeoutResultSet : public Serializable {
		CheckTimeoutResultSet(util::StackAllocator& alloc, EventMonotonicTime currentTime) : 
			Serializable(&alloc), currentTime_(currentTime) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}

		template <typename S>
		void encode(S& out) {
		}
		template <typename S>
		void decode(S& in) {
		}
		EventMonotonicTime currentTime_;
	};


	DSInputMes(util::StackAllocator& alloc, DSOperationType type) :
		Serializable(&alloc), type_(type) {
		switch (type_) {
		case DS_COMMIT:
			value_.commit_ = ALLOC_NEW(*alloc_) Commit(*alloc_);
			break;
		case DS_ABORT:
			value_.abort_ = ALLOC_NEW(*alloc_) Abort(*alloc_);
			break;
		case DS_CONTINUE_CREATE_INDEX:
			value_.continueCreateIndex_ = ALLOC_NEW(*alloc_) ContinueCreateIndex();
			break;
		case DS_CONTINUE_ALTER_CONTAINER:
			value_.continueAlterContainer_ = ALLOC_NEW(*alloc_) ContinueAlterContainer();
			break;
		case DS_SEARCH_BACKGROUND_TASK:
			value_.searchBackgroundTask_ = ALLOC_NEW(*alloc_) SearchBackgroundTask();
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}

	DSInputMes(util::StackAllocator& alloc) :
		Serializable(&alloc), type_(DS_UNDEF) {}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		bool allowExpiration) :
		Serializable(&alloc), type_(type) {
		switch (type_) {
		case DS_COMMIT:
			value_.commit_ = ALLOC_NEW(*alloc_) Commit(*alloc_, allowExpiration);
			break;
		case DS_ABORT:
			value_.abort_ = ALLOC_NEW(*alloc_) Abort(*alloc_, allowExpiration);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type, util::XArray<uint8_t>* binary, PutRowOption option,
		SchemaVersionId verId, bool isEmptyLog = false) :
		Serializable(&alloc), type_(type) {
		value_.putRow_ = ALLOC_NEW(*alloc_) PutRow(alloc, binary, option, verId, isEmptyLog);
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type, util::XArray<uint8_t>* binary,
		SchemaVersionId verId) :
		Serializable(&alloc), type_(type) {
		switch (type_) {
		case DS_APPEND_ROW:
			value_.appendRow_ = ALLOC_NEW(*alloc_) AppendRow(alloc, binary, verId);
			break;
		case DS_REMOVE_ROW:
			value_.removeRow_ = ALLOC_NEW(*alloc_) RemoveRow(alloc, binary, verId);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type, RowId rowId, util::XArray<uint8_t>* binary,
		SchemaVersionId verId, bool isEmptyLog = false) :
		Serializable(&alloc), type_(type) {
		value_.updateRowById_ = ALLOC_NEW(*alloc_) UpdateRowById(alloc, rowId, binary, verId, isEmptyLog);
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type, RowId rowId,
		SchemaVersionId verId) :
		Serializable(&alloc), type_(type) {
		value_.removeRowById_ = ALLOC_NEW(*alloc_) RemoveRowById(alloc, rowId, verId);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type, util::XArray<uint8_t>* binary,
		bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.getRow_ = ALLOC_NEW(*alloc_) GetRow(alloc, binary, forUpdate, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		RowId position, ResultSize limit,
		ResultSize size,
		SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.getRowSet_ = ALLOC_NEW(*alloc_) GetRowSet(alloc, position, limit, size, verId, emNow);
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type, 
		const char* dbName, FullContainerKey* containerKey,
		util::String& query, ResultSize limit,
		ResultSize size, ResultSetOption* rsQueryOption,
		bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.queryTQL_ = ALLOC_NEW(*alloc_) QueryTQL(alloc, dbName, containerKey, query, limit, size, rsQueryOption, forUpdate, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		GeometryQuery& query, ResultSize limit,
		ResultSize size,
		bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		if (type == DS_QUERY_GEOMETRY_WITH_EXCLUSION) {
			value_.queryGeometryWithExclusion_ = ALLOC_NEW(*alloc_) QueryGeometryWithExclusion(alloc, query, limit, size, forUpdate, verId, emNow);
		}
		else {
			value_.queryGeometryRelated_ = ALLOC_NEW(*alloc_) QueryGeometryRelated(alloc, query, limit, size, forUpdate, verId, emNow);
		}
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		TimeRelatedCondition& condition,
		SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.getRowTimeRelated_ = ALLOC_NEW(*alloc_) GetRowTimeRelated(alloc, condition, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		AggregateQuery& query,
		SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.queryAggregate_ = ALLOC_NEW(*alloc_) QueryAggregate(alloc, query, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		RangeQuery& query, ResultSize limit,
		ResultSize size,
		bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.queryTimeRange_ = ALLOC_NEW(*alloc_) QueryTimeRange(alloc, query, limit, size, forUpdate, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		InterpolateCondition& query,
		SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.queryInterPolate_ = ALLOC_NEW(*alloc_) QueryInterPolate(alloc, query, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		SamplingQuery& query, ResultSize limit,
		ResultSize size,
		bool forUpdate, SchemaVersionId verId, EventMonotonicTime emNow) :
		Serializable(&alloc), type_(type) {
		value_.queryTimeSampling_ = ALLOC_NEW(*alloc_) QueryTimeSampling(alloc, query, limit, size, forUpdate, verId, emNow);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		ResultSetId rsId,
		ResultSize startPos,
		ResultSize fetchNum,
		SchemaVersionId verId) :
		Serializable(&alloc), type_(type) {
		value_.queryFetchResultSet_ = ALLOC_NEW(*alloc_) QueryFetchResultSet(alloc, rsId, startPos, fetchNum, verId);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		int64_t val) :
		Serializable(&alloc), type_(type) {
		switch (type_) {
		case DS_QUERY_CLOSE_RESULT_SET:
			value_.queryCloseResultSet_ = ALLOC_NEW(*alloc_) QueryCloseResultSet(alloc, val);
			break;
		case DS_SCAN_CHUNK_GROUP:
			value_.scanChunkGroup_ = ALLOC_NEW(*alloc_) ScanChunkGroup(alloc, val);
			break;
		case DS_CHECK_TIMEOUT_RESULT_SET:
			value_.checkTimeoutResultSet_ = ALLOC_NEW(*alloc_) CheckTimeoutResultSet(alloc, val);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		const FullContainerKey* containerKey, TablePartitioningVersionId id) :
		Serializable(&alloc), type_(type) {
		value_.updateTablePartitioningId_ = ALLOC_NEW(*alloc_) UpdateTablePartitioningId(alloc, id);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		const FullContainerKey* key, ContainerType containerType, bool isCaseSensitive) :
		Serializable(&alloc), type_(type) {
		value_.dropContainer_ = ALLOC_NEW(*alloc_) DropContainer(alloc, key, containerType, isCaseSensitive);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		const FullContainerKey* key, ContainerType containerType) :
		Serializable(&alloc), type_(type) {
		value_.getContainer_ = ALLOC_NEW(*alloc_) GetContainer(alloc, key, containerType);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		const FullContainerKey* key, ContainerType containerType, util::XArray<uint8_t>* binary, 
		bool modifiable, int32_t featureVersion, bool isCaseSensitive) :
		Serializable(&alloc), type_(type) {
		value_.putContainer_ = ALLOC_NEW(*alloc_) PutContainer(alloc, key, containerType, binary,
			modifiable, featureVersion, isCaseSensitive);
	}

	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		IndexInfo* info, bool isCaseSensitive, CreateDropIndexMode mode,
		SchemaVersionId verId) :
		Serializable(&alloc), type_(type) {
		switch (type_) {
		case DS_CREATE_INDEX:
			value_.createIndex_ = ALLOC_NEW(*alloc_) CreateIndex(alloc, info, isCaseSensitive, mode, verId);
			break;
		case DS_DROP_INDEX:
			value_.dropIndex_ = ALLOC_NEW(*alloc_) DropIndex(alloc, info, isCaseSensitive, mode, verId);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		ContainerType containerType, bool allowExpiration = false) :
		Serializable(&alloc), type_(type) {
		value_.getContainerObject_ = ALLOC_NEW(*alloc_) GetContainerObject(alloc, containerType, allowExpiration);
	}
	DSInputMes(util::StackAllocator& alloc, DSOperationType type,
		BGTask bgTask) :
		Serializable(&alloc), type_(type) {
		value_.executeBackgroundTask_ = ALLOC_NEW(*alloc_) ExecuteBackgroundTask(alloc, bgTask);
	}

	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}

	template <typename S>
	void encode(S& out) {
		switch (type_) {
		case DS_COMMIT:
			value_.commit_->encode(out);
			break;
		case DS_ABORT:
			value_.abort_->encode(out);
			break;
		case DS_PUT_CONTAINER:
			value_.putContainer_->encode(out);
			break;
		case DS_DROP_CONTAINER:
			value_.dropContainer_->encode(out);
			break;
		case DS_CREATE_INDEX:
			value_.createIndex_->encode(out);
			break;
		case DS_DROP_INDEX:
			value_.dropIndex_->encode(out);
			break;
		case DS_CONTINUE_CREATE_INDEX:
			value_.continueCreateIndex_->encode(out);
			break;
		case DS_CONTINUE_ALTER_CONTAINER:
			value_.continueAlterContainer_->encode(out);
			break;
		case DS_PUT_ROW:
			value_.putRow_->encode(out);
			break;
		case DS_UPDATE_ROW_BY_ID:
			value_.updateRowById_->encode(out);
			break;
		case DS_REMOVE_ROW:
			value_.removeRow_->encode(out);
			break;
		case DS_REMOVE_ROW_BY_ID:
			value_.removeRowById_->encode(out);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}
	template <typename S>
	void decode(S& in) {
		switch (type_) {
		case DS_COMMIT:
			value_.commit_->decode(in);
			break;
		case DS_ABORT:
			value_.abort_->decode(in);
			break;
		case DS_PUT_CONTAINER:
			value_.putContainer_->decode(in);
			break;
		case DS_DROP_CONTAINER:
			value_.dropContainer_->decode(in);
			break;
		case DS_CREATE_INDEX:
			value_.createIndex_->decode(in);
			break;
		case DS_DROP_INDEX:
			value_.dropIndex_->decode(in);
			break;
		case DS_CONTINUE_CREATE_INDEX:
			value_.continueCreateIndex_->decode(in);
			break;
		case DS_CONTINUE_ALTER_CONTAINER:
			value_.continueAlterContainer_->decode(in);
			break;
		case DS_PUT_ROW:
			value_.putRow_->decode(in);
			break;
		case DS_UPDATE_ROW_BY_ID:
			value_.updateRowById_->decode(in);
			break;
		case DS_REMOVE_ROW:
			value_.removeRow_->decode(in);
			break;
		case DS_REMOVE_ROW_BY_ID:
			value_.removeRowById_->decode(in);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			break;
		}
	}

	union UValue {
		Commit* commit_;
		Abort* abort_;
		PutContainer* putContainer_;
		UpdateTablePartitioningId* updateTablePartitioningId_;
		DropContainer* dropContainer_;
		GetContainer* getContainer_;
		CreateIndex* createIndex_;
		DropIndex* dropIndex_;
		ContinueCreateIndex* continueCreateIndex_;
		ContinueAlterContainer* continueAlterContainer_;
		PutRow* putRow_;
		AppendRow* appendRow_;
		UpdateRowById* updateRowById_;
		RemoveRow* removeRow_;
		RemoveRowById* removeRowById_;
		GetRow* getRow_;
		GetRowSet* getRowSet_;
		QueryTQL* queryTQL_;
		QueryGeometryWithExclusion* queryGeometryWithExclusion_;
		QueryGeometryRelated* queryGeometryRelated_;
		GetRowTimeRelated* getRowTimeRelated_;
		QueryAggregate* queryAggregate_;
		QueryTimeRange* queryTimeRange_;
		QueryInterPolate* queryInterPolate_;
		QueryTimeSampling* queryTimeSampling_;
		QueryFetchResultSet* queryFetchResultSet_;
		QueryCloseResultSet* queryCloseResultSet_;
		GetContainerObject* getContainerObject_;
		ScanChunkGroup* scanChunkGroup_;
		SearchBackgroundTask* searchBackgroundTask_;
		ExecuteBackgroundTask* executeBackgroundTask_;
		CheckTimeoutResultSet* checkTimeoutResultSet_;
	};

	DSOperationType type_;
	UValue value_;
};

struct ResponsMesV4 : public Serializable {
	ResponsMesV4(util::StackAllocator& alloc) : Serializable(&alloc) {}
	virtual ~ResponsMesV4();
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}
	template<typename S>
	void encode(S& out) {
	}
	template<typename S>
	void decode(S& in) {
	}
};

struct SchemaMessage : public ResponsMesV4 {
	SchemaMessage(util::StackAllocator& alloc)
		: ResponsMesV4(alloc), schemaVersionId_(UNDEF_SCHEMAVERSIONID),
		containerId_(UNDEF_CONTAINERID),
		containerKeyBin_(alloc),
		containerSchemaBin_(alloc),
		containerAttribute_(CONTAINER_ATTR_ANY) {}
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}
	template<typename S>
	void encode(S& out) {
		out << schemaVersionId_;
		out << containerId_;
		DataStoreUtil::encodeVarSizeBinaryData<S>(out, containerKeyBin_.data(),
			containerKeyBin_.size());
		DataStoreUtil::encodeBinaryData<S>(out, containerSchemaBin_.data(),
			containerSchemaBin_.size());
		{
			int32_t containerAttribute =
				static_cast<int32_t>(containerAttribute_);
			out << containerAttribute;
		}
	}
	template<typename S>
	void decode(EventByteInStream& in) {
	}

	SchemaVersionId schemaVersionId_;
	ContainerId containerId_;
	util::XArray<uint8_t> containerKeyBin_;
	util::XArray<uint8_t> containerSchemaBin_;
	ContainerAttribute containerAttribute_;
};

struct WithExistMessage : public ResponsMesV4 {
	WithExistMessage(util::StackAllocator& alloc, bool existFlag, Serializable* mes) :
		ResponsMesV4(alloc), existFlag_(existFlag), mes_(mes){}
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}
	template<typename S>
	void encode(S& out) {
		DataStoreUtil::encodeBooleanData<S>(out, existFlag_);
		if (mes_ != NULL) {
			mes_->encode(out);
		}
	}
	template<typename S>
	void decode(S& in) {
	}
	bool existFlag_;
	Serializable *mes_;
};

struct RowExistMessage : public ResponsMesV4 {
	RowExistMessage(util::StackAllocator& alloc, bool exist) : ResponsMesV4(alloc), existFlag_(exist) {}
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}
	template<typename S>
	void encode(S& out) {
		DataStoreUtil::encodeBooleanData<S>(out, existFlag_);
	}
	template<typename S>
	void decode(S& in) {
	}
	bool existFlag_;
};

struct SimpleQueryMessage : public ResponsMesV4 {
	SimpleQueryMessage(util::StackAllocator& alloc, ResultSetGuard* rsGuard, ResultSet* rs, bool existFlag)
		: ResponsMesV4(alloc), rsGuard_(rsGuard), rs_(rs), existFlag_(existFlag) {}
	virtual ~SimpleQueryMessage();
	void encode(EventByteOutStream& out);
	void decode(EventByteInStream& in);
	void encode(OutStream& out);
	template<typename S>
	void encode(S& out);
	template<typename S>
	void decode(S& in) {
	}
	ResultSetGuard* rsGuard_;
	ResultSet* rs_;
	bool existFlag_;
};

struct DSBaseOutputMes : public Serializable {
	DSBaseOutputMes(util::StackAllocator& alloc, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		Serializable(&alloc), mes_(mes), dsLog_(dsLog) {}
	~DSBaseOutputMes() {
		ALLOC_DELETE(*alloc_, mes_);
		ALLOC_DELETE(*alloc_, dsLog_);
	}
	void encode(EventByteOutStream& out) {
		encode<EventByteOutStream>(out);
	}
	void decode(EventByteInStream& in) {
		decode<EventByteInStream>(in);
	}
	void encode(OutStream& out) {
		encode<OutStream>(out);
	}
	template<typename S>
	void encode(S& out) {
		if (mes_ != NULL) {
			mes_->encode(out);
		}
	}
	template<typename S>
	void decode(S& in) {
		if (mes_ != NULL) {
			mes_->decode(in);
		}
	}
	ResponsMesV4* mes_;
	DataStoreLogV4* dsLog_;
};

struct DSOutputMes : public DSBaseOutputMes {
	DSOutputMes(util::StackAllocator& alloc, PutStatus status, uint64_t num, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		DSBaseOutputMes(alloc, mes, dsLog), status_(status), isExecuted_(status != PutStatus::NOT_EXECUTED), num_(num) {}
	DSOutputMes(util::StackAllocator& alloc, bool isExecuted, uint64_t num, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		DSBaseOutputMes(alloc, mes, dsLog), status_(PutStatus::UPDATE), isExecuted_(isExecuted), num_(num) {
		if (isExecuted_) {
			status_ = PutStatus::NOT_EXECUTED;
		}
	}
	PutStatus status_; 
	bool isExecuted_;
	uint64_t num_;
};

struct DSPutContainerOutputMes : public DSBaseOutputMes {
	DSPutContainerOutputMes(util::StackAllocator& alloc, PutStatus status, bool isExecuted,
		KeyDataStoreValue storeValue, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		DSBaseOutputMes(alloc, mes, dsLog), status_(status), isExecuted_(isExecuted),
		storeValue_(storeValue) {}
	PutStatus status_; 
	bool isExecuted_;
	KeyDataStoreValue storeValue_;
};

struct DSRowSetOutputMes : public DSBaseOutputMes {
	DSRowSetOutputMes(util::StackAllocator& alloc, ResultSetGuard* rsGuard, ResultSet* rs, RowId last, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		DSBaseOutputMes(alloc, mes, dsLog), rsGuard_(rsGuard), rs_(rs), last_(last) {}
	~DSRowSetOutputMes();
	ResultSetGuard* rsGuard_;
	ResultSet* rs_;
	RowId last_;
};

struct DSTQLOutputMes : public DSBaseOutputMes {
	DSTQLOutputMes(util::StackAllocator& alloc, ResultSetGuard* rsGuard, ResultSet* rs, ResponsMesV4* mes, DataStoreLogV4* dsLog) :
		DSBaseOutputMes(alloc, mes, dsLog), rsGuard_(rsGuard), rs_(rs) {}
	~DSTQLOutputMes();
	ResultSetGuard* rsGuard_;
	ResultSet* rs_;
};

struct DSContainerOutputMes : public DSBaseOutputMes {
	DSContainerOutputMes(util::StackAllocator& alloc, BaseContainer* container) :
		DSBaseOutputMes(alloc, NULL, NULL), container_(container) {}
	~DSContainerOutputMes();
	BaseContainer* releaseContainerPtr() {
		BaseContainer *ptr = container_;
		container_ = NULL;
		return ptr;
	}
	BaseContainer* container_;
};

struct DSScanChunkGroupOutputMes : public DSBaseOutputMes {
	DSScanChunkGroupOutputMes(util::StackAllocator& alloc, uint64_t num) :
		DSBaseOutputMes(alloc, NULL, NULL), num_(num) {}
	uint64_t num_;
};

struct DSBackgroundTaskOutputMes : public DSBaseOutputMes {
	DSBackgroundTaskOutputMes(util::StackAllocator& alloc, BGTask bgTask) :
		DSBaseOutputMes(alloc, NULL, NULL), bgTask_(bgTask) {}
	BGTask bgTask_;
};

#endif
