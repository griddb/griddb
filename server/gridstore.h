/*------------------------------------------------------------------*/
// Copyright (c) 2017 TOSHIBA Digital Solutions Corporation. All Rights Reserved.
/*------------------------------------------------------------------*/


#ifndef GRIDSTORE_H_
#define GRIDSTORE_H_



#include <stdlib.h>		

#if defined(_MSC_VER) && (_MSC_VER <= 1500)
#include "gsext/stdint.h"
#else
#include <stdint.h>
#endif



#ifndef GS_CLIENT_VERSION_MAJOR

#define GS_CLIENT_VERSION_MAJOR 5
#endif

#ifndef GS_CLIENT_VERSION_MINOR

#define GS_CLIENT_VERSION_MINOR 3
#endif


#ifdef __cplusplus
extern "C" {
#endif

#ifndef GS_INTERNAL_DEFINITION_VISIBLE
#define GS_INTERNAL_DEFINITION_VISIBLE 1
#endif

#if GS_INTERNAL_DEFINITION_VISIBLE

#if defined(GS_DLL_INSIDE) && defined(__GNUC__) && !defined(_WIN32)
#define GS_DLL_PUBLIC __attribute__ ((visibility("default")))
#else
#define GS_DLL_PUBLIC
#endif

#ifdef _WIN32
#define GS_API_CALL __stdcall
#else
#define GS_API_CALL
#endif

#if defined(__GNUC__) && !defined(GS_WARN_UNUSED)
#define GS_STATIC_HEADER_FUNC_ATTR __attribute__((unused))
#else
#define GS_STATIC_HEADER_FUNC_ATTR
#endif

#define GS_STATIC_HEADER_FUNC_SPECIFIER GS_STATIC_HEADER_FUNC_ATTR static

#ifndef GS_DEPRECATION_IGNORABLE
#ifdef GS_DLL_INSIDE
#define GS_DEPRECATION_IGNORABLE 1
#else
#define GS_DEPRECATION_IGNORABLE 0
#endif
#endif

#if !GS_DEPRECATION_IGNORABLE && defined(__GNUC__)
#define GS_DEPRECATED_SYMBOL1(symbol) symbol __attribute__((deprecated))
#elif !GS_DEPRECATION_IGNORABLE && defined(_MSC_VER)
#define GS_DEPRECATED_SYMBOL1(symbol) __declspec(deprecated) symbol
#else
#define GS_DEPRECATED_SYMBOL1(symbol) symbol
#endif

#define GS_DEPRECATED_FUNC(func) GS_DEPRECATED_SYMBOL1(func)
#define GS_DEPRECATED_VAR(func) GS_DEPRECATED_SYMBOL1(func)

#ifndef GS_DEPRECATED_FUNC_ENABLED
#define GS_DEPRECATED_FUNC_ENABLED 1
#endif

#ifndef GS_EXPERIMENTAL_TOOL_ENABLED
#define GS_EXPERIMENTAL_TOOL_ENABLED 0
#endif

#if GS_DEPRECATED_FUNC_ENABLED


#ifndef GS_COMPATIBILITY_FACTORY_BETA_0_3
#define GS_COMPATIBILITY_FACTORY_BETA_0_3 0
#endif



#ifndef GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10
#define GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10 0
#endif


#ifndef GS_COMPATIBILITY_TIME_SERIES_SAMPLING_BETA_0_1
#define GS_COMPATIBILITY_TIME_SERIES_SAMPLING_BETA_0_1 0
#endif


#ifndef GS_COMPATIBILITY_GET_MULTIPLE_ROWS_BETA_0_3
#define GS_COMPATIBILITY_GET_MULTIPLE_ROWS_BETA_0_3 0
#endif

#ifndef GS_COMPATIBILITY_VALUE_1_1_106
#define GS_COMPATIBILITY_VALUE_1_1_106 0
#endif

#ifndef GS_COMPATIBILITY_MULTIPLE_CONTAINERS_1_1_106
#define GS_COMPATIBILITY_MULTIPLE_CONTAINERS_1_1_106 0
#endif

#ifndef GS_COMPATIBILITY_DEPRECATE_FETCH_OPTION_SIZE
#define GS_COMPATIBILITY_DEPRECATE_FETCH_OPTION_SIZE 1
#endif

#endif 

#if !defined(GS_COMPATIBILITY_SUPPORT_1_5) && \
	(GS_CLIENT_VERSION_MAJOR > 1 || \
	(GS_CLIENT_VERSION_MAJOR == 1 && GS_CLIENT_VERSION_MINOR >= 5))
#define GS_COMPATIBILITY_SUPPORT_1_5 1
#else
#define GS_COMPATIBILITY_SUPPORT_1_5 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_2_0) && \
	(GS_CLIENT_VERSION_MAJOR > 2 || \
	(GS_CLIENT_VERSION_MAJOR == 2 && GS_CLIENT_VERSION_MINOR >= 0))
#define GS_COMPATIBILITY_SUPPORT_2_0 1
#else
#define GS_COMPATIBILITY_SUPPORT_2_0 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_2_1) && \
	(GS_CLIENT_VERSION_MAJOR > 2 || \
	(GS_CLIENT_VERSION_MAJOR == 2 && GS_CLIENT_VERSION_MINOR >= 1))
#define GS_COMPATIBILITY_SUPPORT_2_1 1
#else
#define GS_COMPATIBILITY_SUPPORT_2_1 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_3_5) && \
	(GS_CLIENT_VERSION_MAJOR > 3 || \
	(GS_CLIENT_VERSION_MAJOR == 3 && GS_CLIENT_VERSION_MINOR >= 5))
#define GS_COMPATIBILITY_SUPPORT_3_5 1
#else
#define GS_COMPATIBILITY_SUPPORT_3_5 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_4_0) && \
	(GS_CLIENT_VERSION_MAJOR > 4 || \
	(GS_CLIENT_VERSION_MAJOR == 4 && GS_CLIENT_VERSION_MINOR >= 0))
#define GS_COMPATIBILITY_SUPPORT_4_0 1
#else
#define GS_COMPATIBILITY_SUPPORT_4_0 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_4_1) && \
	(GS_CLIENT_VERSION_MAJOR > 4 || \
	(GS_CLIENT_VERSION_MAJOR == 4 && GS_CLIENT_VERSION_MINOR >= 1))
#define GS_COMPATIBILITY_SUPPORT_4_1 1
#else
#define GS_COMPATIBILITY_SUPPORT_4_1 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_4_2) && \
	(GS_CLIENT_VERSION_MAJOR > 4 || \
	(GS_CLIENT_VERSION_MAJOR == 4 && GS_CLIENT_VERSION_MINOR >= 2))
#define GS_COMPATIBILITY_SUPPORT_4_2 1
#else
#define GS_COMPATIBILITY_SUPPORT_4_2 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_4_3) && \
	(GS_CLIENT_VERSION_MAJOR > 4 || \
	(GS_CLIENT_VERSION_MAJOR == 4 && GS_CLIENT_VERSION_MINOR >= 3))
#define GS_COMPATIBILITY_SUPPORT_4_3 1
#else
#define GS_COMPATIBILITY_SUPPORT_4_3 0
#endif

#if !defined(GS_COMPATIBILITY_SUPPORT_5_3) && \
	(GS_CLIENT_VERSION_MAJOR > 5 || \
	(GS_CLIENT_VERSION_MAJOR == 5 && GS_CLIENT_VERSION_MINOR >= 3))
#define GS_COMPATIBILITY_SUPPORT_5_3 1
#else
#define GS_COMPATIBILITY_SUPPORT_5_3 0
#endif

#endif 


typedef char GSChar;


typedef char GSBool;


#define GS_TRUE 1


#define GS_FALSE 0


typedef int32_t GSEnum;



typedef int64_t GSTimestamp;



typedef struct GSGridStoreFactoryTag GSGridStoreFactory;



typedef struct GSGridStoreTag GSGridStore;



typedef struct GSContainerTag GSContainer;



typedef struct GSQueryTag GSQuery;



typedef struct GSRowSetTag GSRowSet;



typedef struct GSAggregationResultTag GSAggregationResult;



typedef GSContainer GSCollection;



typedef GSContainer GSTimeSeries;

#if GS_COMPATIBILITY_SUPPORT_1_5



typedef struct GSRowTag GSRow;

#if GS_COMPATIBILITY_SUPPORT_4_3


typedef GSRow GSRowKey;

#endif 



typedef struct GSRowKeyPredicateTag GSRowKeyPredicate;



typedef struct GSPartitionControllerTag GSPartitionController;

#endif 


typedef int32_t GSResult;


#define GS_RESULT_OK 0


#define GS_SUCCEEDED(result) ((result) == GS_RESULT_OK)

#if GS_COMPATIBILITY_SUPPORT_5_3

typedef struct GSPreciseTimestampTag {

	
	GSTimestamp base;

	
	uint32_t nanos;

} GSPreciseTimestamp;


#define GS_PRECISE_TIMESTAMP_INITIALIZER \
		{ 0, 0 }

#endif 


typedef struct GSBlobTag {

	
	size_t size;

	
	const void *data;

} GSBlob;


typedef struct GSPropertyEntryTag {

	
	const GSChar *name;

	
	const GSChar *value;

} GSPropertyEntry;


enum GSFetchOptionTag {

	
	GS_FETCH_LIMIT,

#if GS_COMPATIBILITY_SUPPORT_1_5

#if GS_INTERNAL_DEFINITION_VISIBLE
#if !GS_COMPATIBILITY_DEPRECATE_FETCH_OPTION_SIZE
	
	GS_FETCH_SIZE = (GS_FETCH_LIMIT + 1),
#endif
#endif

#if GS_COMPATIBILITY_SUPPORT_4_0
	
	GS_FETCH_PARTIAL_EXECUTION = (GS_FETCH_LIMIT + 2)
#endif 

#endif 
};

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_DEPRECATE_FETCH_OPTION_SIZE
#if GS_DEPRECATED_FUNC_ENABLED
static const enum GSFetchOptionTag GS_DEPRECATED_VAR(GS_FETCH_SIZE) =
		(enum GSFetchOptionTag) (GS_FETCH_LIMIT + 1);
#endif
#endif
#endif


typedef GSEnum GSFetchOption;


enum GSQueryOrderTag {

	
	GS_ORDER_ASCENDING,

	
	GS_ORDER_DESCENDING

};


typedef GSEnum GSQueryOrder;


enum GSIndexTypeFlagTag {

	
	GS_INDEX_FLAG_DEFAULT = -1,

	
	GS_INDEX_FLAG_TREE = 1 << 0,

	
	GS_INDEX_FLAG_HASH = 1 << 1,

	
	GS_INDEX_FLAG_SPATIAL = 1 << 2

};


typedef int32_t GSIndexTypeFlags;


enum GSAggregationTag {

	
	GS_AGGREGATION_MINIMUM,

	
	GS_AGGREGATION_MAXIMUM,

	
	GS_AGGREGATION_TOTAL,

	
	GS_AGGREGATION_AVERAGE,

	
	GS_AGGREGATION_VARIANCE,

	
	GS_AGGREGATION_STANDARD_DEVIATION,

	
	GS_AGGREGATION_COUNT,

	
	GS_AGGREGATION_WEIGHTED_AVERAGE,

};


typedef GSEnum GSAggregation;


enum GSInterpolationModeTag {

	
	GS_INTERPOLATION_LINEAR_OR_PREVIOUS,

	
	GS_INTERPOLATION_EMPTY

};


typedef GSEnum GSInterpolationMode;


enum GSTimeOperatorTag {

	
	GS_TIME_OPERATOR_PREVIOUS,

	
	GS_TIME_OPERATOR_PREVIOUS_ONLY,

	
	GS_TIME_OPERATOR_NEXT,

	
	GS_TIME_OPERATOR_NEXT_ONLY

};


typedef GSEnum GSTimeOperator;


enum GSGeometryOperatorTag {

	
	GS_GEOMETRY_OPERATOR_INTERSECT

};


typedef GSEnum GSGeometryOperator;


enum GSCompressionMethodTag {
#if GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10
	GS_COMPRESSION_NONE,
	GS_COMPRESSION_LOSSLESS,
	GS_COMPRESSION_LOSSY
#else

	
	GS_COMPRESSION_NO,

	
	GS_COMPRESSION_SS,

	
	GS_COMPRESSION_HI

#endif
};


typedef GSEnum GSCompressionMethod;


enum GSTimeUnitTag {
	
	GS_TIME_UNIT_YEAR,

	
	GS_TIME_UNIT_MONTH,

	
	GS_TIME_UNIT_DAY,

	
	GS_TIME_UNIT_HOUR,

	
	GS_TIME_UNIT_MINUTE,

	
	GS_TIME_UNIT_SECOND,

	
	GS_TIME_UNIT_MILLISECOND
};


typedef GSEnum GSTimeUnit;


enum GSContainerTypeTag {

	
	GS_CONTAINER_COLLECTION,

	
	GS_CONTAINER_TIME_SERIES,

};


typedef GSEnum GSContainerType;


enum GSTypeTag {
	
	GS_TYPE_STRING,

	
	GS_TYPE_BOOL,

	
	GS_TYPE_BYTE,

	
	GS_TYPE_SHORT,

	
	GS_TYPE_INTEGER,

	
	GS_TYPE_LONG,

	
	GS_TYPE_FLOAT,

	
	GS_TYPE_DOUBLE,

	
	GS_TYPE_TIMESTAMP,

	
	GS_TYPE_GEOMETRY,

	
	GS_TYPE_BLOB,

	
	GS_TYPE_STRING_ARRAY,

	
	GS_TYPE_BOOL_ARRAY,

	
	GS_TYPE_BYTE_ARRAY,

	
	GS_TYPE_SHORT_ARRAY,

	
	GS_TYPE_INTEGER_ARRAY,

	
	GS_TYPE_LONG_ARRAY,

	
	GS_TYPE_FLOAT_ARRAY,

	
	GS_TYPE_DOUBLE_ARRAY,

	
	GS_TYPE_TIMESTAMP_ARRAY,

#if GS_COMPATIBILITY_SUPPORT_3_5
	
	GS_TYPE_NULL = -1,

#if GS_COMPATIBILITY_SUPPORT_5_3
	
	GS_TYPE_PRECISE_TIMESTAMP = -2
#endif 

#endif
};


typedef GSEnum GSType;


enum GSTypeOptionTag {

#if GS_INTERNAL_DEFINITION_VISIBLE
	GS_TYPE_OPTION_KEY = 1 << 0,
#endif

#if GS_COMPATIBILITY_SUPPORT_3_5

	
	GS_TYPE_OPTION_NULLABLE = 1 << 1,

	
	GS_TYPE_OPTION_NOT_NULL = 1 << 2,

#if GS_COMPATIBILITY_SUPPORT_4_1
	
	GS_TYPE_OPTION_DEFAULT_VALUE_NULL = 1 << 3,

	
	GS_TYPE_OPTION_DEFAULT_VALUE_NOT_NULL = 1 << 4,

#if GS_COMPATIBILITY_SUPPORT_5_3
	
	GS_TYPE_OPTION_TIME_MILLI = 1 << 5,

	
	GS_TYPE_OPTION_TIME_MICRO = 1 << 6,

	
	GS_TYPE_OPTION_TIME_NANO = 1 << 7,
#endif 

#endif 

#endif 

};


typedef int32_t GSTypeOption;


enum GSRowSetTypeTag {
	
	GS_ROW_SET_CONTAINER_ROWS,

	
	GS_ROW_SET_AGGREGATION_RESULT,

	
	GS_ROW_SET_QUERY_ANALYSIS
};


typedef GSEnum GSRowSetType;


typedef struct GSColumnCompressionTag {

	
	const GSChar *columnName;

#if GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10
	GSCompressionMethod method;
	double threshold;
	GSBool thresholdRelative;
#else

	
	GSBool relative;

	
	double rate;

	
	double span;

	
	double width;

#endif
} GSColumnCompression;

#if GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10
#define GS_COLUMN_COMPRESSION_INITIALIZER \
	{ NULL, GS_COMPRESSION_NONE, 0, GS_FALSE }
#else

#define GS_COLUMN_COMPRESSION_INITIALIZER \
	{ NULL, GS_FALSE, 0, 0, 0 }
#endif


typedef struct GSCollectionPropertiesTag {
#if GS_INTERNAL_DEFINITION_VISIBLE
	struct {
		int8_t unused;
	} internal;
#endif
} GSCollectionProperties;


#define GS_COLLECTION_PROPERTIES_INITIALIZER \
	{ 0 }


typedef struct GSTimeSeriesPropertiesTag {

	
	int32_t rowExpirationTime;

	
	GSTimeUnit rowExpirationTimeUnit;

	
	int32_t compressionWindowSize;

	
	GSTimeUnit compressionWindowSizeUnit;

#if !(GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10)

	
	GSCompressionMethod compressionMethod;

#endif

	
	size_t compressionListSize;

	
	GSColumnCompression *compressionList;

#if GS_COMPATIBILITY_SUPPORT_2_0

	
	int32_t expirationDivisionCount;

#endif 

} GSTimeSeriesProperties;

#if GS_COMPATIBILITY_SUPPORT_2_0

#define GS_TIME_SERIES_PROPERTIES_INITIALIZER \
	{ -1, GS_TIME_UNIT_DAY, -1, GS_TIME_UNIT_DAY, \
	GS_COMPRESSION_NO, 0, NULL, -1 }
#elif !GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10
#define GS_TIME_SERIES_PROPERTIES_INITIALIZER \
	{ -1, GS_TIME_UNIT_DAY, -1, GS_TIME_UNIT_DAY, \
	GS_COMPRESSION_NO, 0, NULL }
#else
#define GS_TIME_SERIES_PROPERTIES_INITIALIZER \
	{ -1, GS_TIME_UNIT_DAY, -1, GS_TIME_UNIT_DAY, 0, NULL }
#endif


typedef struct GSColumnInfoTag {

	
	const GSChar *name;

	
	GSType type;

#if GS_COMPATIBILITY_SUPPORT_1_5

	
	GSIndexTypeFlags indexTypeFlags;

#if GS_COMPATIBILITY_SUPPORT_3_5

	
	GSTypeOption options;
#endif 

#endif 

} GSColumnInfo;

#if GS_COMPATIBILITY_SUPPORT_3_5


#define GS_COLUMN_INFO_INITIALIZER \
	{ NULL, GS_TYPE_STRING, GS_INDEX_FLAG_DEFAULT, 0 }

#elif GS_COMPATIBILITY_SUPPORT_1_5

#define GS_COLUMN_INFO_INITIALIZER \
	{ NULL, GS_TYPE_STRING, GS_INDEX_FLAG_DEFAULT }

#else

#define GS_COLUMN_INFO_INITIALIZER \
	{ NULL, GS_TYPE_STRING }

#endif 

#if GS_COMPATIBILITY_SUPPORT_1_5


enum GSTriggerTypeTag {
	
	GS_TRIGGER_REST,

	
	GS_TRIGGER_JMS

};


typedef GSEnum GSTriggerType;


enum GSTriggerEventTypeFlagTag {
	
	GS_TRIGGER_EVENT_PUT = 1 << 0,

	
	GS_TRIGGER_EVENT_DELETE = 1 << 1

};


typedef int32_t GSTriggerEventTypeFlags;


typedef struct GSTriggerInfoTag {

	
	const GSChar *name;

	
	GSTriggerType type;

	
	const GSChar *uri;

	
	GSTriggerEventTypeFlags eventTypeFlags;

	
	const GSChar *const *columnSet;

	
	size_t columnCount;

	
	const GSChar *jmsDestinationType;

	
	const GSChar *jmsDestinationName;

	
	const GSChar *user;

	
	const GSChar *password;

} GSTriggerInfo;


#define GS_TRIGGER_INFO_INITIALIZER \
	{ NULL, GS_TRIGGER_REST, NULL, 0, NULL, 0, NULL, NULL, NULL }

#endif 

#if GS_COMPATIBILITY_SUPPORT_3_5


typedef struct GSIndexInfoTag {

	
	const GSChar *name;

	
	GSIndexTypeFlags type;

	
	int32_t column;

	
	const GSChar *columnName;

#if GS_COMPATIBILITY_SUPPORT_4_3

	
	size_t columnCount;

	
	const int32_t *columnList;

	
	size_t columnNameCount;

	
	const GSChar *const *columnNameList;

#endif 

} GSIndexInfo;

#if GS_COMPATIBILITY_SUPPORT_4_3


#define GS_INDEX_INFO_INITIALIZER \
	{ NULL, GS_INDEX_FLAG_DEFAULT, -1, NULL, 0, NULL, 0, NULL }

#else

#define GS_INDEX_INFO_INITIALIZER \
	{ NULL, GS_INDEX_FLAG_DEFAULT, -1, NULL }

#endif 

#endif 


typedef struct GSContainerInfoTag {

	
	const GSChar *name;

	
	GSContainerType type;

	
	size_t columnCount;

	
	const GSColumnInfo *columnInfoList;

	
	GSBool rowKeyAssigned;

#if GS_COMPATIBILITY_SUPPORT_1_5

	
	GSBool columnOrderIgnorable;

	
	const GSTimeSeriesProperties *timeSeriesProperties;

	
	size_t triggerInfoCount;

	
	const GSTriggerInfo *triggerInfoList;

#if GS_COMPATIBILITY_SUPPORT_2_1
	
	const GSChar *dataAffinity;

#if GS_COMPATIBILITY_SUPPORT_3_5

	
	size_t indexInfoCount;

	
	const GSIndexInfo *indexInfoList;

#if GS_COMPATIBILITY_SUPPORT_4_3

	
	size_t rowKeyColumnCount;

	
	const int32_t *rowKeyColumnList;

#endif 

#endif 

#endif 

#endif 

} GSContainerInfo;

#if GS_COMPATIBILITY_SUPPORT_4_3


#define GS_CONTAINER_INFO_INITIALIZER \
	{ NULL, GS_CONTAINER_COLLECTION, 0, NULL, GS_FALSE, \
	GS_FALSE, NULL, 0, NULL, NULL, 0, NULL, 0, NULL }

#elif GS_COMPATIBILITY_SUPPORT_3_5

#define GS_CONTAINER_INFO_INITIALIZER \
	{ NULL, GS_CONTAINER_COLLECTION, 0, NULL, GS_FALSE, \
	GS_FALSE, NULL, 0, NULL, NULL, 0, NULL }

#elif GS_COMPATIBILITY_SUPPORT_2_1

#define GS_CONTAINER_INFO_INITIALIZER \
	{ NULL, GS_CONTAINER_COLLECTION, 0, NULL, GS_FALSE, \
	GS_FALSE, NULL, 0, NULL, NULL }

#elif GS_COMPATIBILITY_SUPPORT_1_5

#define GS_CONTAINER_INFO_INITIALIZER \
	{ NULL, GS_CONTAINER_COLLECTION, 0, NULL, GS_FALSE, \
	GS_FALSE, NULL, 0, NULL }

#else

#define GS_CONTAINER_INFO_INITIALIZER \
	{ NULL, GS_CONTAINER_COLLECTION, 0, NULL, GS_FALSE }

#endif 

#if GS_INTERNAL_DEFINITION_VISIBLE
struct GSBindingTag;
typedef const struct GSBindingTag* (*GSBindingGetterFunc)();
typedef struct GSBindingEntryTag {
	const GSChar *columnName;
	GSType elementType;
	size_t offset;
	size_t arraySizeOffset;
	GSTypeOption options;		
#if GS_COMPATIBILITY_SUPPORT_4_3
	const struct GSBindingTag *keyBinding;
	GSBindingGetterFunc keyBindingGetter;
#endif
} GSBindingEntry;
#endif


typedef struct GSBindingTag {
#if GS_INTERNAL_DEFINITION_VISIBLE
	GSBindingEntry *entries;
	size_t entryCount;
#endif
} GSBinding;


typedef struct GSQueryAnalysisEntryTag {

	
	int32_t id;

	
	int32_t depth;

	
	const GSChar *type;

	
	const GSChar *valueType;

	
	const GSChar *value;

	
	const GSChar *statement;

} GSQueryAnalysisEntry;


#define GS_QUERY_ANALYSIS_ENTRY_INITIALIZER \
	{ 0, 0, NULL, NULL, NULL, NULL }

#if GS_COMPATIBILITY_SUPPORT_1_5


typedef struct GSContainerRowEntryTag {

	
	const GSChar *containerName;

	
	void *const *rowList;

#if GS_COMPATIBILITY_MULTIPLE_CONTAINERS_1_1_106
	size_t rowListSize;
#else
	
	size_t rowCount;
#endif

} GSContainerRowEntry;


#define GS_CONTAINER_ROW_ENTRY_INITIALIZER \
	{ NULL, NULL, 0 }


typedef struct GSRowKeyPredicateEntryTag {

	
	const GSChar *containerName;

	
	GSRowKeyPredicate *predicate;

} GSRowKeyPredicateEntry;


#define GS_ROW_KEY_PREDICATE_ENTRY_INITIALIZER \
	{ NULL, NULL }


typedef union GSValueTag {

	
	const GSChar *asString;

	
	GSBool asBool;

	
	int8_t asByte;

	
	int16_t asShort;

	
	int32_t asInteger;

	
	int64_t asLong;

	
	float asFloat;

	
	double asDouble;

	
	GSTimestamp asTimestamp;

#if GS_COMPATIBILITY_SUPPORT_5_3
	
	GSPreciseTimestamp asPreciseTimestamp;
#endif 

	
	const GSChar *asGeometry;

	
	GSBlob asBlob;

#if GS_COMPATIBILITY_VALUE_1_1_106

	struct {
		size_t size;
		const GSChar *const *elements;
	} asStringArray;

	struct {
		size_t size;
		const GSBool *elements;
	} asBoolArray;

	struct {
		size_t size;
		const int8_t *elements;
	} asByteArray;

	struct {
		size_t size;
		const int16_t *elements;
	} asShortArray;

	struct {
		size_t size;
		const int32_t *elements;
	} asIntegerArray;

	struct {
		size_t size;
		const int64_t *elements;
	} asLongArray;

	struct {
		size_t size;
		const float *elements;
	} asFloatArray;

	struct {
		size_t size;
		const double *elements;
	} asDoubleArray;

	struct {
		size_t size;
		const GSTimestamp *elements;
	} asTimestampArray;

#else

	struct {

		
		size_t length;

		union {

			
			const void *data;

			
			const GSChar *const *asString;

			
			const GSBool *asBool;

			
			const int8_t *asByte;

			
			const int16_t *asShort;

			
			const int32_t *asInteger;

			
			const int64_t *asLong;

			
			const float *asFloat;

			
			const double *asDouble;

			
			const GSTimestamp *asTimestamp;

		}
		
		elements;

	}
	
	asArray;

#endif

} GSValue;

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


typedef struct GSTimeZoneTag {
#if GS_INTERNAL_DEFINITION_VISIBLE
	struct {
		int64_t offsetMillis;
	} internalData;
#endif 
} GSTimeZone;


#define GS_TIME_ZONE_INITIALIZER \
	{ { 0 } }


#define GS_TIMESTAMP_DEFAULT 0

#endif 

#if GS_COMPATIBILITY_SUPPORT_5_3

typedef struct GSTimestampFormatOptionTag {
	
	const GSTimeZone *timeZone;
} GSTimestampFormatOption;


#define GS_TIMESAMP_FORMAT_OPTION_INITIALIZER \
	{ NULL }

#endif 

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_SUPPORT_5_3
#define GS_INTERNAL_TIME_STRING_SIZE_MAX 37
#else
#define GS_INTERNAL_TIME_STRING_SIZE_MAX 32
#endif 
#endif 


#define GS_TIME_STRING_SIZE_MAX GS_INTERNAL_TIME_STRING_SIZE_MAX

#if GS_COMPATIBILITY_SUPPORT_4_3


#define GS_TIME_ZONE_STRING_SIZE_MAX 8

#endif 






GS_DLL_PUBLIC void GS_API_CALL gsCloseFactory(
		GSGridStoreFactory **factory, GSBool allRelated);


GS_DLL_PUBLIC GSGridStoreFactory* GS_API_CALL gsGetDefaultFactory();


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetGridStore(
		GSGridStoreFactory *factory,
		const GSPropertyEntry *properties, size_t propertyCount,
		GSGridStore **store);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsCompatibleFunc_GetGridStore1(
		GSGridStoreFactory *factory, const GSPropertyEntry *properties,
		GSGridStore **store));
#endif

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_FACTORY_BETA_0_3
#define gsGetGridStore(factory, properties, store) \
		gsCompatibleFunc_GetGridStore1(factory, properties, store)
#else
#define gsGetGridStore(factory, properties, propertyCount, store) \
		gsGetGridStore(factory, properties, propertyCount, store)
#endif
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetFactoryProperties(
		GSGridStoreFactory *factory,
		const GSPropertyEntry *properties, size_t propertyCount);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsCompatibleFunc_SetFactoryProperties1(
		GSGridStoreFactory *factory, const GSPropertyEntry *properties));
#endif

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_FACTORY_BETA_0_3
#define gsSetFactoryProperties(factory, propertyCount) \
		gsCompatibleFunc_SetFactoryProperties1(factory, propertyCount)
#else
#define gsSetFactoryProperties(factory, properties, propertyCount) \
		gsSetFactoryProperties(factory, properties, propertyCount)
#endif
#endif 






GS_DLL_PUBLIC void GS_API_CALL gsCloseGridStore(
		GSGridStore **store, GSBool allRelated);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDropCollection(
		GSGridStore *store, const GSChar *name);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDropTimeSeries(
		GSGridStore *store, const GSChar *name);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(GSResult GS_API_CALL gsGetRowByPath(
		GSGridStore *store, const GSChar *pathKey, void *rowObj,
		GSBool *exists));
#endif

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsGetCollectionV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSCollection **collection);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetCollection(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSCollection **collection) {
	return gsGetCollectionV4_3(store, name, binding, collection);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetCollection(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSCollection **collection);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerInfoV4_3(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetContainerInfo(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists) {
	return gsGetContainerInfoV4_3(store, name, info, exists);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerInfoV3_3(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetContainerInfo(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists) {
	return gsGetContainerInfoV3_3(store, name, info, exists);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerInfoV2_1(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetContainerInfo(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists) {
	return gsGetContainerInfoV2_1(store, name, info, exists);
}

#elif GS_COMPATIBILITY_SUPPORT_1_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerInfoV1_5(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetContainerInfo(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists) {
	return gsGetContainerInfoV1_5(store, name, info, exists);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerInfo(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsGetTimeSeriesV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSTimeSeries **timeSeries);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetTimeSeries(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSTimeSeries **timeSeries) {
	return gsGetTimeSeriesV4_3(store, name, binding, timeSeries);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetTimeSeries(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSTimeSeries **timeSeries);

#endif 

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(GSResult GS_API_CALL gsPutRowByPath(
		GSGridStore *store, const GSChar *pathKey, const void *rowObj,
		GSBool *exists));
#endif

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutContainer(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	return gsPutContainerV4_3(
			store, name, binding, info, modifiable, container);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainer(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollectionV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSCollectionProperties *properties,
		GSBool modifiable, GSCollection **collection);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutCollection(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSCollectionProperties *properties,
		GSBool modifiable, GSCollection **collection) {
	return gsPutCollectionV4_3(store, name, binding, properties, modifiable, collection);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollection(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSCollectionProperties *properties,
		GSBool modifiable, GSCollection **collection);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeries(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesV4_3(
			store, name, binding, properties, modifiable, timeSeries);
}

#elif GS_COMPATIBILITY_SUPPORT_2_0

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesV2_0(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeries(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesV2_0(
			store, name, binding, properties, modifiable, timeSeries);
}

#elif !GS_COMPATIBILITY_TIME_SERIES_PROPERTIES_0_0_10

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeries(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries);

#else

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsCompatibleFunc_PutTimeSeries1(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries));
#endif

#define gsPutTimeSeries( \
				store, name, binding, properties, modifiable, timeSeries) \
		gsCompatibleFunc_PutTimeSeries1( \
				store, name, binding, properties, modifiable, timeSeries)

#endif

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRowByPath(
		GSGridStore *store, const GSChar *pathKey, GSBool *exists));
#endif

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsDeleteRowByPath(
		GSGridStore *store, const GSChar *pathKey, GSBool *exists));
#endif

#if GS_COMPATIBILITY_SUPPORT_1_5

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutContainerGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	return gsPutContainerGeneralV4_3(
			store, name, info, modifiable, container);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerGeneralV3_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutContainerGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	return gsPutContainerGeneralV3_3(
			store, name, info, modifiable, container);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerGeneralV2_1(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutContainerGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	return gsPutContainerGeneralV2_1(
			store, name, info, modifiable, container);
}

#elif GS_COMPATIBILITY_SUPPORT_2_0

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerGeneralV2_0(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutContainerGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	return gsPutContainerGeneralV2_0(
			store, name, info, modifiable, container);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutContainerGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerGeneral(
		GSGridStore *store, const GSChar *name, GSContainer **container);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollectionGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutCollectionGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSCollection **collection) {
	return gsPutCollectionGeneralV4_3(
			store, name, info, modifiable, collection);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollectionGeneralV3_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutCollectionGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSCollection **collection) {
	return gsPutCollectionGeneralV3_3(
			store, name, info, modifiable, collection);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollectionGeneralV2_1(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutCollectionGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSCollection **collection) {
	return gsPutCollectionGeneralV2_1(
			store, name, info, modifiable, collection);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutCollectionGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSCollection **collection);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetCollectionGeneral(
		GSGridStore *store, const GSChar *name, GSCollection **collection);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesGeneralV4_3(
			store, name, info, modifiable, timeSeries);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesGeneralV3_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesGeneralV3_3(
			store, name, info, modifiable, timeSeries);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesGeneralV2_1(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesGeneralV2_1(
			store, name, info, modifiable, timeSeries);
}

#elif GS_COMPATIBILITY_SUPPORT_2_0

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesGeneralV2_0(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsPutTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	return gsPutTimeSeriesGeneralV2_0(
			store, name, info, modifiable, timeSeries);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries);

#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetTimeSeriesGeneral(
		GSGridStore *store, const GSChar *name, GSTimeSeries **timeSeries);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDropContainer(
		GSGridStore *store, const GSChar *name);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowByStoreV4_3(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsCreateRowByStore(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row) {
	return gsCreateRowByStoreV4_3(store, info, row);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowByStoreV3_3(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsCreateRowByStore(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row) {
	return gsCreateRowByStoreV3_3(store, info, row);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowByStore(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowKeyByStore(
		GSGridStore *store, const GSContainerInfo *info, GSRowKey **key);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsFetchAll(
		GSGridStore *store, GSQuery *const *queryList, size_t queryCount);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutMultipleContainerRows(
		GSGridStore *store, const GSContainerRowEntry *entryList,
		size_t entryCount);

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_MULTIPLE_CONTAINERS_1_1_106
#define gsPutMultipleContainerRow gsPutMultipleContainerRows
#endif
#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetMultipleContainerRows(
		GSGridStore *store,
		const GSRowKeyPredicateEntry *const *predicateList,
		size_t predicateCount,
		const GSContainerRowEntry **entryList, size_t *entryCount);

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_MULTIPLE_CONTAINERS_1_1_106
#define gsGetMultipleContainerRow gsGetMultipleContainerRows
#endif
#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionController(
		GSGridStore *store, GSPartitionController **partitionController);


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowKeyPredicate(
		GSGridStore *store, GSType keyType, GSRowKeyPredicate **predicate);

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowKeyPredicateGeneral(
		GSGridStore *store, const GSContainerInfo *info,
		GSRowKeyPredicate **predicate);

#endif 

#endif 






GS_DLL_PUBLIC void GS_API_CALL gsCloseContainer(
		GSContainer **container, GSBool allRelated);

#if GS_DEPRECATED_FUNC_ENABLED

GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
	GSResult GS_API_CALL gsCreateEventNotification(
	GSContainer *container, const GSChar *url));

#endif 

#if GS_COMPATIBILITY_SUPPORT_1_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateTrigger(
		GSContainer *container, const GSTriggerInfo *info);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateIndex(
		GSContainer *container,
		const GSChar *columnName, GSIndexTypeFlags flags);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateIndexDetailV4_3(
		GSContainer *container, const GSIndexInfo *info);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsCreateIndexDetail(
		GSContainer *container, const GSIndexInfo *info) {
	return gsCreateIndexDetailV4_3(container, info);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateIndexDetail(
		GSContainer *container, const GSIndexInfo *info);

#endif 

#if GS_DEPRECATED_FUNC_ENABLED

GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
	GSResult GS_API_CALL gsDropEventNotification(
	GSContainer *container, const GSChar *url));

#endif 

#if GS_COMPATIBILITY_SUPPORT_1_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsDropTrigger(
		GSContainer *container, const GSChar *name);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsDropIndex(
		GSContainer *container,
		const GSChar *columnName, GSIndexTypeFlags flags);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsDropIndexDetailV4_3(
		GSContainer *container, const GSIndexInfo *info);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsDropIndexDetail(
		GSContainer *container, const GSIndexInfo *info) {
	return gsDropIndexDetailV4_3(container, info);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsDropIndexDetail(
		GSContainer *container, const GSIndexInfo *info);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsFlush(GSContainer *container);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRow(
		GSContainer *container, const void *key, void *rowObj, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRow(
		GSContainer *container, const void *key, const void *rowObj,
		GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutMultipleRows(
		GSContainer *container, const void *const *rowObjs, size_t rowCount,
		GSBool *exists);

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsCompatibleFunc_PutMultipleRows1(
		GSContainer *container, size_t rowCount, const void *const *rowObjs,
		GSBool *exists));
#endif

#if GS_COMPATIBILITY_GET_MULTIPLE_ROWS_BETA_0_3
#define gsPutMultipleRows(container, rowCount, rowObjs, exists) \
		gsCompatibleFunc_PutMultipleRows1(container, rowCount, rowObjs, exists)
#else
#define gsPutMultipleRows(container, rowObjs, rowCount, exists) \
		gsPutMultipleRows(container, rowObjs, rowCount, exists)
#endif
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsQuery(GSContainer *container,
		const GSChar *queryString, GSQuery **query);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRow(
		GSContainer *container, const void *key, GSBool *exists));
#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRow(
		GSContainer *container, const void *key, GSBool *exists);

#if GS_COMPATIBILITY_SUPPORT_1_5


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetContainerType(
		GSContainer *container, GSContainerType *type);


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowByContainer(
		GSContainer *container, GSRow **row);

#endif 






GS_DLL_PUBLIC GSResult GS_API_CALL gsAbort(GSContainer *container);


GS_DLL_PUBLIC GSResult GS_API_CALL gsCommit(GSContainer *container);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowForUpdate(
		GSContainer *container, const void *key, void *rowObj, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetAutoCommit(
		GSContainer *container, GSBool enabled);






GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByInteger(
		GSContainer *container, int32_t key, void *rowObj,
		GSBool forUpdate, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByLong(
		GSContainer *container, int64_t key, void *rowObj,
		GSBool forUpdate, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByTimestamp(
		GSContainer *container, GSTimestamp key, void *rowObj,
		GSBool forUpdate, GSBool *exists);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByPreciseTimestamp(
		GSContainer *container, const GSPreciseTimestamp *key, void *rowObj,
		GSBool forUpdate, GSBool *exists);
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByString(
		GSContainer *container, const GSChar *key, void *rowObj,
		GSBool forUpdate, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowByInteger(
		GSContainer *container, int32_t key, const void *rowObj,
		GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowByLong(
		GSContainer *container, int64_t key, const void *rowObj,
		GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowByTimestamp(
		GSContainer *container, GSTimestamp key, const void *rowObj,
		GSBool *exists);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowByPreciseTimestamp(
		GSContainer *container, const GSPreciseTimestamp *key,
		const void *rowObj, GSBool *exists);
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowByString(
		GSContainer *container, const GSChar *key, const void *rowObj,
		GSBool *exists);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRowByInteger(
		GSContainer *container, int32_t key, GSBool *exists));
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRowByLong(
		GSContainer *container, int64_t key, GSBool *exists));
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRowByTimestamp(
		GSContainer *container, GSTimestamp key, GSBool *exists));
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsRemoveRowByString(
		GSContainer *container, const GSChar *key, GSBool *exists));
#endif


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowByInteger(
		GSContainer *container, int32_t key, GSBool *exists);



GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowByLong(
		GSContainer *container, int64_t key, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowByTimestamp(
		GSContainer *container, GSTimestamp key, GSBool *exists);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowByPreciseTimestamp(
		GSContainer *container, const GSPreciseTimestamp *key, GSBool *exists);
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowByString(
		GSContainer *container, const GSChar *key, GSBool *exists);

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowGeneral(
		GSContainer *container, GSRowKey *keyObj, GSRow *rowObj,
		GSBool forUpdate, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsPutRowGeneral(
		GSContainer *container, GSRowKey *keyObj, GSRow *rowObj,
		GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteRowGeneral(
		GSContainer *container, GSRowKey *keyObj, GSBool *exists);

#endif 






GS_DLL_PUBLIC GSResult GS_API_CALL gsQueryByGeometry(
		GSCollection *collection, const GSChar *column, const GSChar *geometry,
		GSGeometryOperator geometryOp, GSQuery **query);


GS_DLL_PUBLIC GSResult GS_API_CALL gsQueryByGeometryWithDisjointCondition(
		GSCollection *collection, const GSChar *column,
		const GSChar *geometryIntersection, const GSChar *geometryDisjoint,
		GSQuery **query);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSResult GS_API_CALL gsQueryByGeometryWithExclusion(
		GSCollection *collection, const GSChar *column,
		const GSChar *geometryIntersection, const GSChar *geometryDisjoint,
		GSQuery **query));
#endif






GS_DLL_PUBLIC GSResult GS_API_CALL gsAggregateTimeSeries(
		GSTimeSeries *timeSeries, GSTimestamp start, GSTimestamp end,
		const GSChar *column, GSAggregation aggregation,
		GSAggregationResult **aggregationResult);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAppendTimeSeriesRow(
		GSTimeSeries *timeSeries, const void *rowObj, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowByBaseTime(
		GSTimeSeries *timeSeries, GSTimestamp base, GSTimeOperator timeOp,
		void *rowObj, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsInterpolateTimeSeriesRow(
		GSTimeSeries *timeSeries, GSTimestamp base, const GSChar *column,
		void *rowObj, GSBool *exists);


GS_DLL_PUBLIC GSResult GS_API_CALL gsQueryByTimeSeriesRange(
		GSTimeSeries *timeSeries, GSTimestamp start, GSTimestamp end,
		GSQuery **query);


GS_DLL_PUBLIC GSResult GS_API_CALL gsQueryByTimeSeriesOrderedRange(
		GSTimeSeries *timeSeries,
		const GSTimestamp *start, const GSTimestamp *end,
		GSQueryOrder order, GSQuery **query);


GS_DLL_PUBLIC GSResult GS_API_CALL gsQueryByTimeSeriesSampling(
		GSTimeSeries *timeSeries, GSTimestamp start, GSTimestamp end,
		const GSChar *const *columnSet, size_t columnCount,
		GSInterpolationMode mode, int32_t interval, GSTimeUnit intervalUnit,
		GSQuery **query);

#if GS_DEPRECATED_FUNC_ENABLED
GS_DLL_PUBLIC GS_DEPRECATED_FUNC(GSResult GS_API_CALL
		gsCompatibleFunc_QueryByTimeSeriesSampling1(
		GSTimeSeries *timeSeries, GSTimestamp start, GSTimestamp end,
		const GSChar *const *columnSet,
		int32_t interval, GSTimeUnit intervalUnit,
		GSQuery **query));
#endif

#if GS_INTERNAL_DEFINITION_VISIBLE
#if GS_COMPATIBILITY_TIME_SERIES_SAMPLING_BETA_0_1
#define gsQueryByTimeSeriesSampling( \
				timeSeries, start, end, columnSet, interval, intervalUnit, query) \
		gsCompatibleFunc_QueryByTimeSeriesSampling1( \
				timeSeries, start, end, columnSet, interval, intervalUnit, query)
#else
#define gsQueryByTimeSeriesSampling( \
				timeSeries, start, end, columnSet, columnCount, \
				mode, interval, intervalUnit, query) \
		gsQueryByTimeSeriesSampling( \
				timeSeries, start, end, columnSet, columnCount, \
				mode, interval, intervalUnit, query)
#endif
#endif 





#if GS_COMPATIBILITY_SUPPORT_1_5


GS_DLL_PUBLIC void GS_API_CALL gsCloseRow(GSRow **row);

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSchemaV4_3(
		GSRow *row, GSContainerInfo *schemaInfo);
#endif



GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetRowSchema(
		GSRow *row, GSContainerInfo *schemaInfo) {
	return gsGetRowSchemaV4_3(row, schemaInfo);
}

#elif GS_COMPATIBILITY_SUPPORT_3_5

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSchemaV3_3(
		GSRow *row, GSContainerInfo *schemaInfo);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetRowSchema(
		GSRow *row, GSContainerInfo *schemaInfo) {
	return gsGetRowSchemaV3_3(row, schemaInfo);
}

#elif GS_COMPATIBILITY_SUPPORT_2_1

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSchemaV2_1(
		GSRow *row, GSContainerInfo *schemaInfo);

GS_STATIC_HEADER_FUNC_SPECIFIER GSResult gsGetRowSchema(
		GSRow *row, GSContainerInfo *schemaInfo) {
	return gsGetRowSchemaV2_1(row, schemaInfo);
}

#else

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSchema(
		GSRow *row, GSContainerInfo *schemaInfo);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldGeneral(
		GSRow *row, int32_t column, const GSValue *fieldValue,
		GSType type);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldGeneral(
		GSRow *row, int32_t column, GSValue *fieldValue, GSType *type);

#if GS_COMPATIBILITY_SUPPORT_3_5


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldNull(
		GSRow *row, int32_t column);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldNull(
		GSRow *row, int32_t column, GSBool *nullValue);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByString(
		GSRow *row, int32_t column, const GSChar *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsString(
		GSRow *row, int32_t column, const GSChar **fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByBool(
		GSRow *row, int32_t column, GSBool fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsBool(
		GSRow *row, int32_t column, GSBool *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByByte(
		GSRow *row, int32_t column, int8_t fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsByte(
		GSRow *row, int32_t column, int8_t *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByShort(
		GSRow *row, int32_t column, int16_t fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsShort(
		GSRow *row, int32_t column, int16_t *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByInteger(
		GSRow *row, int32_t column, int32_t fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsInteger(
		GSRow *row, int32_t column, int32_t *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByLong(
		GSRow *row, int32_t column, int64_t fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsLong(
		GSRow *row, int32_t column, int64_t *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByFloat(
		GSRow *row, int32_t column, float fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsFloat(
		GSRow *row, int32_t column, float *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByDouble(
		GSRow *row, int32_t column, double fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsDouble(
		GSRow *row, int32_t column, double *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByTimestamp(
		GSRow *row, int32_t column, GSTimestamp fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsTimestamp(
		GSRow *row, int32_t column, GSTimestamp *fieldValue);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByPreciseTimestamp(
		GSRow *row, int32_t column, const GSPreciseTimestamp *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsPreciseTimestamp(
		GSRow *row, int32_t column, GSPreciseTimestamp *fieldValue);
#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByGeometry(
		GSRow *row, int32_t column, const GSChar *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsGeometry(
		GSRow *row, int32_t column, const GSChar **fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByBlob(
		GSRow *row, int32_t column, const GSBlob *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsBlob(
		GSRow *row, int32_t column, GSBlob *fieldValue);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByStringArray(
		GSRow *row, int32_t column, const GSChar *const *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsStringArray(
		GSRow *row, int32_t column, const GSChar *const **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByBoolArray(
		GSRow *row, int32_t column, const GSBool *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsBoolArray(
		GSRow *row, int32_t column, const GSBool **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByByteArray(
		GSRow *row, int32_t column, const int8_t *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsByteArray(
		GSRow *row, int32_t column, const int8_t **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByShortArray(
		GSRow *row, int32_t column, const int16_t *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsShortArray(
		GSRow *row, int32_t column, const int16_t **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByIntegerArray(
		GSRow *row, int32_t column, const int32_t *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsIntegerArray(
		GSRow *row, int32_t column, const int32_t **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByLongArray(
		GSRow *row, int32_t column, const int64_t *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsLongArray(
		GSRow *row, int32_t column, const int64_t **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByFloatArray(
		GSRow *row, int32_t column, const float *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsFloatArray(
		GSRow *row, int32_t column, const float **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByDoubleArray(
		GSRow *row, int32_t column, const double *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsDoubleArray(
		GSRow *row, int32_t column, const double **fieldValue,
		size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetRowFieldByTimestampArray(
		GSRow *row, int32_t column, const GSTimestamp *fieldValue,
		size_t size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowFieldAsTimestampArray(
		GSRow *row, int32_t column, const GSTimestamp **fieldValue,
		size_t *size);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowByRow(
		GSRow *row, GSRow **destRow);


GS_DLL_PUBLIC GSResult GS_API_CALL gsCreateRowKeyByRow(
		GSRow *row, GSRowKey **key);

#endif 






GS_DLL_PUBLIC void GS_API_CALL gsCloseQuery(GSQuery **query);


GS_DLL_PUBLIC GSResult GS_API_CALL gsFetch(
		GSQuery *query, GSBool forUpdate, GSRowSet **rowSet);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetFetchOption(
		GSQuery *query, GSFetchOption fetchOption,
		const void *value, GSType valueType);

#if GS_COMPATIBILITY_SUPPORT_1_5


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSet(
		GSQuery *query, GSRowSet **rowSet);

#endif 






GS_DLL_PUBLIC void GS_API_CALL gsCloseRowSet(GSRowSet **rowSet);


GS_DLL_PUBLIC GSResult GS_API_CALL gsDeleteCurrentRow(
		GSRowSet *rowSet);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetNextRow(
		GSRowSet *rowSet, void *rowObj);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetNextAggregation(
		GSRowSet *rowSet, GSAggregationResult **aggregationResult);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetNextQueryAnalysis(
		GSRowSet *rowSet, GSQueryAnalysisEntry *queryAnalysis);


GS_DLL_PUBLIC GSRowSetType GS_API_CALL gsGetRowSetType(GSRowSet *rowSet);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetRowSetSchema(
		GSRowSet *rowSet, GSContainerInfo *schemaInfo, GSBool *exists);
#endif 


GS_DLL_PUBLIC int32_t GS_API_CALL gsGetRowSetSize(GSRowSet *rowSet);


GS_DLL_PUBLIC GSBool GS_API_CALL gsHasNextRow(GSRowSet *rowSet);


GS_DLL_PUBLIC GSResult GS_API_CALL gsUpdateCurrentRow(
		GSRowSet *rowSet, const void *rowObj);






GS_DLL_PUBLIC void GS_API_CALL gsCloseAggregationResult(
		GSAggregationResult **aggregationResult);


GS_DLL_PUBLIC GSBool GS_API_CALL gsGetAggregationValue(
		GSAggregationResult *aggregationResult, void *value,
		GSType valueType);

#if GS_COMPATIBILITY_SUPPORT_3_5


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetAggregationValueAsLong(
		GSAggregationResult *aggregationResult, int64_t *value,
		GSBool *assigned);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetAggregationValueAsDouble(
		GSAggregationResult *aggregationResult, double *value,
		GSBool *assigned);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetAggregationValueAsTimestamp(
		GSAggregationResult *aggregationResult, GSTimestamp *value,
		GSBool *assigned);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetAggregationValueAsPreciseTimestamp(
		GSAggregationResult *aggregationResult, GSPreciseTimestamp *value,
		GSBool *assigned);
#endif 

#endif 





#if GS_COMPATIBILITY_SUPPORT_1_5


GS_DLL_PUBLIC void GS_API_CALL gsCloseRowKeyPredicate(
		GSRowKeyPredicate **predicate);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateKeyType(
		GSRowKeyPredicate *predicate, GSType *keyType);

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateKeySchema(
		GSRowKeyPredicate *predicate, GSContainerInfo *info);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartGeneralKey(
		GSRowKeyPredicate *predicate, GSRowKey **keyObj);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyGeneral(
		GSRowKeyPredicate *predicate, const GSValue **startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyAsString(
		GSRowKeyPredicate *predicate, const GSChar **startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyAsInteger(
		GSRowKeyPredicate *predicate, const int32_t **startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyAsLong(
		GSRowKeyPredicate *predicate, const int64_t **startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyAsTimestamp(
		GSRowKeyPredicate *predicate, const GSTimestamp **startKey);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateStartKeyAsPreciseTimestamp(
		GSRowKeyPredicate *predicate, const GSPreciseTimestamp **startKey);
#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishGeneralKey(
		GSRowKeyPredicate *predicate, GSRowKey **keyObj);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyGeneral(
		GSRowKeyPredicate *predicate, const GSValue **finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyAsString(
		GSRowKeyPredicate *predicate, const GSChar **finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyAsInteger(
		GSRowKeyPredicate *predicate, const int32_t **finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyAsLong(
		GSRowKeyPredicate *predicate, const int64_t **finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyAsTimestamp(
		GSRowKeyPredicate *predicate, const GSTimestamp **finishKey);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateFinishKeyAsPreciseTimestamp(
		GSRowKeyPredicate *predicate, const GSPreciseTimestamp **finishKey);
#endif 


#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctGeneralKeys(
		GSRowKeyPredicate *predicate, GSRowKey *const **keyObjList,
		size_t *size);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysGeneral(
		GSRowKeyPredicate *predicate, const GSValue **keyList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysAsString(
		GSRowKeyPredicate *predicate,
		const GSChar *const **keyList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysAsInteger(
		GSRowKeyPredicate *predicate, const int32_t **keyList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysAsLong(
		GSRowKeyPredicate *predicate, const int64_t **keyList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysAsTimestamp(
		GSRowKeyPredicate *predicate,
		const GSTimestamp **keyList, size_t *size);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPredicateDistinctKeysAsPreciseTimestamp(
		GSRowKeyPredicate *predicate,
		const GSPreciseTimestamp **keyList, size_t *size);
#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartGeneralKey(
		GSRowKeyPredicate *predicate, GSRowKey *keyObj);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyGeneral(
		GSRowKeyPredicate *predicate, const GSValue *startKey, GSType keyType);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyByString(
		GSRowKeyPredicate *predicate, const GSChar *startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyByInteger(
		GSRowKeyPredicate *predicate, const int32_t *startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyByLong(
		GSRowKeyPredicate *predicate, const int64_t *startKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyByTimestamp(
		GSRowKeyPredicate *predicate, const GSTimestamp *startKey);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateStartKeyByPreciseTimestamp(
		GSRowKeyPredicate *predicate, const GSPreciseTimestamp *startKey);
#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishGeneralKey(
		GSRowKeyPredicate *predicate, GSRowKey *keyObj);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyGeneral(
		GSRowKeyPredicate *predicate, const GSValue *finishKey, GSType keyType);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyByString(
		GSRowKeyPredicate *predicate, const GSChar *finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyByInteger(
		GSRowKeyPredicate *predicate, const int32_t *finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyByLong(
		GSRowKeyPredicate *predicate, const int64_t *finishKey);


GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyByTimestamp(
		GSRowKeyPredicate *predicate, const GSTimestamp *finishKey);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsSetPredicateFinishKeyByPreciseTimestamp(
		GSRowKeyPredicate *predicate, const GSPreciseTimestamp *finishKey);
#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateGeneralKey(
		GSRowKeyPredicate *predicate, GSRowKey *keyObj);

#endif 


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyGeneral(
		GSRowKeyPredicate *predicate, const GSValue *key, GSType keyType);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyByString(
		GSRowKeyPredicate *predicate, const GSChar *key);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyByInteger(
		GSRowKeyPredicate *predicate, int32_t key);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyByLong(
		GSRowKeyPredicate *predicate, int64_t key);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyByTimestamp(
		GSRowKeyPredicate *predicate, GSTimestamp key);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSResult GS_API_CALL gsAddPredicateKeyByPreciseTimestamp(
		GSRowKeyPredicate *predicate, const GSPreciseTimestamp *key);
#endif 

#endif 





#if GS_COMPATIBILITY_SUPPORT_1_5


GS_DLL_PUBLIC void GS_API_CALL gsClosePartitionController(
		GSPartitionController **controller);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionCount(
		GSPartitionController *controller, int32_t *partitionCount);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionContainerCount(
		GSPartitionController *controller, int32_t partitionIndex,
		int64_t *containerCount);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionContainerNames(
		GSPartitionController *controller, int32_t partitionIndex,
		int64_t start, const int64_t *limit,
		const GSChar *const **nameList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionHosts(
		GSPartitionController *controller, int32_t partitionIndex,
		const GSChar *const **addressList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionOwnerHost(
		GSPartitionController *controller, int32_t partitionIndex,
		const GSChar **address);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionBackupHosts(
		GSPartitionController *controller, int32_t partitionIndex,
		const GSChar *const **addressList, size_t *size);


GS_DLL_PUBLIC GSResult GS_API_CALL gsAssignPartitionPreferableHost(
		GSPartitionController *controller, int32_t partitionIndex,
		const GSChar *host);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetPartitionIndexOfContainer(
		GSPartitionController *controller, const GSChar *containerName,
		int32_t *partitionIndex);

#endif 






GS_DLL_PUBLIC GSTimestamp GS_API_CALL gsCurrentTime();

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC int64_t GS_API_CALL gsGetTimeField(
		GSTimestamp timestamp, GSTimeUnit timeUnit);


GS_DLL_PUBLIC int64_t GS_API_CALL gsGetZonedTimeField(
		GSTimestamp timestamp, GSTimeUnit timeUnit, const GSTimeZone *zone);


GS_DLL_PUBLIC GSBool GS_API_CALL gsSetTimeField(
		GSTimestamp *timestamp, int64_t field, GSTimeUnit timeUnit);


GS_DLL_PUBLIC GSBool GS_API_CALL gsSetZonedTimeField(
		GSTimestamp *timestamp, int64_t field, GSTimeUnit timeUnit,
		const GSTimeZone *zone);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3

#if GS_INTERNAL_DEFINITION_VISIBLE
GS_DLL_PUBLIC GSTimestamp GS_API_CALL gsAddTimeV4_3(
		GSTimestamp timestamp, int64_t amount, GSTimeUnit timeUnit);
#endif


GS_STATIC_HEADER_FUNC_SPECIFIER GSTimestamp gsAddTime(
		GSTimestamp timestamp, int64_t amount, GSTimeUnit timeUnit) {
	return gsAddTimeV4_3(timestamp, amount, timeUnit);
}

#else

GS_DLL_PUBLIC GSTimestamp GS_API_CALL gsAddTime(
		GSTimestamp timestamp, int32_t amount, GSTimeUnit timeUnit);

#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC GSTimestamp GS_API_CALL gsAddZonedTime(
		GSTimestamp timestamp, int64_t amount, GSTimeUnit timeUnit,
		const GSTimeZone *zone);


GS_DLL_PUBLIC int64_t GS_API_CALL gsGetTimeDiff(
		GSTimestamp timestamp1, GSTimestamp timestamp2, GSTimeUnit timeUnit);


GS_DLL_PUBLIC int64_t GS_API_CALL gsGetZonedTimeDiff(
		GSTimestamp timestamp1, GSTimestamp timestamp2, GSTimeUnit timeUnit,
		const GSTimeZone *zone);

#endif 


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatTime(
		GSTimestamp timestamp, GSChar *strBuf, size_t bufSize);

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatZonedTime(
		GSTimestamp timestamp, GSChar *strBuf, size_t bufSize,
		const GSTimeZone *zone);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC size_t GS_API_CALL gsFormatPreciseTime(
		const GSPreciseTimestamp *timestamp, GSChar *strBuf, size_t bufSize,
		const GSTimestampFormatOption *option);
#endif 

#endif 


GS_DLL_PUBLIC GSBool GS_API_CALL gsParseTime(
		const GSChar *str, GSTimestamp *timestamp);

#if GS_COMPATIBILITY_SUPPORT_5_3

GS_DLL_PUBLIC GSBool GS_API_CALL gsParsePreciseTime(
		const GSChar *str, GSPreciseTimestamp *timestamp,
		const GSTimestampFormatOption *option);
#endif 

#if GS_COMPATIBILITY_SUPPORT_4_3


GS_DLL_PUBLIC int64_t GS_API_CALL gsGetTimeZoneOffset(
		const GSTimeZone *zone, GSTimeUnit timeUnit);


GS_DLL_PUBLIC GSBool GS_API_CALL gsSetTimeZoneOffset(
		GSTimeZone *zone, int64_t offset, GSTimeUnit timeUnit);


GS_DLL_PUBLIC size_t gsFormatTimeZone(
		const GSTimeZone *zone, GSChar *strBuf, size_t bufSize);


GS_DLL_PUBLIC GSBool GS_API_CALL gsParseTimeZone(
		const GSChar *str, GSTimeZone *zone);

#endif 








GS_DLL_PUBLIC size_t GS_API_CALL gsGetErrorStackSize(
		void *gsResource);


GS_DLL_PUBLIC GSResult GS_API_CALL gsGetErrorCode(
		void *gsResource, size_t stackIndex);


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorMessage(
		void *gsResource, size_t stackIndex, GSChar *strBuf, size_t bufSize);


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorLocation(
		void *gsResource, size_t stackIndex, GSChar *strBuf, size_t bufSize);

#if GS_COMPATIBILITY_SUPPORT_1_5



#if GS_COMPATIBILITY_DEPRECATE_FETCH_OPTION_SIZE
#if GS_DEPRECATED_FUNC_ENABLED

GS_DLL_PUBLIC GS_DEPRECATED_FUNC(
		GSBool GS_API_CALL gsIsRecoverableError(GSResult result));
#endif
#endif


GS_DLL_PUBLIC GSBool GS_API_CALL gsIsTimeoutError(GSResult result);

#endif 


#if GS_COMPATIBILITY_SUPPORT_4_2

GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorName(
		void *gsResource, size_t stackIndex, GSChar *strBuf, size_t bufSize);


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorDescription(
		void *gsResource, size_t stackIndex, GSChar *strBuf, size_t bufSize);


GS_DLL_PUBLIC size_t GS_API_CALL gsGetErrorParameterCount(
		void *gsResource, size_t stackIndex);


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorParameterName(
		void *gsResource, size_t stackIndex, size_t parameterIndex,
		GSChar *strBuf, size_t bufSize);


GS_DLL_PUBLIC size_t GS_API_CALL gsFormatErrorParameterValue(
		void *gsResource, size_t stackIndex, size_t parameterIndex,
		GSChar *strBuf, size_t bufSize);
#endif 





#if GS_EXPERIMENTAL_TOOL_ENABLED
typedef struct GSExperimentalRowIdTag {
	struct {
		GSCollection *container;
		int64_t transactionId;
		int64_t baseId;
	} internal;
} GSExperimentalRowId;

GS_DLL_PUBLIC GSResult GS_API_CALL gsExperimentalGetRowIdForUpdate(
		GSRowSet *rowSet, GSExperimentalRowId *rowId);
GS_DLL_PUBLIC GSResult GS_API_CALL gsExperimentalUpdateRowById(
		GSContainer *container, const GSExperimentalRowId *rowId,
		const void *rowObj);
GS_DLL_PUBLIC GSResult GS_API_CALL gsExperimentalDeleteRowById(
		GSContainer *container, const GSExperimentalRowId *rowId);
#endif







#if GS_INTERNAL_DEFINITION_VISIBLE
#define GS_STRUCT_BINDING_GETTER_NAME(type) \
	gsSetupStructBindingOf_##type
#endif


#define GS_GET_STRUCT_BINDING(type) \
	GS_STRUCT_BINDING_GETTER_NAME(type) ()


#define GS_STRUCT_BINDING(type, entries) \
	GS_STATIC_HEADER_FUNC_SPECIFIER const GSBinding* \
	GS_GET_STRUCT_BINDING(type) { \
		typedef type GSBindingType; \
		static GSBindingEntry assignedEntries[] = { \
			entries \
		}; \
		static GSBinding binding = { \
			assignedEntries, \
			sizeof(assignedEntries) / sizeof(*assignedEntries) \
		}; \
		return &binding; \
	}

#if GS_INTERNAL_DEFINITION_VISIBLE
#define GS_STRUCT_OFFSET_OF(member) \
	( \
		(uintptr_t) &((GSBindingType*) 0)->member - \
		(uintptr_t) ((GSBindingType*) 0) \
	)

#if GS_COMPATIBILITY_SUPPORT_4_3
#define GS_STRUCT_BINDING_CUSTOM_DETAIL( \
	name, elementType, offset, arraySizeOffset, options, keyBinding, \
	keyBindingGetter ) \
	{ name, elementType, offset, arraySizeOffset, options, keyBinding, \
	keyBindingGetter },
#else
#define GS_STRUCT_BINDING_CUSTOM_DETAIL( \
	name, elementType, offset, arraySizeOffset, options, keyBinding, \
	keyBindingGetter ) \
	{ name, elementType, offset, arraySizeOffset, options },
#endif 

#define GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
	name, member, memberType, options) \
	GS_STRUCT_BINDING_CUSTOM_DETAIL( \
		name, memberType, GS_STRUCT_OFFSET_OF(member), \
		(size_t) -1, options, NULL, NULL)

#define GS_STRUCT_BINDING_CUSTOM_ELEMENT(member, memberType, options) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
		#member, member, memberType, options)

#define GS_STRUCT_BINDING_CUSTOM_NAMED_ARRAY( \
	name, member, sizeMember, elementType, options) \
	GS_STRUCT_BINDING_CUSTOM_DETAIL( \
		name, elementType, GS_STRUCT_OFFSET_OF(member), \
		GS_STRUCT_OFFSET_OF(sizeMember), options, NULL, NULL)

#define GS_STRUCT_BINDING_CUSTOM_ARRAY( \
	member, sizeMember, elementType, options) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ARRAY( \
		#member, member, sizeMember, elementType, options)
#endif 


#define GS_STRUCT_BINDING_NAMED_ELEMENT(name, member, memberType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
		name, member, memberType, 0)


#define GS_STRUCT_BINDING_NAMED_KEY(name, member, memberType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
		name, member, memberType, GS_TYPE_OPTION_KEY)


#define GS_STRUCT_BINDING_NAMED_ARRAY( \
	name, member, sizeMember, elementType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ARRAY( \
		name, member, sizeMember, elementType, 0)


#define GS_STRUCT_BINDING_ELEMENT(member, memberType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
		#member, member, memberType, 0)


#define GS_STRUCT_BINDING_KEY(member, memberType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ELEMENT( \
		#member, member, memberType, GS_TYPE_OPTION_KEY)


#define GS_STRUCT_BINDING_ARRAY(member, sizeMember, elementType) \
	GS_STRUCT_BINDING_CUSTOM_NAMED_ARRAY( \
		#member, member, sizeMember, elementType, 0)

#if GS_COMPATIBILITY_SUPPORT_4_3


#define GS_STRUCT_BINDING_COMPOSITE_KEY(member, bindingType) \
	GS_STRUCT_BINDING_CUSTOM_DETAIL( \
		#member, -1, GS_STRUCT_OFFSET_OF(member), \
		(size_t) -1, 0, NULL, GS_STRUCT_BINDING_GETTER_NAME(bindingType))

#endif 

#ifdef __cplusplus
}
#endif

#endif 
