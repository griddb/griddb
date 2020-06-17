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
#include "gridstore.h"
#include "assert.h"

GSResult GS_API_CALL gsGetCollectionV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSCollection **collection) {
	(void) store;
	(void) name;
	(void) binding;
	(void) collection;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsGetContainerInfoV4_3(
		GSGridStore *store, const GSChar *name, GSContainerInfo *info,
		GSBool *exists) {
	(void) store;
	(void) name;
	(void) info;
	(void) exists;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsGetTimeSeriesV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, GSTimeSeries **timeSeries) {
	(void) store;
	(void) name;
	(void) binding;
	(void) timeSeries;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutContainerV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	(void) store;
	(void) name;
	(void) binding;
	(void) info;
	(void) modifiable;
	(void) container;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutCollectionV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSCollectionProperties *properties,
		GSBool modifiable, GSCollection **collection) {
	(void) store;
	(void) name;
	(void) binding;
	(void) properties;
	(void) modifiable;
	(void) collection;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutTimeSeriesV4_3(
		GSGridStore *store, const GSChar *name,
		const GSBinding *binding, const GSTimeSeriesProperties *properties,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	(void) store;
	(void) name;
	(void) binding;
	(void) properties;
	(void) modifiable;
	(void) timeSeries;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutContainerGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	(void) store;
	(void) name;
	(void) info;
	(void) modifiable;
	(void) container;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutCollectionGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSContainer **container) {
	(void) store;
	(void) name;
	(void) info;
	(void) modifiable;
	(void) container;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsPutTimeSeriesGeneralV4_3(
		GSGridStore *store, const GSChar *name,
		const GSContainerInfo *info,
		GSBool modifiable, GSTimeSeries **timeSeries) {
	(void) store;
	(void) name;
	(void) info;
	(void) modifiable;
	(void) timeSeries;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsCreateRowByStoreV4_3(
		GSGridStore *store, const GSContainerInfo *info, GSRow **row) {
	(void) store;
	(void) info;
	(void) row;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsCreateIndexDetailV4_3(
		GSContainer *container, const GSIndexInfo *info) {
	(void) container;
	(void) info;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsDropIndexDetailV4_3(
		GSContainer *container, const GSIndexInfo *info) {
	(void) container;
	(void) info;
	assert(0);
	return -1;
}

GSResult GS_API_CALL gsGetRowSchemaV4_3(
		GSRow *row, GSContainerInfo *schemaInfo) {
	(void) row;
	(void) schemaInfo;
	assert(0);
	return -1;
}

GSTimestamp GS_API_CALL gsAddTimeV4_3(
		GSTimestamp timestamp, int64_t amount, GSTimeUnit timeUnit) {
	(void) timestamp;
	(void) amount;
	(void) timeUnit;
	assert(0);
	return -1;
}
