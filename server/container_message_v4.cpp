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
#include "container_message_v4.h"
#include "result_set.h"
#include "base_container.h"

ResponsMesV4::~ResponsMesV4() {
}

SimpleQueryMessage::~SimpleQueryMessage() {
	ALLOC_DELETE(*alloc_, rsGuard_);
}

void SimpleQueryMessage::encode(EventByteOutStream& out) {
	encode<EventByteOutStream>(out);
}

void SimpleQueryMessage::decode(EventByteInStream& in) {
	decode<EventByteInStream>(in);
}

void SimpleQueryMessage::encode(OutStream& out) {
	encode<OutStream>(out);
}

template<typename S>
void SimpleQueryMessage::encode(S& out) {
	DataStoreUtil::encodeBooleanData<S>(out, existFlag_);
	if (existFlag_) {
		DataStoreUtil::encodeBinaryData<S>(out, rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		DataStoreUtil::encodeBinaryData<S>(out, rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	}
}

DSRowSetOutputMes::~DSRowSetOutputMes() {
	ALLOC_DELETE(*alloc_, rsGuard_);
}

DSTQLOutputMes::~DSTQLOutputMes() {
	ALLOC_DELETE(*alloc_, rsGuard_);
}

DSContainerOutputMes::~DSContainerOutputMes() {
	ALLOC_DELETE(*alloc_, container_);
}
