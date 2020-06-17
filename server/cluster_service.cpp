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
#include "cluster_service.h"
#include "cluster_manager.h"


void ClusterService::setClusterHandler(EventEngine::Source &source) {
	UNUSED_VARIABLE(source);
}

void ClusterSystemCommandHandler::operator()(EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(ev);

	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

void HeartbeatHandler::operator()(EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(ev);

	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

void NotifyClusterHandler::operator()(EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(ev);

	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

void TimerNotifyClusterHandler::operator()(EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(ev);

	GS_THROW_USER_ERROR(
			GS_ERROR_CM_NOT_SUPPORTED, "not support cluster operation");
}

