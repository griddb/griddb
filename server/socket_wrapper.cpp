/*
    Copyright (c) 2020 TOSHIBA Digital Solutions Corporation

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
	@brief Implementation of socket wrapper
*/

#include "socket_wrapper.h"
#include "gs_error_common.h"
#include <cassert>


SocketFactory::SocketFactory() :
		funcTable_("SocketFactory"),
		obj_(NULL) {
	funcTable_.assign(PlainSocketFactory::getProvider());
}

SocketFactory::~SocketFactory() {
	close();
}

void SocketFactory::initialize(util::LibraryFunctions::ProviderFunc provider) {
	close();
	funcTable_.assign(provider);

	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_CREATE_FACTORY>()(&obj_, &ex),
			funcTable_, ex, "");
}

void SocketFactory::close() throw() {
	if (obj_ != NULL) {
		try {
			funcTable_.resolve<Functions::FUNC_CLOSE>()(obj_);
		}
		catch (...) {
			assert(false);
		}
		obj_ = NULL;
	}
}

void SocketFactory::assign(
		util::LibraryFunctions::ProviderFunc provider,
		SocketFactoryTag *obj) {
	close();
	funcTable_.assign(provider);
	obj_ = obj;
}

SocketFactoryTag* SocketFactory::release() throw() {
	SocketFactoryTag *obj = obj_;
	obj_ = NULL;
	return obj;
}


void SocketFactory::create(AbstractSocket &socket) {
	util::LibraryFunctions::ProviderFunc providerFunc;
	AbstractSocketTag *sockObj;
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_CREATE_SOCKET>()(
					obj_, &providerFunc, &sockObj, &ex),
			funcTable_, ex, "");
	socket.initialize(providerFunc, sockObj);
}

void SocketFactory::setSslClientMode(bool clientMode) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SET_SSL_CLIENT_MODE>()(
					obj_, static_cast<int32_t>(clientMode), &ex),
			funcTable_, ex, "");
}

void SocketFactory::setSslVerifyMode(bool verifyMode) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SET_SSL_VERIFY_MODE>()(
					obj_, static_cast<int32_t>(verifyMode), &ex),
			funcTable_, ex, "");
}

void SocketFactory::setSslCertFile(const char8_t *file) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SET_SSL_CERT_FILE>()(
					obj_, file, &ex),
			funcTable_, ex, "");
}

void SocketFactory::setSslKeyFile(const char8_t *file) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SET_SSL_KEY_FILE>()(
					obj_, file, &ex),
			funcTable_, ex, "");
}

void SocketFactory::setSslMaxProtocolVersion(
		SslConfig::SslProtocolVersion version) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SET_SSL_MAX_PROTOCOL_VERSION>()(
					obj_, version, &ex),
			funcTable_, ex, "");
}


AbstractSocket::AbstractSocket() :
		funcTable_("Socket"),
		obj_(NULL) {
}

AbstractSocket::~AbstractSocket() {
	try {
		close();
	}
	catch (...) {
	}
}

void AbstractSocket::initialize(
		util::LibraryFunctions::ProviderFunc provider,
		AbstractSocketTag *obj) {
	close();
	funcTable_.assign(provider);
	obj_ = obj;
}

void AbstractSocket::close() {
	if (obj_ != NULL) {
		try {
			util::Socket::close();
		}
		catch (...) {
		}
		try {
			funcTable_.resolve<Functions::FUNC_CLOSE>()(obj_);
		}
		catch (...) {
			assert(false);
		}
		obj_ = NULL;
	}
}

void AbstractSocket::attach(util::File::FD fd) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_ATTACH>()(obj_, fd, &ex),
			funcTable_, ex, "");
	util::Socket::attach(fd);
}

util::File::FD AbstractSocket::detach() {
	if (obj_ == NULL) {
		return INITIAL_FD;
	}

	util::File::FD fd = util::Socket::detach();
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_DETACH>()(obj_, &ex),
			funcTable_, ex, "");
	return fd;
}

void AbstractSocket::shutdown(bool forReceive, bool forSend) {
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SHUTDOWN>()(
					obj_, static_cast<int32_t>(forReceive),
					static_cast<int32_t>(forSend), &ex),
			funcTable_, ex, "");
}

ssize_t AbstractSocket::receive(void *buf, size_t len, int flags) {
	int64_t receivedSize;
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_RECEIVE>()(
					obj_, buf, len, flags, &receivedSize, &ex),
			funcTable_, ex, "");
	return static_cast<ssize_t>(receivedSize);
}

ssize_t AbstractSocket::send(const void *buf, size_t len, int flags) {
	int64_t sentSize;
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_SEND>()(
					obj_, buf, len, flags, &sentSize, &ex),
			funcTable_, ex, "");
	return static_cast<ssize_t>(sentSize);
}

ssize_t AbstractSocket::read(void *buf, size_t len) {
	return receive(buf, len);
}

ssize_t AbstractSocket::write(const void *buf, size_t len) {
	return send(buf, len);
}

ssize_t AbstractSocket::read(void*, size_t, off_t) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

ssize_t AbstractSocket::write(const void*, size_t, off_t) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

void AbstractSocket::read(util::IOOperation&) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

void AbstractSocket::write(util::IOOperation&) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

AbstractSocket::SocketAction AbstractSocket::getNextAction() {
	int32_t action;
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_GET_NEXT_ACTION>()(
					obj_, &action, &ex),
			funcTable_, ex, "");
	return static_cast<SocketAction>(action);
}

bool AbstractSocket::isPending() {
	int32_t pending;
	UtilExceptionTag *ex;
	GS_COMMON_CHECK_LIBRARY_ERROR(
			funcTable_.resolve<Functions::FUNC_IS_PENDING>()(
					obj_, &pending, &ex),
			funcTable_, ex, "");
	return !!pending;
}


PlainSocketFactory::FuncTable PlainSocketFactory::FUNC_TABLE;
PlainSocketFactory::Initializer PlainSocketFactory::FUNC_TABLE_INITIALIZER(
		FUNC_TABLE);

util::LibraryFunctions::ProviderFunc PlainSocketFactory::getProvider() throw() {
	return &provideFunctions;
}

int32_t PlainSocketFactory::provideFunctions(
		const void *const **funcList, size_t *funcCount) throw() {
	return FUNC_TABLE.getFunctionList(funcList, funcCount);
}

void PlainSocketFactory::close(SocketFactoryTag *factory) throw() {
	assert(factory == NULL);
	static_cast<void>(factory);
}

int32_t PlainSocketFactory::createFactory(
		SocketFactoryTag **factory, UtilExceptionTag **ex) throw() {
	util::LibraryFunctions::trySetEmpty(factory);
	return util::LibraryFunctions::succeed(ex);
}

int32_t PlainSocketFactory::createSocket(
		SocketFactoryTag *factory,
		util::LibraryFunctions::ProviderFunc *providerFunc,
		AbstractSocketTag **socket, UtilExceptionTag **ex) throw() {
	static_cast<void>(factory);
	util::LibraryFunctions::trySetEmpty(providerFunc);
	util::LibraryFunctions::trySetEmpty(socket);
	try {
		util::LibraryFunctions::deref(providerFunc) =
				PlainSocket::getProvider();

		AbstractSocketTag *&socketOut = util::LibraryFunctions::deref(socket);
		socketOut = UTIL_NEW PlainSocket();

		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

PlainSocketFactory::Initializer::Initializer(FuncTable &table) {
	table.set<Functions::FUNC_EXCEPTION_PROVIDER>(
			util::LibraryException::getDefaultProvider());
	table.set<Functions::FUNC_CLOSE>(&close);
	table.set<Functions::FUNC_CREATE_FACTORY>(&createFactory);
	table.set<Functions::FUNC_CREATE_SOCKET>(&createSocket);
}


PlainSocket::FuncTable PlainSocket::FUNC_TABLE;
PlainSocket::Initializer PlainSocket::FUNC_TABLE_INITIALIZER(
		FUNC_TABLE);

util::LibraryFunctions::ProviderFunc PlainSocket::getProvider() throw() {
	return &provideFunctions;
}

int32_t PlainSocket::provideFunctions(
		const void *const **funcList, size_t *funcCount) throw() {
	return FUNC_TABLE.getFunctionList(funcList, funcCount);
}

void PlainSocket::close(AbstractSocketTag *socket) throw() {
	delete as(socket);
}

int32_t PlainSocket::attach(
		AbstractSocketTag *socket, util::File::FD fd,
		UtilExceptionTag **ex) throw() {
	try {
		deref(socket).base_.attach(fd);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::detach(
		AbstractSocketTag *socket, UtilExceptionTag **ex) throw() {
	try {
		deref(socket).base_.detach();
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::shutdown(
		AbstractSocketTag *socket, int32_t forReceive, int32_t forSend,
		UtilExceptionTag **ex) throw() {
	try {
		deref(socket).base_.shutdown(!!forReceive, !!forSend);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::receive(
		AbstractSocketTag *socket, void *buf, size_t len, int32_t flags,
		int64_t *receivedSize, UtilExceptionTag **ex) throw() {
	util::LibraryFunctions::trySet<int64_t>(receivedSize, -1);
	try {
		int64_t &receivedSizeOut = util::LibraryFunctions::deref(receivedSize);
		receivedSizeOut = deref(socket).base_.receive(buf, len, flags);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::send(
		AbstractSocketTag *socket, const void *buf, size_t len,
		int32_t flags, int64_t *sentSize, UtilExceptionTag **ex) throw() {
	util::LibraryFunctions::trySet<int64_t>(sentSize, -1);
	try {
		int64_t &sentSizeOut = util::LibraryFunctions::deref(sentSize);
		sentSizeOut = deref(socket).base_.send(buf, len, flags);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::getNextAction(
		AbstractSocketTag *socket, int32_t *action,
		UtilExceptionTag **ex) throw() {
	util::LibraryFunctions::trySet<int32_t>(
			action, AbstractSocket::ACTION_NONE);
	try {
		deref(socket);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

int32_t PlainSocket::isPending(
		AbstractSocketTag *socket, int32_t *pending,
		UtilExceptionTag **ex) throw() {
	util::LibraryFunctions::trySet<int32_t>(pending, 0);
	try {
		deref(socket);
		return util::LibraryFunctions::succeed(ex);
	}
	catch (...) {
		std::exception e;
		return GS_COMMON_LIBRARY_EXCEPTION_CONVERT(e, ex, "");
	}
}

PlainSocket& PlainSocket::deref(
		AbstractSocketTag *socket) {
	return util::LibraryFunctions::deref(as(socket));
}

PlainSocket* PlainSocket::as(
		AbstractSocketTag *socket) throw() {
	return static_cast<PlainSocket*>(socket);
}

PlainSocket::Initializer::Initializer(FuncTable &table) {
	table.set<Functions::FUNC_EXCEPTION_PROVIDER>(
			util::LibraryException::getDefaultProvider());
	table.set<Functions::FUNC_CLOSE>(&close);
	table.set<Functions::FUNC_ATTACH>(&attach);
	table.set<Functions::FUNC_DETACH>(&detach);
	table.set<Functions::FUNC_SHUTDOWN>(&shutdown);
	table.set<Functions::FUNC_RECEIVE>(&receive);
	table.set<Functions::FUNC_SEND>(&send);
	table.set<Functions::FUNC_GET_NEXT_ACTION>(&getNextAction);
	table.set<Functions::FUNC_IS_PENDING>(&isPending);
}
