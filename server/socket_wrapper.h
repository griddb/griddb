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
	@brief Definition of socket wrapper
*/

#ifndef SOCKET_WRAPPER_H_
#define SOCKET_WRAPPER_H_

#include "util/net.h"
#include "util/system.h"

struct SocketFactoryTag {};
struct AbstractSocketTag {};

class AbstractSocket;

struct SslConfig {
	enum SslProtocolVersion {
		SSL_VER_NONE,
		SSL_VER_SSL3,
		SSL_VER_TLS1_0,
		SSL_VER_TLS1_1,
		SSL_VER_TLS1_2,
		SSL_VER_TLS1_3
	};
};

class SocketFactory {
public:
	struct Functions;

	SocketFactory();
	~SocketFactory();

	void initialize(util::LibraryFunctions::ProviderFunc provider);
	void close() throw();

	void assign(
			util::LibraryFunctions::ProviderFunc provider,
			SocketFactoryTag *obj);
	SocketFactoryTag* release() throw();

	void create(AbstractSocket &socket);

	void setSslClientMode(bool clientMode);
	void setSslVerifyMode(bool verifyMode);
	void setSslCertFile(const char8_t *file);
	void setSslKeyFile(const char8_t *file);
	void setSslMaxProtocolVersion(SslConfig::SslProtocolVersion version);

private:
	SocketFactory(const SocketFactory&);
	SocketFactory& operator=(const SocketFactory&);

	util::LibraryFunctionTable<Functions> funcTable_;
	SocketFactoryTag *obj_;
};

struct SocketFactory::Functions {
	enum {
		FUNC_EXCEPTION_PROVIDER,
		FUNC_CLOSE,
		FUNC_CREATE_FACTORY,
		FUNC_CREATE_SOCKET,
		FUNC_SET_SSL_CLIENT_MODE,
		FUNC_SET_SSL_VERIFY_MODE,
		FUNC_SET_SSL_CERT_FILE,
		FUNC_SET_SSL_KEY_FILE,
		FUNC_SET_SSL_MAX_PROTOCOL_VERSION,

		END_FUNC
	};

	template<size_t F, int = 0>
	struct Traits {
	};

	template<int C> struct Traits<FUNC_EXCEPTION_PROVIDER, C> {
		typedef util::LibraryFunctions::ProviderFunc Func;
	};
	template<int C> struct Traits<FUNC_CLOSE, C> {
		typedef void (*Func)(SocketFactoryTag*);
	};
	template<int C> struct Traits<FUNC_CREATE_FACTORY, C> {
		typedef int32_t (*Func)(SocketFactoryTag**, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_CREATE_SOCKET, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, util::LibraryFunctions::ProviderFunc*,
				AbstractSocketTag**, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SET_SSL_CLIENT_MODE, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, int32_t, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SET_SSL_VERIFY_MODE, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, int32_t, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SET_SSL_CERT_FILE, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, const char8_t*, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SET_SSL_KEY_FILE, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, const char8_t*, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SET_SSL_MAX_PROTOCOL_VERSION, C> {
		typedef int32_t (*Func)(
				SocketFactoryTag*, int32_t, UtilExceptionTag**);
	};
};

class AbstractSocket : public util::Socket {
public:
	struct Functions;

	enum SocketAction {
		ACTION_NONE,
		ACTION_READ,
		ACTION_WRITE
	};

	AbstractSocket();
	~AbstractSocket();

	void initialize(
			util::LibraryFunctions::ProviderFunc provider,
			AbstractSocketTag *obj);

	virtual void close();

	virtual void attach(util::File::FD fd);
	virtual util::File::FD detach();

	void shutdown(bool forReceive, bool forSend);
	ssize_t receive(void *buf, size_t len, int flags = 0);
	ssize_t send(const void *buf, size_t len, int flags = 0);

	virtual ssize_t read(void *buf, size_t len);
	virtual ssize_t write(const void *buf, size_t len);

	virtual ssize_t read(void *buf, size_t blen, off_t offset);
	virtual ssize_t write(const void *buf, size_t blen, off_t offset);

	virtual void read(util::IOOperation &operation);
	virtual void write(util::IOOperation &operation);

	SocketAction getNextAction();
	bool isPending();

private:
	AbstractSocket(const AbstractSocket&);
	AbstractSocket& operator=(const AbstractSocket&);

	util::LibraryFunctionTable<Functions> funcTable_;
	AbstractSocketTag *obj_;
};

struct AbstractSocket::Functions {
	enum {
		FUNC_EXCEPTION_PROVIDER,
		FUNC_CLOSE,
		FUNC_ATTACH,
		FUNC_DETACH,
		FUNC_SHUTDOWN,
		FUNC_RECEIVE,
		FUNC_SEND,
		FUNC_GET_NEXT_ACTION,
		FUNC_IS_PENDING,

		END_FUNC
	};

	template<size_t F, int = 0>
	struct Traits {
	};

	template<int C> struct Traits<FUNC_EXCEPTION_PROVIDER, C> {
		typedef util::LibraryFunctions::ProviderFunc Func;
	};
	template<int C> struct Traits<FUNC_CLOSE, C> {
		typedef void (*Func)(AbstractSocketTag*);
	};
	template<int C> struct Traits<FUNC_ATTACH, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, util::File::FD, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_DETACH, C> {
		typedef int32_t (*Func)(AbstractSocketTag*, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SHUTDOWN, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, int32_t, int32_t, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_RECEIVE, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, void*, size_t, int32_t, int64_t*,
				UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_SEND, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, const void*, size_t, int32_t, int64_t*,
				UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_GET_NEXT_ACTION, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, int32_t*, UtilExceptionTag**);
	};
	template<int C> struct Traits<FUNC_IS_PENDING, C> {
		typedef int32_t (*Func)(
				AbstractSocketTag*, int32_t*, UtilExceptionTag**);
	};
};

class PlainSocketFactory {
public:
	typedef SocketFactory::Functions Functions;

	static util::LibraryFunctions::ProviderFunc getProvider() throw();

private:
	typedef util::LibraryFunctionTable<
			Functions>::Builder<Functions::END_FUNC> FuncTable;

	struct Initializer {
		explicit Initializer(FuncTable &table);
	};

	static int32_t provideFunctions(
			const void *const **funcList, size_t *funcCount) throw();

	static void close(SocketFactoryTag *factory) throw();
	static int32_t createFactory(
			SocketFactoryTag **factory, UtilExceptionTag **ex) throw();
	static int32_t createSocket(
			SocketFactoryTag *factory,
			util::LibraryFunctions::ProviderFunc *providerFunc,
			AbstractSocketTag **socket, UtilExceptionTag **ex) throw();

	static FuncTable FUNC_TABLE;
	static Initializer FUNC_TABLE_INITIALIZER;
};

class PlainSocket : public AbstractSocketTag {
public:
	typedef AbstractSocket::Functions Functions;

	static util::LibraryFunctions::ProviderFunc getProvider() throw();

private:
	typedef util::LibraryFunctionTable<
			Functions>::Builder<Functions::END_FUNC> FuncTable;

	struct Initializer {
		explicit Initializer(FuncTable &table);
	};

	static int32_t provideFunctions(
			const void *const **funcList, size_t *funcCount) throw();

	static void close(AbstractSocketTag *socket) throw();
	static int32_t attach(
			AbstractSocketTag *socket, util::File::FD fd,
			UtilExceptionTag **ex) throw();
	static int32_t detach(
			AbstractSocketTag *socket, UtilExceptionTag **ex) throw();
	static int32_t shutdown(
			AbstractSocketTag *socket, int32_t forReceive, int32_t forSend,
			UtilExceptionTag **ex) throw();
	static int32_t receive(
			AbstractSocketTag *socket, void *buf, size_t len, int32_t flags,
			int64_t *receivedSize, UtilExceptionTag **ex) throw();
	static int32_t send(
			AbstractSocketTag *socket, const void *buf, size_t len,
			int32_t flags, int64_t *sentSize, UtilExceptionTag **ex) throw();
	static int32_t getNextAction(
			AbstractSocketTag *socket, int32_t *action,
			UtilExceptionTag **ex) throw();
	static int32_t isPending(
			AbstractSocketTag *socket, int32_t *pending,
			UtilExceptionTag **ex) throw();

	static PlainSocket& deref(AbstractSocketTag *socket);
	static PlainSocket* as(AbstractSocketTag *socket) throw();

	static FuncTable FUNC_TABLE;
	static Initializer FUNC_TABLE_INITIALIZER;

	util::Socket base_;
};

#endif
