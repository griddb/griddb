/*
    Copyright (c) 2012 TOSHIBA CORPORATION.
    
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
/*
    Copyright (c) 2008, Yubin Lim(purewell@gmail.com).
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions 
    are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Purewell nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*!
	@file
    @brief Definition of Utility of network
*/
#ifndef UTIL_NET_H_
#define UTIL_NET_H_

#include "util/file.h"

#include <memory>
#include <vector>
#include <ostream>
#include <map>

struct sockaddr;
struct sockaddr_storage;
struct addrinfo;
struct in6_addr;
struct pollfd;

namespace util {

class IOPollBase;
class SocketAddress;

/*!
	@brief Types of network I/O events.
*/



class IOPollEvent {
public:
	static const int TYPE_READ;
	static const int TYPE_WRITE;
	static const int TYPE_ERROR;
	static const int TYPE_READ_WRITE;
	

	IOPollEvent() : flags_(0) {}
	IOPollEvent(int flags) : flags_(flags) {}
	operator int() const { return flags_; }
	IOPollEvent& operator=(int flags) { flags_ = flags; return *this; }

private:
	int flags_;
};

/*!
	@brief Handlers of network I/O events.
*/



class IOPollHandler {
public:
	virtual ~IOPollHandler();

	
	
	
	
	virtual void handlePollEvent(IOPollBase *io, IOPollEvent event) = 0;

	virtual File& getFile() = 0;
};

/*!
	@brief Common interface to accept network I/O events: e.g. Socket.
*/




class IOPollBase {
public:
	static const uint32_t WAIT_INFINITY;

	virtual ~IOPollBase();

	
	
	
	
	virtual void add(IOPollHandler *handler, IOPollEvent event) = 0;

	
	
	
	
	virtual void modify(IOPollHandler *handler, IOPollEvent event) = 0;

	
	
	
	virtual void remove(IOPollHandler *handler) = 0;

	
	
	
	virtual bool dispatch(uint32_t msec = WAIT_INFINITY) = 0;

	
	
	virtual const char* getType(void) const = 0;





	virtual void setInterruptible();








	virtual void interrupt();

private:
	struct InterruptionData;

	UTIL_UNIQUE_PTR<InterruptionData> interruptionData_;
};

/*!
	@brief Initializes and finalizes Socket library.
*/



struct SocketLibrary {
public:
	SocketLibrary();
	~SocketLibrary();

	static void checkAvailability();

private:
#ifdef _WIN32
	static int32_t counter_;
#endif
};

/*!
	@brief Organizes order of initialization and finalization of Socket library.
*/






static SocketLibrary g_socketLibrary;


/*!
	@brief Wrapper of Socket for network I/O.
*/



class Socket : public File, public IOPollHandler {
public:
	static const int TYPE_DATAGRAM;
	static const int TYPE_STREAM;

	static const uint32_t DEFAULT_MAX_BACKLOG_SIZE;

public:
	Socket(const char *user = NULL);
	virtual ~Socket();

public:
	virtual void close(void);

	
	
	
	
	virtual void open(int family, int type, int protocol = 0);

	
	
	virtual void open(const char8_t*,
			FileFlag, FilePermission = DEFAULT_PERMISSION);

	
	
	
	SocketAddress& getSocketName(SocketAddress &addr) const;

	
	
	
	SocketAddress& getPeerName(SocketAddress &addr) const;





	void shutdown(bool forReceive, bool forSend);

	
	
	void bind(const SocketAddress &addr);

	
	
	void listen(uint32_t backlogSize = DEFAULT_MAX_BACKLOG_SIZE);

	
	
	
	
	bool accept(Socket *socket, SocketAddress *addr = NULL) const;

	
	
	
	
	bool connect(const SocketAddress &addr);

	
	
	
	
	
	ssize_t receive(void *buf, size_t len, int flags = 0);

	
	
	
	
	
	
	ssize_t receiveFrom(
			void *buf, size_t len, SocketAddress *addr, int flags = 0);

	
	
	
	
	
	ssize_t send(const void *buf, size_t len, int flags = 0);

	
	
	
	
	
	
	ssize_t sendTo(const void *buf,
			size_t len, const SocketAddress &addr, int flags = 0);

	virtual ssize_t read(void *buf, size_t len);
	virtual ssize_t write(const void *buf, size_t len);

	virtual ssize_t read(void *buf, size_t blen, off_t offset);
	virtual ssize_t write(const void *buf, size_t blen, off_t offset);

	virtual void read(IOOperation &operation);
	virtual void write(IOOperation &operation);

	virtual off_t tell();

	
	
	bool getReuseAddress(void) const;

	
	
	
	void setReuseAddress(bool value);

	
	
	bool getKeepAlive(void) const;

	
	
	
	void setKeepAlive(bool value);

	void setKeepAlive(
			int32_t idleSeconds, int32_t intervalSeconds, int32_t retryCount);

	
	
	int32_t getReceiveBufferSize(void) const;

	
	
	
	void setReceiveBufferSize(int32_t value);

	
	
	int32_t getSendBufferSize(void) const;

	
	
	
	void setSendBufferSize(int32_t value);

	uint32_t getReceiveTimeout(void) const;

	void setReceiveTimeout(uint32_t value);

	uint32_t getSendTimeout(void) const;

	void setSendTimeout(uint32_t value);

	
	
	bool getNoDelay(void) const;

	
	
	
	void setNoDelay(bool value);

	bool getLinger(int32_t *timeoutSecs = NULL);

	void setLinger(bool enabled, int32_t timeoutSecs);

	int32_t getMulticastTTL();

	void setMulticastTTL(int32_t value);

	bool getMulticastLoopback();

	void setMulticastLoopback(bool value);

	
	
	
	virtual void setBlockingMode(bool value);

	void joinMulticastGroup(
			const SocketAddress &multicastAddr,
			const SocketAddress *interfaceAddr = NULL);

	void leaveMulticastGroup(
			const SocketAddress &multicastAddr,
			const SocketAddress *interfaceAddr = NULL);

	void handlePollEvent(IOPollBase *io, IOPollEvent event);

	File& getFile();

	const char* getUser() const;

#ifdef _WIN64
	typedef unsigned __int64 SocketHandle;
#elif defined(_WIN32)
	typedef __w64 unsigned int SocketHandle;
#else
	typedef int SocketHandle;
#endif

	static const SocketHandle INVALID_SOCKET_HANDLE;

	inline static SocketHandle toSocket(FD fd) {
#ifdef _WIN32
		return reinterpret_cast<SocketHandle>(fd);
#else
		return fd;
#endif
	}

	inline static FD toFD(SocketHandle socketHandle) {
#ifdef _WIN32
		return reinterpret_cast<FD>(socketHandle);
#else
		return socketHandle;
#endif
	}

	static bool isTimeoutError(int32_t errorCode);

private:
	static const int ADDITIONAL_MESSAGE_FLAGS;

	Socket(const Socket&);
	Socket& operator=(const Socket&);

	
	
	
	
	
	void getOption(int level, int name, void *value, size_t len) const;

	
	
	
	
	
	void setOption(int level, int name, const void *value, size_t len);

	
	
	
	
	int32_t getInt32Option(int level, int name) const;

	
	
	
	
	void setInt32Option(int level, int name, int32_t value);

	
	
	
	
	int64_t getInt64Option(int level, int name) const;

	
	
	
	
	void setInt64Option(int level, int name, int64_t value);

	
	
	
	
	bool getBoolOption(int level, int name) const;

	
	
	
	
	void setBoolOption(int level, int name, bool value);

	void setMulticastInterfaceOption(
			bool join,
			const SocketAddress &multicastAddr,
			const SocketAddress *interfaceAddr);

	bool isBlockable() const;

	int family_;

	const char *user_;
};

#if UTIL_FAILURE_SIMULATION_ENABLED
class SocketFailureSimulator {
public:
	typedef bool (*FilterHandler)(
			const Socket &socket, int32_t targetType, int operationType);

	static void set(int32_t targetType, uint64_t startCount, uint64_t endCount,
			FilterHandler handler);

	static void checkOperation(const Socket &socket, int operationType);

	static uint64_t getLastOperationCount() { return lastOperationCount_; }

private:
	SocketFailureSimulator();
	~SocketFailureSimulator();

	static volatile bool enabled_;
	static volatile FilterHandler handler_;
	static volatile int32_t targetType_;
	static volatile uint64_t startCount_;
	static volatile uint64_t endCount_;
	static volatile uint64_t lastOperationCount_;
};
#endif	

/*!
	@brief Address Socket for sending and receiving
*/



class SocketAddress {
public:
	static const int FAMILY_INET;
	static const int FAMILY_INET6;

	static const int FLAG_NOFQDN;
	static const int FLAG_NAMEREQD;
	static const int FLAG_NUMERICHOST;
	static const int FLAG_NUMERICSERV;

	static const SocketAddress INET_ANY;
	static const SocketAddress INET_BROADCAST;
	static const SocketAddress INET_LOOPBACK;

	static const SocketAddress INET6_ANY;
	static const SocketAddress INET6_LOOPBACK;

public:
	struct Inet {
		uint8_t value_[4];
	};

	struct Inet6 {
		uint8_t value_[16];
	};

public:
	
	SocketAddress();

	
	explicit SocketAddress(const char8_t *host, const char8_t *service = NULL,
			int family = 0, int sockType = 0);
	
	explicit SocketAddress(const char8_t *host, uint16_t port = 0,
			int family = 0, int sockType = 0);

	SocketAddress(const Inet &addr, uint16_t port);
	SocketAddress(const Inet6 &addr, uint16_t port);

	SocketAddress(const SocketAddress &another);
	SocketAddress& operator=(const SocketAddress &another);

	
	virtual ~SocketAddress();

public:
	static const SocketAddress& getAny(int family = 0);
	static const SocketAddress& getLoopback(int family = 0);

	static void getHostName(u8string &hostName);

	
	static SocketAddress getLocalHost(
			uint16_t port = 0, int family = 0, int sockType = 0);

	
	static void getAll(std::vector<SocketAddress> &addrList,
			const char8_t *host, const char8_t *service = NULL,
			int family = 0, int sockType = 0);

	
	static void getAll(std::vector<SocketAddress> &addrList,
			const char8_t *host, uint16_t port = 0,
			int family = 0, int sockType = 0);

	bool isAny() const;
	bool isLoopback() const;

	
	void assign(const char8_t *host, const char8_t *service = NULL,
			int family = 0, int sockType = 0);

	
	void assign(const char8_t *host, uint16_t port = 0,
			int family = 0, int sockType = 0);

	void assign(const Inet &addr, uint16_t port);

	void assign(const Inet6 &addr, uint16_t port);

	void clear();

	
	bool isEmpty(void) const;

	
	
	struct sockaddr* getAddress(struct sockaddr_storage *addr) const;

	
	
	
	size_t getSize(void) const;

	
	
	int getFamily(void) const;

	void getName(u8string *host, u8string *service = NULL,
			int flags = FLAG_NUMERICHOST | FLAG_NUMERICSERV) const;

	void getIP(u8string *host, uint16_t *port = NULL) const;

	
	void getIP(Inet *inAddr, uint16_t *port = NULL) const;

	
	void getIP(Inet6 *in6Addr, uint16_t *port = NULL) const;

	uint16_t getPort() const { return port_; }

	void setPort(uint16_t port) { port_ = port; }

private:
	friend class Socket;

	void assign(const struct addrinfo &info);

	void assign(const struct sockaddr *addr, size_t size);

	static struct addrinfo* getAddressInfo(
			const char8_t *host, const char8_t *service,
			int family, int sockType);

	static Inet toInet(uint32_t inAddr);
	static Inet6 toInet6(const struct in6_addr &in6Addr);

private:
	int family_;
	uint16_t port_;
	union {
		Inet inet_;
		Inet6 inet6_;
	} addr_;
};

std::ostream& operator<<(std::ostream &s, const SocketAddress &addr);
std::ostream& operator<<(std::ostream &s, const SocketAddress::Inet &addr);
std::ostream& operator<<(std::ostream &s, const SocketAddress::Inet6 &addr);

/*!
	@brief Waits network I/O using select system calls.
*/



class IOPollSelect : public IOPollBase {
public:
	IOPollSelect();
	virtual ~IOPollSelect();

	virtual void add(IOPollHandler *handler, IOPollEvent event);
	virtual void modify(IOPollHandler *handler, IOPollEvent event);
	virtual void remove(IOPollHandler *handler);

	virtual bool dispatch(uint32_t msec);
	virtual const char* getType(void) const;

private:
	typedef std::map<Socket::FD, IOPollHandler*> FDMap;
	typedef FDMap::iterator FDMapItr;
	typedef FDMap::const_iterator FDMapCItr;

private:
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

#ifdef UTIL_HAVE_EPOLL_CTL
/*!
	@brief Waits network I/O using epoll system calls.
*/



class IOPollEPoll : public IOPollBase {
public:
	IOPollEPoll();
	virtual ~IOPollEPoll();

public:
	virtual void add(IOPollHandler *handler, IOPollEvent event);
	virtual void modify(IOPollHandler *handler, IOPollEvent event);
	virtual void remove(IOPollHandler *handler);
	virtual bool dispatch(uint32_t msec);
	virtual const char* getType(void) const;

private:
private:
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};
#else
typedef IOPollSelect IOPollEPoll;
#endif 

#ifdef UTIL_HAVE_POLL
/*!
	@brief States IOPollPoll
*/



class IOPollPollCont {
public:
	IOPollPollCont();
	virtual ~IOPollPollCont();

	void add(int fd, IOPollHandler *file, IOPollEvent events);
	void remove(int fd);
	void modify(int fd, IOPollEvent events);
	void clear(void);

	inline struct pollfd* getPolls(void) {
		return vec_;
	}

	inline size_t getCurrentSize(void) const {
		return cur_;
	}

	IOPollHandler* getFile(int fd);
	const IOPollHandler* getFile(int fd) const;

private:
	void increase(void);

private:
	typedef struct FileDes {
		IOPollHandler *file;
		size_t index;

		inline FileDes(IOPollHandler *pf, size_t i) :
				file(pf), index(i) {
		}
	} FileDes;

	typedef std::map<int, FileDes> IndexCount;
	typedef IndexCount::iterator IndexItr;
	typedef IndexCount::const_iterator IndexCItr;

	IOPollPollCont(const IOPollPollCont&);
	IOPollPollCont& operator=(const IOPollPollCont&);

private:
	IndexCount index_;
	struct pollfd *vec_;
	size_t max_;
	size_t cur_;
};

/*!
	@brief Waits network I/O using poll system calls.
*/



class IOPollPoll : public IOPollBase {
public:
	IOPollPoll();
	virtual ~IOPollPoll();

public:
	virtual void add(IOPollHandler *handler, IOPollEvent event);
	virtual void modify(IOPollHandler *handler, IOPollEvent event);
	virtual void remove(IOPollHandler *handler);
	virtual bool dispatch(uint32_t msec);
	virtual const char* getType(void) const;

private:
	IOPollPollCont cont_;
};
#else
typedef IOPollSelect IOPollPoll;
#endif 

typedef IOPollEPoll IOPoll;

class Pipe : public IOPollHandler {
public:
	virtual ~Pipe();

	void handlePollEvent(IOPollBase *io, IOPollEvent event);

	File& getFile();

	static void create(Pipe *&writeTerm, Pipe *&readTerm);

	static const ssize_t NOTIFICATION_SIZE = 1;

	int32_t writeCount_;
	int32_t readCount_;

private:
	Pipe(util::File::FD fd);

	File file_;

	uint8_t readBuf[1];
};

} 

#endif
