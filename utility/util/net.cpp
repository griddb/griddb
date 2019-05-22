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

#ifndef FD_SETSIZE
#define FD_SETSIZE 64
#endif

#include "util/net.h"
#include "util/thread.h"
#include "util/os.h"
#include <sstream>
#include <map>
#include <cassert>

#ifdef _WIN32
#include "MSTcpIp.h"
#else
#include <sys/param.h>
#include <sys/eventfd.h>
#endif

#ifdef UTIL_HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifndef UTIL_IOPOLL_CHECKER
#define UTIL_IOPOLL_CHECKER 0
#endif

namespace util {


#ifdef _WIN32
const int IOPollEvent::TYPE_READ = 1 << 0;
const int IOPollEvent::TYPE_WRITE = 1 << 1;
const int IOPollEvent::TYPE_ERROR = 1 << 2;
#else
const int IOPollEvent::TYPE_READ = POLLIN;
const int IOPollEvent::TYPE_WRITE = POLLOUT;
const int IOPollEvent::TYPE_ERROR = POLLERR;
#endif
const int IOPollEvent::TYPE_READ_WRITE = (TYPE_READ | TYPE_WRITE);


IOPollHandler::~IOPollHandler() {
}


struct IOPollBase::InterruptionData : public IOPollHandler {

	InterruptionData();

	virtual ~InterruptionData();

	virtual void handlePollEvent(IOPollBase *io, IOPollEvent ev);

	virtual File& getFile();

	void interrupt();

	Atomic<bool> interrupting_;
	Socket socketPair[2];
};

IOPollBase::InterruptionData::InterruptionData() {
#ifdef _WIN32
	util::Socket listenerSocket;
	listenerSocket.open(
			util::SocketAddress::FAMILY_INET, util::Socket::TYPE_STREAM);
	listenerSocket.setReuseAddress(false);
	listenerSocket.bind(util::SocketAddress::INET_LOOPBACK);
	listenerSocket.listen(1);

	util::SocketAddress addr;
	listenerSocket.getSocketName(addr);

	socketPair[0].open(
			util::SocketAddress::FAMILY_INET, util::Socket::TYPE_STREAM);
	socketPair[0].connect(addr);

	if (!listenerSocket.accept(&socketPair[1])) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#else
	socketPair[1].attach(eventfd(EFD_NONBLOCK, 0));
#endif
}

IOPollBase::InterruptionData::~InterruptionData() {
}

void IOPollBase::InterruptionData::handlePollEvent(
		IOPollBase*, IOPollEvent ev) {
	if (ev & IOPollEvent::TYPE_READ) {
#ifdef _WIN32
		uint8_t buf[1];
		socketPair[1].receive(buf, sizeof(buf));
#else
		eventfd_t value;
		eventfd_read(socketPair[1].getHandle(), &value);
#endif

		interrupting_ = false;
	}
}

File& IOPollBase::InterruptionData::getFile() {
	return socketPair[1];
}

void IOPollBase::InterruptionData::interrupt() {
	bool expected = false;
	if (interrupting_.compareExchange(expected, true)) {
#ifdef _WIN32
		uint8_t buf[1] = { 0 };
		socketPair[0].send(buf, sizeof(buf));
#else
		const eventfd_t value = 1;
		eventfd_write(socketPair[1].getHandle(), value);
#endif
	}
}


const uint32_t IOPollBase::WAIT_INFINITY = static_cast<uint32_t>(-1);

IOPollBase::~IOPollBase() {
}

void IOPollBase::setInterruptible() {
	if (interruptionData_.get() != NULL) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

	UTIL_UNIQUE_PTR<InterruptionData> data(UTIL_NEW InterruptionData);
	add(data.get(), util::IOPollEvent::TYPE_READ);
	interruptionData_.reset(data.release());
}

void IOPollBase::interrupt() {
	if (interruptionData_.get() == NULL) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

	interruptionData_->interrupt();
}


#ifdef _WIN32
int32_t SocketLibrary::counter_ = 0;
#endif

SocketLibrary::SocketLibrary() {
#ifdef _WIN32
	if (counter_++ != 0) {
		return;
	}

	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		return;
	}

	do {
		if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
			return;
		}

		const wchar_t *const envName = L"SystemRoot";
		if (GetEnvironmentVariableW(envName, NULL, 0) == 0) {
			UINT length = GetWindowsDirectoryW(NULL, 0);
			if (length > 0) {
				std::vector<wchar_t> windowsDir;
				windowsDir.resize(length - 1);
				length = GetWindowsDirectoryW(&windowsDir[0], length);

				if (length > 0 &&
						SetEnvironmentVariableW(envName, &windowsDir[0]) == 0) {
					break;
				}
			}
			if (length == 0) {
				break;
			}
		}

		return;
	}
	while (false);
	WSACleanup();
#endif
}

SocketLibrary::~SocketLibrary() {
#ifdef _WIN32
	if (--counter_ != 0) {
		return;
	}

	WSACleanup();
#endif
}

void SocketLibrary::checkAvailability() {
#ifdef _WIN32
	if (counter_ <= 0) {
		UTIL_THROW_UTIL_ERROR(CODE_INVALID_STATUS,
				"Socket library is not available");
	}
#endif
}


const int Socket::TYPE_DATAGRAM = SOCK_DGRAM;
const int Socket::TYPE_STREAM = SOCK_STREAM;

const uint32_t Socket::DEFAULT_MAX_BACKLOG_SIZE = SOMAXCONN;

#ifdef _WIN32
const Socket::SocketHandle Socket::INVALID_SOCKET_HANDLE = INVALID_SOCKET;
#else
const Socket::SocketHandle Socket::INVALID_SOCKET_HANDLE = -1;
#endif

#ifdef _WIN32
const int Socket::ADDITIONAL_MESSAGE_FLAGS = 0;
#else
const int Socket::ADDITIONAL_MESSAGE_FLAGS = MSG_NOSIGNAL;
#endif

Socket::Socket(const char *user) : family_(0), user_(user) {
	SocketLibrary::checkAvailability();
}

Socket::~Socket() try {
	close();
}
catch (...) {
}

void Socket::close(void) {
	FD fd = detach();
	if (INITIAL_FD != fd) {
#ifndef _WIN32
		::close(fd);
#else
		::closesocket(toSocket(fd));
#endif
	}

	family_ = 0;
}

void Socket::open(int family, int type, int protocol) {
	const SocketHandle handle = socket(family, type, protocol);

	if (INVALID_SOCKET_HANDLE == handle) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	attach(toFD(handle));
	family_ = family;
}

void Socket::open(const char8_t*, FileFlag, FilePermission) {
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION, "");
}

SocketAddress& Socket::getSocketName(SocketAddress &addr) const {
	addr.clear();
	sockaddr_storage ss;

	socklen_t size = sizeof(ss);
	sockaddr *sa = reinterpret_cast<sockaddr*>(&ss);

	if (0 != getsockname(toSocket(fd_), sa, &size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	addr.assign(sa, size);
	return addr;
}

SocketAddress& Socket::getPeerName(SocketAddress &addr) const {
	addr.clear();
	sockaddr_storage ss;

	socklen_t size = sizeof(ss);
	sockaddr *sa = reinterpret_cast<sockaddr*>(&ss);

	if (0 != getpeername(toSocket(fd_), sa, &size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	addr.assign(sa, size);
	return addr;
}

void Socket::shutdown(bool forReceive, bool forSend) {
	int type;
	if (forReceive) {
#ifdef _WIN32
		type = forSend ? SD_BOTH : SD_RECEIVE;
#else
		type = forSend ? SHUT_RDWR : SHUT_RD;
#endif
	}
	else if (forSend) {
#ifdef _WIN32
		type = SD_SEND;
#else
		type = SHUT_WR;
#endif
	}
	else {
		return;
	}

	if (0 != ::shutdown(toSocket(fd_), type)) {
#ifdef _WIN32
		if (WSAGetLastError() == WSAENOTCONN) {
			return;
		}
#else
		if (errno == ENOTCONN) {
			return;
		}
#endif
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

void Socket::bind(const SocketAddress &addr) {
	struct sockaddr_storage ss;
	if (0 != ::bind( toSocket(fd_),
			addr.getAddress(&ss), static_cast<socklen_t>(addr.getSize()) )) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

void Socket::listen(uint32_t backlogSize) {
	if (0 != ::listen(toSocket(fd_), static_cast<int>(backlogSize))) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

bool Socket::accept(Socket *socket, SocketAddress *addr) const {
	if (addr != NULL) {
		addr->clear();
	}
	sockaddr_storage ss;

	socklen_t size = sizeof(ss);
	sockaddr *sa = reinterpret_cast<sockaddr*>(&ss);

	const SocketHandle handle = ::accept(toSocket(fd_), sa, &size);

	if (INVALID_SOCKET_HANDLE == handle) {
		if (isBlockable()) {
			return false;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	socket->attach(toFD(handle));
	if (addr != NULL) {
		addr->assign(sa, size);
	}

	return true;
}

bool Socket::connect(const SocketAddress &addr) {
#if UTIL_FAILURE_SIMULATION_ENABLED
	SocketFailureSimulator::checkOperation(*this, 0);
#endif

	struct sockaddr_storage ss;
	if (0 != ::connect( toSocket(fd_), addr.getAddress(&ss),
			static_cast<socklen_t>(addr.getSize()) )) {
#ifdef _WIN32
		switch (WSAGetLastError()) {
		case WSAEALREADY:
		case WSAEINPROGRESS:
		case WSAEWOULDBLOCK:
			return false;
		}
#else
		if (errno == EINPROGRESS) {
			return false;
		}
#endif
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return true;
}

ssize_t Socket::receive(void *buf, size_t len, int flags) {
#if UTIL_FAILURE_SIMULATION_ENABLED
	SocketFailureSimulator::checkOperation(*this, 0);
#endif

	const ssize_t result = recv(toSocket(fd_),
			static_cast<char*>(buf), static_cast<int>(len),
			flags | ADDITIONAL_MESSAGE_FLAGS);
	if (result < 0) {
		if (isBlockable()) {
			return -1;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return result;
}

ssize_t Socket::receiveFrom(void *buf, size_t len,
		SocketAddress *addr, int flags) {
#if UTIL_FAILURE_SIMULATION_ENABLED
	SocketFailureSimulator::checkOperation(*this, 0);
#endif

	sockaddr_storage ss;

	socklen_t size = sizeof(ss);
	sockaddr *sa = reinterpret_cast<sockaddr*>(&ss);

	const ssize_t result = ::recvfrom(toSocket(fd_),
			static_cast<char*>(buf), static_cast<int>(len),
			flags | ADDITIONAL_MESSAGE_FLAGS, sa, &size);
	if (result < 0) {
		if (isBlockable()) {
			return -1;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (NULL != addr) {
		addr->assign(sa, size);
	}

	return result;
}

ssize_t Socket::send(const void *buf, size_t len, int flags) {
#if UTIL_FAILURE_SIMULATION_ENABLED
	SocketFailureSimulator::checkOperation(*this, 0);
#endif

	const ssize_t result = ::send(toSocket(fd_),
			static_cast<const char*>(buf), static_cast<int>(len),
			flags | ADDITIONAL_MESSAGE_FLAGS);
	if (result < 0) {
		if (isBlockable()) {
			return -1;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return result;
}

ssize_t Socket::sendTo(const void *buf, size_t len,
		const SocketAddress &addr, int flags) {
#if UTIL_FAILURE_SIMULATION_ENABLED
	SocketFailureSimulator::checkOperation(*this, 0);
#endif

	struct sockaddr_storage ss;
	const ssize_t result = ::sendto(toSocket(fd_),
			static_cast<const char*>(buf), static_cast<int>(len),
			flags | ADDITIONAL_MESSAGE_FLAGS,
			addr.getAddress(&ss), static_cast<socklen_t>(addr.getSize()));
	if (result < 0) {
		if (isBlockable()) {
			return -1;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return result;
}

ssize_t Socket::read(void *buf, size_t len) {
	return receive(buf, len);
}

ssize_t Socket::write(const void *buf, size_t len) {
	return send(buf, len);
}

ssize_t Socket::read(void*, size_t, off_t) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

ssize_t Socket::write(const void*, size_t, off_t) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

void Socket::read(IOOperation&) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

void Socket::write(IOOperation&) {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

off_t Socket::tell() {
	UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
}

void Socket::getOption(int level, int name, void *value, size_t len) const {
	socklen_t resultLen = static_cast<socklen_t>(len);

	if (0 != getsockopt(toSocket(fd_), level, name,
			static_cast<char*>(value), &resultLen)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (static_cast<size_t>(resultLen) != len) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}
}

void Socket::setOption(int level, int name, const void *value, size_t len) {
	if (0 != setsockopt(toSocket(fd_), level, name,
			static_cast<const char*>(value), static_cast<socklen_t>(len))) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

int32_t Socket::getInt32Option(int level, int name) const {
	int32_t value;
	getOption(level, name, &value, sizeof(value));
	return value;
}

void Socket::setInt32Option(int level, int name, int32_t value) {
	setOption(level, name, &value, sizeof(value));
}

int64_t Socket::getInt64Option(int level, int name) const {
	int64_t value;
	getOption(level, name, &value, sizeof(value));
	return value;
}

void Socket::setInt64Option(int level, int name, int64_t value) {
	setOption(level, name, &value, sizeof(value));
}

bool Socket::getBoolOption(int level, int name) const {
	return (getInt32Option(level, name) != 0);
}

void Socket::setBoolOption(int level, int name, bool value) {
	setInt32Option(level, name, value);
}

bool Socket::getReuseAddress(void) const {
	return getBoolOption(SOL_SOCKET, SO_REUSEADDR);
}

void Socket::setReuseAddress(bool value) {
	setBoolOption(SOL_SOCKET, SO_REUSEADDR, value);
}

bool Socket::getKeepAlive(void) const {
	return getBoolOption(SOL_SOCKET, SO_KEEPALIVE);
}

void Socket::setKeepAlive(bool value) {
	setBoolOption(SOL_SOCKET, SO_KEEPALIVE, value);
}

void Socket::setKeepAlive(
		int32_t idleSeconds, int32_t intervalSeconds, int32_t retryCount) {
#ifdef _WIN32
	(void) retryCount;

	tcp_keepalive keepAlive;
	keepAlive.onoff = 1;
	keepAlive.keepalivetime = static_cast<uint32_t>(idleSeconds);
	keepAlive.keepaliveinterval = static_cast<uint32_t>(intervalSeconds);

	DWORD resultSize;
	if (WSAIoctl(toSocket(fd_), SIO_KEEPALIVE_VALS,
			&keepAlive, sizeof(keepAlive), NULL, 0, &resultSize, NULL, NULL) != 0) {
		UTIL_THROW_PLATFORM_ERROR("");
	}
#else
	setKeepAlive(true);
	setInt32Option(IPPROTO_TCP, TCP_KEEPIDLE, idleSeconds);
	setInt32Option(IPPROTO_TCP, TCP_KEEPINTVL, intervalSeconds);
	setInt32Option(IPPROTO_TCP, TCP_KEEPCNT, retryCount);
#endif
}

int32_t Socket::getReceiveBufferSize(void) const {
	return getInt32Option(SOL_SOCKET, SO_RCVBUF);
}

void Socket::setReceiveBufferSize(int32_t value) {
	setInt32Option(SOL_SOCKET, SO_RCVBUF, value);
}

int32_t Socket::getSendBufferSize(void) const {
	return getInt32Option(SOL_SOCKET, SO_SNDBUF);
}

void Socket::setSendBufferSize(int32_t value) {
	setInt32Option(SOL_SOCKET, SO_SNDBUF, value);
}

uint32_t Socket::getReceiveTimeout(void) const {
#ifdef _WIN32
	uint32_t value;
	getOption(SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
	return value;
#else
	timeval tv;
	getOption(SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	return static_cast<uint32_t>(FileLib::getUnixTime(tv));
#endif
}

void Socket::setReceiveTimeout(uint32_t value) {
#ifdef _WIN32
	setOption(SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
#else
	const timeval tv = FileLib::getTimeval(value);
	setOption(SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}

uint32_t Socket::getSendTimeout(void) const {
#ifdef _WIN32
	uint32_t value;
	getOption(SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
	return value;
#else
	timeval tv;
	getOption(SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	return static_cast<uint32_t>(FileLib::getUnixTime(tv));
#endif
}

void Socket::setSendTimeout(uint32_t value) {
#ifdef _WIN32
	setOption(SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#else
	const timeval tv = FileLib::getTimeval(value);
	setOption(SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
}

bool Socket::getNoDelay(void) const {
	return getBoolOption(IPPROTO_TCP, TCP_NODELAY);
}

void Socket::setNoDelay(bool value) {
	setBoolOption(IPPROTO_TCP, TCP_NODELAY, value);
}

bool Socket::getLinger(int32_t *timeoutSecs) {
	linger lingerValue;
	getOption(SOL_SOCKET, SO_LINGER, &lingerValue, sizeof(lingerValue));
	if (timeoutSecs != NULL) {
		*timeoutSecs = lingerValue.l_linger;
	}
	return (lingerValue.l_onoff != 0);
}

void Socket::setLinger(bool enabled, int32_t timeoutSecs) {
	linger lingerValue;

	lingerValue.l_onoff = (enabled ? 1 : 0);
#ifdef _WIN32
	lingerValue.l_linger = static_cast<u_short>(timeoutSecs);
#else
	lingerValue.l_linger = timeoutSecs;
#endif
	setOption(SOL_SOCKET, SO_LINGER, &lingerValue, sizeof(lingerValue));
}

int32_t Socket::getMulticastTTL() {
	switch (family_) {
	case AF_INET:
		return getInt32Option(IPPROTO_IP, IP_MULTICAST_TTL);
	case AF_INET6:
		return getInt32Option(IPPROTO_IPV6, IPV6_MULTICAST_HOPS);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
}

void Socket::setMulticastTTL(int32_t value) {
	switch (family_) {
	case AF_INET:
		setInt32Option(IPPROTO_IP, IP_MULTICAST_TTL, value);
		break;
	case AF_INET6:
		setInt32Option(IPPROTO_IPV6, IPV6_MULTICAST_HOPS, value);
		break;
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
}

bool Socket::getMulticastLoopback() {
	int optionLevel;
	int optionName;
	switch (family_) {
	case AF_INET:
		optionLevel = IPPROTO_IP;
		optionName = IP_MULTICAST_LOOP;
		break;
	case AF_INET6:
		optionLevel = IPPROTO_IPV6;
		optionName = IPV6_MULTICAST_LOOP;
		break;
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

#ifdef _WIN32
	u_char value;
	getOption(optionLevel, optionName, &value, sizeof(value));
	return (value != 0);
#else
	return getBoolOption(optionLevel, optionName);
#endif
}

void Socket::setMulticastLoopback(bool value) {
	int optionLevel;
	int optionName;
	switch (family_) {
	case AF_INET:
		optionLevel = IPPROTO_IP;
		optionName = IP_MULTICAST_LOOP;
		break;
	case AF_INET6:
		optionLevel = IPPROTO_IPV6;
		optionName = IPV6_MULTICAST_LOOP;
		break;
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

#ifdef _WIN32
	const u_char charValue = (value ? 1 : 0);
	setOption(optionLevel, optionName, &charValue, sizeof(charValue));
#else
	setBoolOption(optionLevel, optionName, value);
#endif
}

void Socket::setBlockingMode(bool value) {
#ifdef _WIN32
	u_long val = (value ? 0 : 1);
	if (::ioctlsocket(toSocket(fd_), FIONBIO, &val) == SOCKET_ERROR) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	File::setBlockingMode(value);
#endif
}

bool Socket::isBlockable() const {
#ifdef _WIN32
	return (WSAGetLastError() == WSAEWOULDBLOCK);
#else
	return (errno == EAGAIN || errno == EWOULDBLOCK);
#endif
}

void Socket::joinMulticastGroup(
		const SocketAddress &multicastAddr,
		const SocketAddress *interfaceAddr) {
	setMulticastInterfaceOption(true, multicastAddr, interfaceAddr);
}

void Socket::leaveMulticastGroup(
		const SocketAddress &multicastAddr,
		const SocketAddress *interfaceAddr) {
	setMulticastInterfaceOption(false, multicastAddr, interfaceAddr);
}

void Socket::setMulticastInterfaceOption(
		bool join,
		const SocketAddress &multicastAddr,
		const SocketAddress *interfaceAddr) {
	if (family_ != multicastAddr.getFamily()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}
	if (interfaceAddr != NULL && !interfaceAddr->isEmpty() &&
			family_ != interfaceAddr->getFamily()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

#ifndef _WIN32
	if (join) {
		int value = 0;
		setOption(IPPROTO_IP, IP_MULTICAST_ALL, &value, sizeof(value));
	}
#endif

	struct sockaddr_storage ss;
	switch (multicastAddr.getFamily()) {
	case AF_INET: {
		if (interfaceAddr == NULL || interfaceAddr->isEmpty()) {
			interfaceAddr = &SocketAddress::INET_ANY;
		}
		ip_mreq value;
		value.imr_multiaddr = reinterpret_cast<sockaddr_in*>(
				multicastAddr.getAddress(&ss))->sin_addr;
		value.imr_interface = reinterpret_cast<sockaddr_in*>(
				interfaceAddr->getAddress(&ss))->sin_addr;
		setOption(IPPROTO_IP,
				(join ? IP_ADD_MEMBERSHIP : IP_DROP_MEMBERSHIP),
				&value, sizeof(value));
		break;
	}
	case AF_INET6: {
		if (interfaceAddr != NULL || interfaceAddr->isEmpty()) {
			UTIL_THROW_NOIMPL_UTIL();
		}
		ipv6_mreq value;
		value.ipv6mr_multiaddr = reinterpret_cast<sockaddr_in6*>(
				multicastAddr.getAddress(&ss))->sin6_addr;
		value.ipv6mr_interface = 0;
		setOption(IPPROTO_IPV6,
				(join ? IPV6_ADD_MEMBERSHIP : IPV6_DROP_MEMBERSHIP),
				&value, sizeof(value));
		break;
	}
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}
}

void Socket::handlePollEvent(IOPollBase*, IOPollEvent) {
}

File& Socket::getFile() {
	return *this;
}

const char* Socket::getUser() const {
	return user_;
}

bool Socket::isTimeoutError(int32_t errorCode) {
#ifdef _WIN32
	return (errorCode == WSAETIMEDOUT || errorCode == WSAEWOULDBLOCK);
#else
	return (errorCode == ETIMEDOUT || errorCode == EAGAIN ||
			errorCode == EWOULDBLOCK);
#endif
}

#if UTIL_FAILURE_SIMULATION_ENABLED
volatile bool SocketFailureSimulator::enabled_ = false;
volatile SocketFailureSimulator::FilterHandler
		SocketFailureSimulator::handler_ = NULL;
volatile int32_t SocketFailureSimulator::targetType_ = 0;
volatile uint64_t SocketFailureSimulator::startCount_ = 0;
volatile uint64_t SocketFailureSimulator::endCount_ = 0;
volatile uint64_t SocketFailureSimulator::lastOperationCount_ = 0;

void SocketFailureSimulator::set(
		int32_t targetType, uint64_t startCount, uint64_t endCount,
		FilterHandler handler) {
	if (startCount < endCount) {
		if (handler == NULL) {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
		}
		enabled_ = false;
		handler_ = handler;
		targetType_ = targetType;
		startCount_ = startCount;
		endCount_ = endCount;
		lastOperationCount_ = 0;
		enabled_ = true;
	}
	else {
		enabled_ = false;
		handler_ = NULL;
		targetType_ = 0;
		startCount_ = 0;
		endCount_ = 0;
	}
}

void SocketFailureSimulator::checkOperation(
		const Socket &socket, int operationType) {
	FilterHandler handler = handler_;
	if (enabled_ && handler != NULL &&
			(*handler)(socket, targetType_, operationType)) {
		const uint64_t count = lastOperationCount_;
		lastOperationCount_++;

		if (startCount_ <= count && count < endCount_) {
#ifdef _WIN32
			SetLastError(static_cast<DWORD>(-1));
#else
			errno = -1;
#endif
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
}

#endif	


const int SocketAddress::FAMILY_INET = AF_INET;
const int SocketAddress::FAMILY_INET6 = AF_INET6;

const int SocketAddress::FLAG_NOFQDN = NI_NOFQDN;
const int SocketAddress::FLAG_NAMEREQD = NI_NAMEREQD;
const int SocketAddress::FLAG_NUMERICHOST = NI_NUMERICHOST;
const int SocketAddress::FLAG_NUMERICSERV = NI_NUMERICSERV;

const SocketAddress SocketAddress::INET_ANY(
		toInet(static_cast<uint32_t>(INADDR_ANY)), 0);
const SocketAddress SocketAddress::INET_BROADCAST(
		toInet(static_cast<uint32_t>(INADDR_BROADCAST)), 0);
const SocketAddress SocketAddress::INET_LOOPBACK(
		toInet(static_cast<uint32_t>(INADDR_LOOPBACK)), 0);

const SocketAddress SocketAddress::INET6_ANY(toInet6(in6addr_any), 0);
const SocketAddress SocketAddress::INET6_LOOPBACK(
		toInet6(in6addr_loopback), 0);

SocketAddress::SocketAddress() {
	clear();
}

SocketAddress::SocketAddress(
		const char8_t *host, const char8_t *service,
		int family, int sockType) {
	assign(host, service, family, sockType);
}

SocketAddress::SocketAddress(
		const char8_t *host, uint16_t port,
		int family, int sockType) {
	assign(host, port, family, sockType);
}

SocketAddress::SocketAddress(const Inet &addr, uint16_t port) {
	assign(addr, port);
}

SocketAddress::SocketAddress(const Inet6 &addr, uint16_t port) {
	assign(addr, port);
}

SocketAddress::SocketAddress(const SocketAddress &another) {
	family_ = another.family_;
	port_ = another.port_;
	addr_ = another.addr_;
}

SocketAddress& SocketAddress::operator=(const SocketAddress &another) {
	family_ = another.family_;
	port_ = another.port_;
	addr_ = another.addr_;
	return *this;
}

SocketAddress::~SocketAddress() {
}

const SocketAddress& SocketAddress::getAny(int family) {
	if (family == FAMILY_INET6) {
		return INET6_ANY;
	}
	else {
		return INET_ANY;
	}
}

const SocketAddress& SocketAddress::getLoopback(int family) {
	if (family == FAMILY_INET6) {
		return INET6_LOOPBACK;
	}
	else {
		return INET_LOOPBACK;
	}
}

void SocketAddress::getHostName(u8string &hostName) {
#ifdef _WIN32
	const int MAXHOSTNAMELEN = 256;
#endif

	char8_t hostNameChars[MAXHOSTNAMELEN];
	if (gethostname(hostNameChars, MAXHOSTNAMELEN) != 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	hostName = hostNameChars;
}

SocketAddress SocketAddress::getLocalHost(
		uint16_t port, int family, int sockType) {
	u8string hostName;
	getHostName(hostName);

	SocketAddress address;
	address.assign(hostName.c_str(), port, family, sockType);

	return address;
}

void SocketAddress::getAll(std::vector<SocketAddress> &addrList,
		const char8_t *host, const char8_t *service,
		int family, int sockType) {
	addrinfo *topInfo = getAddressInfo(host, service, family, sockType);
	try {
		size_t count = 1;
		for (addrinfo *info = topInfo; (info = info->ai_next) != NULL;) {
			count++;
		}
		addrList.resize(count);
		std::vector<SocketAddress>::iterator itr = addrList.begin();
		for (addrinfo *info = topInfo; info != NULL; info = info->ai_next) {
			itr->assign(*info);
			++itr;
		}
		freeaddrinfo(topInfo);
	}
	catch (...) {
		addrList.clear();
		freeaddrinfo(topInfo);
		throw;
	}
}

void SocketAddress::getAll(std::vector<SocketAddress> &addrList,
		const char8_t *host, uint16_t port,
		int family, int sockType) {
	NormalOStringStream os;
	os << port;
	getAll(addrList, host, os.str().c_str(), family, sockType);
}

bool SocketAddress::isAny() const {
	switch (family_) {
	case FAMILY_INET:
		return memcmp(
				&addr_.inet_, &INET_ANY.addr_.inet_, sizeof(Inet)) == 0;
	case FAMILY_INET6:
		return memcmp(
				&addr_.inet6_, &INET_ANY.addr_.inet6_, sizeof(Inet6)) == 0;
	default:
		return false;
	}
}

bool SocketAddress::isLoopback() const {
	switch (family_) {
	case FAMILY_INET:
		return memcmp(
				&addr_.inet_, &INET_LOOPBACK.addr_.inet_, sizeof(Inet)) == 0;
	case FAMILY_INET6:
		return memcmp(
				&addr_.inet6_, &INET6_LOOPBACK.addr_.inet6_, sizeof(Inet6)) == 0;
	default:
		return false;
	}
}

void SocketAddress::assign(
		const char8_t *host, const char8_t *service,
		int family, int sockType) {
	addrinfo *info = getAddressInfo(host, service, family, sockType);
	try {
		assign(*info);
		freeaddrinfo(info);
	}
	catch (...) {
		freeaddrinfo(info);
		throw;
	}
}

void SocketAddress::assign(
		const char8_t *host, uint16_t port, int family, int sockType) {
	NormalOStringStream os;
	os << port;
	assign(host, os.str().c_str(), family, sockType);
}

void SocketAddress::assign(const Inet &addr, uint16_t port) {
	family_ = FAMILY_INET;
	port_ = port;
	addr_.inet_ = addr;
}

void SocketAddress::assign(const Inet6 &addr, uint16_t port) {
	family_ = FAMILY_INET6;
	port_ = port;
	addr_.inet6_ = addr;
}

void SocketAddress::assign(const addrinfo &info) {
	assign(info.ai_addr, info.ai_addrlen);
}

void SocketAddress::assign(const sockaddr *addr, size_t size) {
	switch (addr->sa_family) {
	case AF_INET: {
		if (size != sizeof(sockaddr_in)) {
			UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
					"Illegal address size");
		}
		const sockaddr_in *inAddr = reinterpret_cast<const sockaddr_in*>(addr);
		family_ = FAMILY_INET;
		port_ = ntohs(inAddr->sin_port);
		memcpy(&addr_.inet_.value_,
				&inAddr->sin_addr, sizeof(addr_.inet_.value_));
		break;
	}
	case AF_INET6: {
		if (size != sizeof(sockaddr_in6)) {
			UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT,
					"Illegal address size");
		}
		family_ = FAMILY_INET6;
		const sockaddr_in6 *in6Addr =
				reinterpret_cast<const sockaddr_in6*>(addr);
		port_ = ntohs(in6Addr->sin6_port);
		memcpy(&addr_.inet6_.value_,
				&in6Addr->sin6_addr, sizeof(addr_.inet6_.value_));
		break;
	}
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
}

void SocketAddress::clear() {
	family_ = 0;
	port_ = 0;
	memset(&addr_, 0, sizeof(addr_));
}

bool SocketAddress::isEmpty(void) const {
	return (family_ == 0);
}

sockaddr* SocketAddress::getAddress(sockaddr_storage *addr) const {
	switch (family_) {
	case FAMILY_INET: {
		sockaddr_in *inAddr = reinterpret_cast<sockaddr_in*>(addr);
		inAddr->sin_family = AF_INET;
		inAddr->sin_port = htons(port_);
		memcpy(&inAddr->sin_addr,
				&addr_.inet_.value_, sizeof(addr_.inet_.value_));
		return reinterpret_cast<sockaddr*>(inAddr);
	}
	case FAMILY_INET6: {
		sockaddr_in6 *in6Addr = reinterpret_cast<sockaddr_in6*>(addr);
		in6Addr->sin6_family = AF_INET6;
		in6Addr->sin6_port = htons(port_);
		in6Addr->sin6_flowinfo = 0;
		in6Addr->sin6_scope_id = 0;
		memcpy(&in6Addr->sin6_addr,
				&addr_.inet6_.value_, sizeof(addr_.inet6_.value_));
		return reinterpret_cast<sockaddr*>(in6Addr);
	}
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
}

size_t SocketAddress::getSize(void) const {
	switch (family_) {
	case FAMILY_INET:
		return sizeof(sockaddr_in);
	case FAMILY_INET6:
		return sizeof(sockaddr_in6);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}
}

int SocketAddress::getFamily(void) const {
	return family_;
}

void SocketAddress::getName(
		u8string *host, u8string *service, int flags) const {
	SocketLibrary::checkAvailability();

	char hostChars[256];
	char serviceChars[16];

	sockaddr_storage addr;
	const int ret = getnameinfo(
			getAddress(&addr), static_cast<socklen_t>(getSize()),
			hostChars, sizeof(hostChars),
			serviceChars, sizeof(serviceChars), flags);
	if (0 != ret) {
#ifndef _WIN32
		if (EAI_SYSTEM != ret) {
			UTIL_THROW_PLATFORM_ERROR_WITH_CODE(
					TYPE_ADDRINFO_LINUX, ret, NULL);
		}
#endif
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (host) {
		*host = hostChars;
	}

	if (service) {
		*service = serviceChars;
	}
}

void SocketAddress::getIP(u8string *host, uint16_t *port) const {
	u8string service;
	getName(host, &service, FLAG_NUMERICHOST | FLAG_NUMERICSERV);

	if (port) {
		*port = static_cast<uint16_t>(atoi(service.c_str()));
	}
}

void SocketAddress::getIP(Inet *inAddr, uint16_t *port) const {
	if (family_ != FAMILY_INET) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

	*inAddr = addr_.inet_;
	if (port) {
		*port = port_;
	}
}

void SocketAddress::getIP(Inet6 *in6Addr, uint16_t *port) const {
	if (family_ != FAMILY_INET6) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

	*in6Addr = addr_.inet6_;
	if (port) {
		*port = port_;
	}
}

int32_t SocketAddress::compare(const SocketAddress &another) const {
	const int family = getFamily();

	if (family != another.getFamily()) {
		return family - another.getFamily();
	}

	int32_t addrComp;

	uint16_t port;
	uint16_t anotherPort;

	if (family == util::SocketAddress::FAMILY_INET) {
		util::SocketAddress::Inet addr;
		util::SocketAddress::Inet anotherAddr;
		getIP(&addr, &port);
		another.getIP(&anotherAddr, &anotherPort);

		addrComp = memcmp(&addr, &anotherAddr, sizeof(addr));
	}
	else if (family == util::SocketAddress::FAMILY_INET6) {
		util::SocketAddress::Inet6 addr;
		util::SocketAddress::Inet6 anotherAddr;
		getIP(&addr, &port);
		another.getIP(&anotherAddr, &anotherPort);

		addrComp = memcmp(&addr, &anotherAddr, sizeof(addr));
	}
	else {
		port = 0;
		anotherPort = 0;
		addrComp = 0;
	}

	if (addrComp != 0) {
		return addrComp;
	}

	return static_cast<int32_t>(port) - static_cast<int32_t>(anotherPort);
}

addrinfo* SocketAddress::getAddressInfo(
		const char8_t *host, const char8_t *service,
		int family, int sockType) {
	SocketLibrary::checkAvailability();

	addrinfo hints, *result;
	hints.ai_flags = 0;
	hints.ai_family = family;
	hints.ai_socktype = sockType;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_canonname = NULL;
	hints.ai_addr = NULL;
	hints.ai_next = NULL;

	if (host == NULL) {
		hints.ai_flags |= AI_PASSIVE;
	}

	const int ret = getaddrinfo(host, service, &hints, &result);
	if (0 != ret) {
#ifndef _WIN32
		if (EAI_SYSTEM != ret) {
			UTIL_THROW_PLATFORM_ERROR_WITH_CODE( \
					TYPE_ADDRINFO_LINUX, ret, NULL);
		}
#endif
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	return result;
}

SocketAddress::Inet SocketAddress::toInet(uint32_t inAddr) {
	Inet addr;
	convertTypeDirect(addr, htonl(inAddr));
	return addr;
}

SocketAddress::Inet6 SocketAddress::toInet6(const struct in6_addr &in6Addr) {
	Inet6 addr;
	convertTypeDirect(addr, in6Addr);
	return addr;
}

template<typename D, typename S>
void SocketAddress::convertTypeDirect(D &dest, const S &src) {
	UTIL_STATIC_ASSERT(sizeof(dest) == sizeof(src));
	memcpy(&dest, &src, sizeof(src));
}

std::ostream& operator<<(std::ostream &s, const SocketAddress &addr) {
	LocaleUtils::CLocaleScope localeScope(s);

	switch (addr.getFamily()) {
	case SocketAddress::FAMILY_INET: {
		SocketAddress::Inet inAddr;
		uint16_t port;
		addr.getIP(&inAddr, &port);
		return s << inAddr << ":" << port;
	}
	case SocketAddress::FAMILY_INET6:
		SocketAddress::Inet6 in6Addr;
		uint16_t port;
		addr.getIP(&in6Addr, &port);
		return s << "[" << in6Addr << "]:" << port;
	default:
		s << "(empty address)";
		return s;
	}
}

std::ostream& operator<<(std::ostream &s, const SocketAddress::Inet &addr) {
	LocaleUtils::CLocaleScope localeScope(s);

	for (size_t i = 0; i < sizeof(addr.value_); ++i) {
		if (i > 0) {
			s << ".";
		}
		s << static_cast<uint32_t>(addr.value_[i]);
	}
	return s;
}

std::ostream& operator<<(std::ostream &s, const SocketAddress::Inet6 &addr) {
	LocaleUtils::CLocaleScope localeScope(s);

	try {
		const SocketAddress sockAddr(addr, 0);
		u8string host;
		sockAddr.getName(&host);

		s << host;
	}
	catch (...) {
		s.setstate(std::ios::failbit);
		if (s.exceptions() | std::ios::failbit) {
			throw;
		}
	}

	return s;
}


struct IOPollSelect::Data {
	FDMap fds_;
	fd_set rset_;
	fd_set wset_;
	fd_set eset_;

#if UTIL_IOPOLL_CHECKER
	util::Atomic<int64_t> checker_;
	struct ChekerScope {
		ChekerScope(Data &data, bool entering) : data_(data), entering_(entering) {
			data.checkEntering(entering);
		}

		~ChekerScope() {
			data_.checkEntering(!entering_);
		}

		Data &data_;
		bool entering_;
	};

	void checkEntering(bool entering) {
		if (entering) {
			if (++checker_ != 1) {
				util::DebugUtils::interrupt();
				abort();
			}
		}
		else {
			if (--checker_ != 0) {
				util::DebugUtils::interrupt();
				abort();
			}
		}
	}
#endif
};

IOPollSelect::IOPollSelect() : data_(new Data()) {
	FD_ZERO(&data_->rset_);
	FD_ZERO(&data_->wset_);
	FD_ZERO(&data_->eset_);
}

IOPollSelect::~IOPollSelect() {
}

void IOPollSelect::add(IOPollHandler *handler, IOPollEvent event) {
#if UTIL_IOPOLL_CHECKER
	Data::ChekerScope checkerScope(*data_, true);
#endif
	const Socket::FD fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);
	assert(data_->fds_.find(fd) == data_->fds_.end());

	if (data_->fds_.size() >= FD_SETSIZE) {
		UTIL_THROW_UTIL_ERROR(CODE_SIZE_LIMIT_EXCEEDED,
				"Too many sockets are opened (limitPerIOSelector=" <<
				FD_SETSIZE << ")");
	}

	if (event & IOPollEvent::TYPE_READ)
		FD_SET(Socket::toSocket(fd), &data_->rset_);
	if (event & IOPollEvent::TYPE_WRITE)
		FD_SET(Socket::toSocket(fd), &data_->wset_);
	if (event & IOPollEvent::TYPE_ERROR)
		FD_SET(Socket::toSocket(fd), &data_->eset_);

	data_->fds_.insert(FDMap::value_type(fd, handler));
}

void IOPollSelect::modify(IOPollHandler *handler, IOPollEvent event) {
#if UTIL_IOPOLL_CHECKER
	Data::ChekerScope checkerScope(*data_, true);
#endif
	const Socket::FD fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);
	assert(data_->fds_.find(fd) != data_->fds_.end());

	if (event & IOPollEvent::TYPE_READ)
		FD_SET(Socket::toSocket(fd), &data_->rset_);
	else
		FD_CLR(Socket::toSocket(fd), &data_->rset_);

	if (event & IOPollEvent::TYPE_WRITE)
		FD_SET(Socket::toSocket(fd), &data_->wset_);
	else
		FD_CLR(Socket::toSocket(fd), &data_->wset_);

	if (event & IOPollEvent::TYPE_ERROR)
		FD_SET(Socket::toSocket(fd), &data_->eset_);
	else
		FD_CLR(Socket::toSocket(fd), &data_->eset_);
}

void IOPollSelect::remove(IOPollHandler *handler) {
#if UTIL_IOPOLL_CHECKER
	Data::ChekerScope checkerScope(*data_, true);
#endif
	const Socket::FD fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);
	assert(data_->fds_.find(fd) != data_->fds_.end());

	FD_CLR(Socket::toSocket(fd), &data_->rset_);
	FD_CLR(Socket::toSocket(fd), &data_->wset_);
	FD_CLR(Socket::toSocket(fd), &data_->eset_);

	data_->fds_.erase(fd);
}

bool IOPollSelect::dispatch(uint32_t msec) {
#if UTIL_IOPOLL_CHECKER
	Data::ChekerScope checkerScope(*data_, true);
#endif
	if (data_->fds_.empty()) {
		util::Thread::sleep(msec);
		return false;
	}

	fd_set rset = data_->rset_;
	fd_set wset = data_->wset_;
	fd_set eset = data_->eset_;

	struct timeval time, *optTime;
	if (static_cast<int32_t>(msec) >= 0) {
		time.tv_sec = static_cast<int32_t>(msec / 1000);
		time.tv_usec = (static_cast<int32_t>(msec % 1000)) * 1000;
		optTime = &time;
	}
	else {
		optTime = NULL;
	}

	const int res = select(FD_SETSIZE, &rset, &wset, &eset, optTime);

	if (-1 == res && errno != EAGAIN) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	else if (0 == res) {
		return false;
	}

	typedef std::pair<int, IOPollHandler*> EventEntry;
	EventEntry entryList[FD_SETSIZE];
	EventEntry *entryEnd = entryList;

	FDMapCItr endItr = data_->fds_.end();
	for (FDMapCItr i = data_->fds_.begin(); i != endItr; ++i) {
		assert(entryEnd != entryList + FD_SETSIZE);

		const Socket::FD fd = i->first;
		int eventFlags = 0;

		if (FD_ISSET(fd, &rset))
			eventFlags |= IOPollEvent::TYPE_READ;
		if (FD_ISSET(fd, &wset))
			eventFlags |= IOPollEvent::TYPE_WRITE;
		if (FD_ISSET(fd, &eset))
			eventFlags |= IOPollEvent::TYPE_ERROR;

		if (eventFlags != 0) {
			*entryEnd = EventEntry(eventFlags, i->second);
			++entryEnd;
		}
	}

	for (EventEntry *i = entryList; i != entryEnd; ++i) {
#if UTIL_IOPOLL_CHECKER
		Data::ChekerScope checkerScope(*data_, false);
#endif
		i->second->handlePollEvent(this, i->first);
	}
	return (res > 0);
}

const char* IOPollSelect::getType(void) const {
	static const char type[] = "select";
	return type;
}

#ifdef UTIL_HAVE_EPOLL_CTL


const int UTIL_EPOLL_SIZE = 10240;
const int UTIL_EPOLL_DISPATCH_SIZE = 1024;

struct IOPollEPoll::Data {
	Data() : epollFD_(-1) {}

	int epollFD_;
	epoll_event epollEvents_[UTIL_EPOLL_DISPATCH_SIZE];
};

IOPollEPoll::IOPollEPoll() : data_(new Data()) {
	const int fd = epoll_create(UTIL_EPOLL_SIZE);
	if (-1 == fd) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	data_->epollFD_ = fd;
}

IOPollEPoll::~IOPollEPoll() {
	::close(data_->epollFD_);
}

void IOPollEPoll::add(IOPollHandler *handler, IOPollEvent event) {
	const int fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);

	struct epoll_event ev;
	ev.data.ptr = handler;
	ev.events = event;
	if (0 != epoll_ctl(data_->epollFD_, EPOLL_CTL_ADD, fd, &ev)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

void IOPollEPoll::modify(IOPollHandler *handler, IOPollEvent event) {
	const int fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);

	struct epoll_event ev;
	ev.data.ptr = handler;
	ev.events = event;
	if (0 != epoll_ctl(data_->epollFD_, EPOLL_CTL_MOD, fd, &ev)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

void IOPollEPoll::remove(IOPollHandler *handler) {
	const int fd = handler->getFile().getHandle();
	assert(fd != File::INITIAL_FD);

	if (0 != epoll_ctl(data_->epollFD_, EPOLL_CTL_DEL, fd, NULL)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}

bool IOPollEPoll::dispatch(uint32_t msec) {
	const int ret = epoll_wait(
			data_->epollFD_, data_->epollEvents_,
			UTIL_EPOLL_DISPATCH_SIZE, static_cast<int>(msec));
	if (-1 == ret) {
		if (errno == EINTR) {
			return false;
		} else {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}

	struct epoll_event *i = data_->epollEvents_;
	struct epoll_event *end = data_->epollEvents_ + ret;
	for (; i < end; ++i) {
		static_cast<IOPollHandler*>(i->data.ptr)->handlePollEvent(
				this, i->events);
	}
	return (ret > 0);
}

const char* IOPollEPoll::getType(void) const {
	static const char type[] = "epoll";
	return type;
}

#endif 

#ifdef UTIL_HAVE_POLL


namespace {
const int UTIL_POLL_INIT_SIZE = 10240;
}

IOPollPollCont::IOPollPollCont() :
		vec_(NULL), max_(0), cur_(0) {
}

IOPollPollCont::~IOPollPollCont() {
	delete[] vec_;
	max_ = 0;
	cur_ = 0;
}

void IOPollPollCont::increase(void) {
	if (vec_) {
		size_t nextsize = max_ * 2;
		if (0 == nextsize) {
			nextsize = cur_ + UTIL_POLL_INIT_SIZE;
			if (nextsize < cur_) {
				UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
			}
		}

		pollfd* nextvec = new pollfd[nextsize];

		memcpy(nextvec, vec_, sizeof(pollfd) * cur_);
		delete[] vec_;
		vec_ = nextvec;
		max_ = nextsize;
	} else {
		vec_ = new pollfd[UTIL_POLL_INIT_SIZE];

		cur_ = 0;
		max_ = UTIL_POLL_INIT_SIZE;
	}
}

void IOPollPollCont::clear(void) {
	cur_ = 0;
	index_.clear();
}

const IOPollHandler* IOPollPollCont::getFile(int fd) const {
	IndexCItr ib = index_.find(fd);
	if (index_.end() == ib) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	return ib->second.file;
}

IOPollHandler* IOPollPollCont::getFile(int fd) {
	IndexItr ib = index_.find(fd);
	if (index_.end() == ib) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	return ib->second.file;
}

void IOPollPollCont::add(int fd, IOPollHandler *file, IOPollEvent events) {
	if (index_.find(fd) != index_.end()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	if (cur_ == max_) {
		increase();
	}

	pollfd &fdes = vec_[cur_];
	fdes.fd = fd;
	fdes.events = static_cast<short>(events);

	index_.insert(IndexCount::value_type(fd, FileDes(file, cur_)));
	++cur_;
}

void IOPollPollCont::remove(int fd) {
	if (0 == cur_) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	IndexItr ib = index_.find(fd);
	if (index_.end() == ib) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	if (1 == cur_) {
		cur_ = 0;
		index_.clear();
	}

	const size_t index = ib->second.index;

	size_t lastIndex = cur_ - 1;
	if (index == lastIndex) {
		--lastIndex;
	}

	const int lastfd = vec_[lastIndex].fd;

	vec_[index] = vec_[lastIndex];
	index_.find(lastfd)->second.index = index;

	--cur_;
}

void IOPollPollCont::modify(int fd, IOPollEvent events) {
	IndexItr ib = index_.find(fd);
	if (index_.end() == ib) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_ARGUMENT);
	}

	vec_[ib->second.index].events = static_cast<short>(events);
}

IOPollPoll::IOPollPoll() {
}

IOPollPoll::~IOPollPoll() {
}

void IOPollPoll::add(IOPollHandler *handler, IOPollEvent event) {
	const int fd = handler->getFile().getHandle();
	cont_.add(fd, handler, event);
}

void IOPollPoll::modify(IOPollHandler *handler, IOPollEvent event) {
	const int fd = handler->getFile().getHandle();
	cont_.modify(fd, event);
}

void IOPollPoll::remove(IOPollHandler *handler) {
	const int fd = handler->getFile().getHandle();
	cont_.remove(fd);
}

bool IOPollPoll::dispatch(uint32_t msec) {
	struct pollfd *vec = cont_.getPolls();
	const size_t vecSize = cont_.getCurrentSize();
	const int ret = poll(vec, vecSize, static_cast<int>(msec));
	if (-1 == ret) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	int r = 0;
	int fd;
	int revents;
	IOPollHandler *file = NULL;

	for (size_t i = 0; i < vecSize && r < ret; i++) {
		revents = vec[i].revents;
		if (revents) {
			fd = vec[i].fd;
			file = cont_.getFile(fd);
			if (file) {
				file->handlePollEvent(this, revents);
				++r;
			}
		}
	}
	return (ret > 0);
}

const char* IOPollPoll::getType(void) const {
	static const char type[] = "poll";
	return type;
}

#endif 

Pipe::Pipe(util::File::FD fd) {
	file_.attach(fd);

	writeCount_ = 0;
	readCount_ = 0;
}

Pipe::~Pipe() {
}

void Pipe::handlePollEvent(IOPollBase*, IOPollEvent event) {
	if (event & util::IOPollEvent::TYPE_READ) {
		if (file_.read(readBuf, NOTIFICATION_SIZE) != NOTIFICATION_SIZE) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
}

File& Pipe::getFile() {
	return file_;
}

void Pipe::create(Pipe *&writeTerm, Pipe *&readTerm) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	int fd[2];

	pipe(fd);

	writeTerm = NULL;
	readTerm = NULL;

	try {
		writeTerm = new Pipe(fd[1]);
		readTerm = new Pipe(fd[0]);

	} catch (std::bad_alloc &) {
		if (writeTerm != NULL) {
			delete writeTerm;
		} else {
			close(fd[1]);
		}
		if (readTerm != NULL) {
			delete readTerm;
		} else {
			close(fd[0]);
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

} 
