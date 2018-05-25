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
	@brief Implementation of EventEngine
*/
#include "event_engine.h"

#include "config_table.h"
#include "util/trace.h"
#include <iostream>

UTIL_TRACER_DECLARE(EVENT_ENGINE);
UTIL_TRACER_DECLARE(IO_MONITOR);


EventEngine::EventEngine(
		const Config &config, const Source &source, const char8_t *name) :
		fixedAllocator_(&source.resolveFixedSizeAllocator()),
		name_(name),
		ioWorkerList_(NULL),
		eventWorkerList_(NULL),
		bufferManager_(source.bufferManager_) {

	try {
		source.resolveVariableSizeAllocator();

		config_.reset(UTIL_NEW Config(config));
		pgConfig_.reset(UTIL_NEW PartitionGroupConfig(
				config.partitionCount_, config.concurrency_));
		stats_.reset(UTIL_NEW Stats());

		ndPool_.reset(UTIL_NEW NDPool(*this));
		socketPool_.reset(UTIL_NEW SocketPool(*this));
		dispatcher_.reset(UTIL_NEW Dispatcher(*this));
		limitter_.reset(UTIL_NEW Limitter(*this));

		const NodeDescriptor selfND = ndPool_->getSelfServerND();
		if (!selfND.isEmpty()) {
			selfAddress_ = selfND.getAddress();
		}

		clockGenerator_.reset(UTIL_NEW ClockGenerator(*this));

		if (Manipulator::isListenerEnabled(config)) {
			listener_.reset(UTIL_NEW Listener(*this));
		}

		const uint32_t ioConcurrency = Manipulator::getIOConcurrency(config);
		if (ioConcurrency > 0) {
			ioWorkerList_ = UTIL_NEW IOWorker[ioConcurrency];
			for (uint32_t i = 0; i < ioConcurrency; i++) {
				ioWorkerList_[i].initialize(*this, i);
			}
		}

		eventWorkerList_ = UTIL_NEW EventWorker[config_->concurrency_];
		for (uint32_t i = 0; i < config_->concurrency_; i++) {
			eventWorkerList_[i].initialize(*this, i);
		}

		Manipulator::prepareMulticastSocket(*config_, *ndPool_, *socketPool_);

		stats_->set(Stats::MAIN_LISTENER_COUNT,
				Manipulator::isListenerEnabled(config) ? 1 : 0);

		stats_->set(Stats::MAIN_IO_WORKER_COUNT, ioConcurrency);
		stats_->set(Stats::MAIN_EVENT_WORKER_COUNT, config_->concurrency_);
	}
	catch (...) {
		cleanWorkers();

		try {
			throw;
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(e,
					"Initialization failed (name=" << name <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}

		throw;
	}
}

EventEngine::~EventEngine() {
	try {
		shutdown();
	}
	catch (...) {
	}

	try {
		waitForShutdown();
	}
	catch (...) {
	}

	try {
		cleanWorkers();
	}
	catch (...) {
	}
}

/*!
	@brief Starts EventEngine
*/
void EventEngine::start() {
	try {
		clockGenerator_->start();

		if (Manipulator::isListenerEnabled(*config_)) {
			listener_->start();
		}

		const uint32_t ioConcurrency = Manipulator::getIOConcurrency(*config_);
		for (uint32_t i = 0; i < ioConcurrency; i++) {
			ioWorkerList_[i].start();
		}

		for (uint32_t i = 0; i < config_->concurrency_; i++) {
			eventWorkerList_[i].start();
		}
	}
	catch (...) {
		try {
			shutdown();
		}
		catch (...) {
		}

		try {
			waitForShutdown();
		}
		catch (...) {
		}

		try {
			throw;
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_OR_SYSTEM(e,
					"Failed to start (name=" << name_ <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
}

/*!
	@brief Shutdown EventEngine
*/
void EventEngine::shutdown() {
	for (uint32_t i = 0; i < config_->concurrency_; i++) {
		eventWorkerList_[i].shutdown();
	}

	const uint32_t ioConcurrency = Manipulator::getIOConcurrency(*config_);
	for (uint32_t i = 0; i < ioConcurrency; i++) {
		ioWorkerList_[i].shutdown();
	}

	if (Manipulator::isListenerEnabled(*config_)) {
		listener_->shutdown();
	}

	clockGenerator_->shutdown();
}

/*!
	@brief Waits for shutdown
*/
void EventEngine::waitForShutdown() {
	for (uint32_t i = 0; i < config_->concurrency_; i++) {
		eventWorkerList_[i].waitForShutdown();
	}

	const uint32_t ioConcurrency = Manipulator::getIOConcurrency(*config_);
	for (uint32_t i = 0; i < ioConcurrency; i++) {
		ioWorkerList_[i].waitForShutdown();
	}

	if (Manipulator::isListenerEnabled(*config_)) {
		listener_->waitForShutdown();
	}

	clockGenerator_->waitForShutdown();
}

/*!
	@brief Sends event
*/
bool EventEngine::send(
		Event &ev, const NodeDescriptor &nd,
		const EventRequestOption *option) {
	if (nd.isEmpty()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
	}

	if (ev.isEmpty()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_EE_PARAMETER_INVALID, "Event is empty");
	}

	NodeDescriptor::Body &body = Manipulator::getNDBody(nd);
	if (&body.getEngine() != this) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Engine unmatched (expected=" << getName() <<
				", actual=" << body.getEngine().getName() << ")");
	}

	try {
		LockGuard guard(body.getLock());

		EventSocket *socket = body.getSocket(guard);
		if (socket == NULL) {
			if (nd.getType() == NodeDescriptor::ND_TYPE_CLIENT) {
				GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
						"Client socket already closed");
			}

			SocketReference socketRef(socketPool_->allocate(), &guard);

			body.setSocket(guard, socketRef.get(),
					NodeDescriptor::Body::ND_SOCKET_SENDER);

			if (!socketRef.get()->openAsClient(guard, nd.getAddress())) {
				return false;
			}
			socket = socketRef.release();
		}

		socket->send(guard, ev, option);
		return true;
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
				"Failed to send (engine=" << getName() <<
				", eventType=" << ev.getType() <<
				", nd=" << nd <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		return false;
	}
}

EventMonotonicTime EventEngine::getMonotonicTime() {
	return clockGenerator_->getMonotonicTime();
}


const NodeDescriptor NodeDescriptor::EMPTY_ND;

std::ostream& NodeDescriptor::format(std::ostream &stream) const {
	if (isEmpty()) {
		stream << "(empty)";
	}
	else {
		const Type type = getType();
		switch (type) {
		case ND_TYPE_CLIENT:
			stream << "{clientId=" << (-getId()) << ", address=";
			try {
				EventEngine::LockGuard guard(body_->getLock());

				EventEngine::EventSocket *socket = body_->getSocket(guard);
				if (socket == NULL) {
					stream << "(socket closed)";
				}
				else {
					util::SocketAddress address;
					stream << static_cast<util::Socket&>(
							socket->getFile()).getPeerName(address);
				}
			}
			catch (...) {
			}
			stream << "}";
			break;

		case ND_TYPE_SERVER:
			stream <<
					"{serverId=" << getId() <<
					", address=" << getAddress() <<
					(isSelf() ? ", self=true" : "") << "}";
			break;

		case ND_TYPE_MULTICAST:
			stream << "{type=multicast, address=" << getAddress() << "}";
			break;

		default:
			stream << "{type=unknown(" << static_cast<int32_t>(type) << ")}";
			break;
		}
	}
	return stream;
}


EventEngine::Config::Config() :
		clientNDEnabled_(false),
		serverNDAutoNumbering_(false),
		listenAddress_(),
		multicastAddress_(),
		serverAddress_(),
		ioConcurrencyRate_(1.0),
		keepaliveEnabled_(),
		keepaliveIdle_(600),
		keepaliveInterval_(60),
		keepaliveCount_(5),
		tcpNoDelay_(true),
		backlogSize_(100),
		multicastTTL_(1),
		ioBufferSize_(64 * 1024),
		reconnectIntervalMillis_(1 * 1000),
		connectTimeoutMillis_(10 * 1000),
		filteredPartitionId_(0),
		connectionCountLimit_(-1),
		sendBufferSizeLimit_(-1),
		concurrency_(1),
		partitionCount_(1),
		eventBufferSizeLimit_(-1),
		clockIntervalMillis_(100),
		clockCorrectionMillis_(1000),
		clockCorrectionMaxTrial_(50),
		allocatorGroupId_(util::AllocatorManager::GROUP_ID_ROOT),
		workAllocatorGroupId_(allocatorGroupId_) {
}

EventEngine::Config& EventEngine::Config::setClientNDEnabled(
		bool enabled) {
	clientNDEnabled_ = enabled;
	return *this;
}

EventEngine::Config& EventEngine::Config::setServerNDAutoNumbering(
		bool enabled) {
	serverNDAutoNumbering_ = enabled;
	return *this;
}

EventEngine::Config& EventEngine::Config::setListenAddress(
		const char8_t *address, uint16_t port) {
	listenAddress_.assign(address, port);
	return *this;
}

EventEngine::Config& EventEngine::Config::setMulticastAddress(
		const char8_t *address, uint16_t port) {
	multicastAddress_.assign(address, port);
	return *this;
}

EventEngine::Config& EventEngine::Config::setServerAddress(
		const char8_t *address, uint16_t port) {
	const uint32_t ioWarningThresholdMillis = 500;
	const uint32_t intervalMillis = 500;
	const uint32_t maxTrialCount = 10;

	util::Stopwatch totalWatch;
	totalWatch.start();

	for (uint32_t trial = 1;; trial++) {
		util::Stopwatch monitorWatch;
		monitorWatch.start();

		bool succeeded = false;
		u8string hostName;
		try {
			int family;
			if (strlen(address) == 0) {
				util::SocketAddress::getHostName(hostName);
				family = util::SocketAddress::FAMILY_INET;
			}
			else {
				hostName = address;
				family = 0;
			}
			serverAddress_.assign(hostName.c_str(), port, family);
			succeeded = true;
		}
		catch (std::exception &e) {
			monitorWatch.stop();

			if (trial == 1) {
				UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
						"Retrying to resolve address (" <<
						"host=" << hostName <<
						", port=" << port <<
						", trial=" << trial <<
						", elapsedMillis=" << totalWatch.elapsedMillis() <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
			else if (trial > maxTrialCount) {
				GS_RETHROW_USER_OR_SYSTEM(e,
						"Failed to resolve address (" <<
						"host=" << hostName <<
						", port=" << port <<
						", trial=" << trial <<
						", elapsedMillis=" << totalWatch.elapsedMillis() <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		const uint32_t ioMillis = monitorWatch.elapsedMillis();
		if (ioMillis > ioWarningThresholdMillis) {
			GS_TRACE_WARNING(IO_MONITOR, GS_TRACE_CM_LONG_IO,
					"Address resolve time," << ioMillis <<
					",host," << hostName <<
					",port," << port <<
					",trial," << trial <<
					",elapsed," << totalWatch.elapsedMillis());
		}

		if (succeeded) {
			break;
		}
		util::Thread::sleep(intervalMillis);
	}

	return *this;
}

EventEngine::Config& EventEngine::Config::setConcurrency(
		uint32_t concurrency) {
	concurrency_ = concurrency;
	return *this;
}

EventEngine::Config& EventEngine::Config::setPartitionCount(
		uint32_t partitionCount) {
	partitionCount_ = partitionCount;
	return *this;
}

EventEngine::Config& EventEngine::Config::setAllAllocatorGroup(
		util::AllocatorGroupId id) {
	allocatorGroupId_= id;
	workAllocatorGroupId_= id;
	return *this;
}


EventEngine::BufferManager::~BufferManager() {
}


EventEngine::ExternalBuffer::ExternalBuffer(BufferManager *manager) :
		manager_(manager),
		size_(0),
		capacity_(0) {
}

EventEngine::ExternalBuffer::~ExternalBuffer() {
	try {
		clear();
	}
	catch (...) {
	}
}

void EventEngine::ExternalBuffer::clear() {
	if (capacity_ != 0) {
		assert(manager_ != NULL);

		BufferManager *manager = manager_;
		BufferId id = id_;

		manager_ = NULL;
		id_ = BufferId();
		size_ = 0;
		capacity_ = 0;

		manager->deallocate(id);
	}
}

void EventEngine::ExternalBuffer::swap(ExternalBuffer &another) {
	std::swap(manager_, another.manager_);
	std::swap(id_, another.id_);
	std::swap(size_, another.size_);
	std::swap(capacity_, another.capacity_);
}

bool EventEngine::ExternalBuffer::isEmpty() const {
	return capacity_ == 0;
}

size_t EventEngine::ExternalBuffer::getSize() const {
	return size_;
}

size_t EventEngine::ExternalBuffer::getCapacity() const {
	return capacity_;
}


EventEngine::ExternalBuffer::Reader::Reader(const ExternalBuffer &buffer) :
		manager_(buffer.manager_),
		id_(buffer.id_),
		data_(manager_->latch(id_)) {
}

EventEngine::ExternalBuffer::Reader::~Reader() {
	try {
		manager_->unlatch(id_);
	}
	catch (...) {
	}
}

const void* EventEngine::ExternalBuffer::Reader::data() {
	return data_;
}


EventEngine::ExternalBuffer::Writer::Writer(ExternalBuffer &buffer) :
		bufferRef_(buffer),
		buffer_(buffer.manager_),
		data_(NULL) {

	BufferManager *manager = buffer_.manager_;
	if (manager == NULL) {
		return;
	}

	const std::pair<BufferId, void*> &ret = manager->allocate();
	buffer_.id_ = ret.first;
	buffer_.size_ = 0;
	buffer_.capacity_ = manager->getUnitSize();
	data_ = ret.second;

	if (bufferRef_.size_ != 0) {
		assert(bufferRef_.size_ <= buffer_.capacity_);

		Reader reader(bufferRef_);
		memcpy(data_, reader.data(), bufferRef_.size_);
		buffer_.size_ = bufferRef_.size_;
	}

	bufferRef_.clear();
}

EventEngine::ExternalBuffer::Writer::~Writer() {
	BufferManager *manager = buffer_.manager_;
	if (manager == NULL) {
		return;
	}

	try {
		const BufferId id = buffer_.id_;
		buffer_.swap(bufferRef_);
		manager->unlatch(id);
	}
	catch (...) {
	}
}

size_t EventEngine::ExternalBuffer::Writer::tryAppend(
		const void *data, size_t size) {
	const size_t offset = buffer_.size_;
	const size_t capacity = buffer_.capacity_;
	assert(offset <= capacity);

	const size_t consuming = std::min(size, capacity - offset);

	if (offset < capacity && consuming > 0) {
		memcpy(static_cast<uint8_t*>(data_) + offset, data, consuming);
		buffer_.size_ = offset + consuming;
	}

	return consuming;
}


EventEngine::NDPool::NDPool(EventEngine &ee) :
		ee_(ee),
		config_(*ee_.config_),
		varAllocator_(util::AllocatorInfo(config_.allocatorGroupId_, "ndVar")),
		userDataConstructor_(NULL),
		userDataDestructor_(NULL),
		timeMapPool_(util::AllocatorInfo(config_.allocatorGroupId_, "ndTimeMap")),
		bodyPool_(util::AllocatorInfo(config_.allocatorGroupId_, "ndBody")),
		addressPool_(util::AllocatorInfo(config_.allocatorGroupId_, "ndAddress")),
		bodyFreeLink_(NULL),
		serverNDList_(varAllocator_),
		serverNDMap_(SocketAddressLess(), varAllocator_),
		lastClientNDId_(-1) {

	if (!config_.serverAddress_.isEmpty()) {
		bool found;
		selfServerND_ = putServerND(config_.serverAddress_, 0, false, found);
	}

	if (!config_.multicastAddress_.isEmpty()) {
		multicastND_ = NodeDescriptor(
				allocateBody(-1, NodeDescriptor::ND_TYPE_MULTICAST, false));

		Manipulator::getNDBody(multicastND_).address_ =
				&config_.multicastAddress_;
	}
}

EventEngine::NDPool::~NDPool() try {
	for (NDMap::iterator it = serverNDMap_.begin();
			it != serverNDMap_.end(); ++it) {
		addressPool_.deallocate(it->second.address_);
	}

	multicastND_ = NodeDescriptor();
	selfServerND_ = NodeDescriptor();
	serverNDMap_.clear();
	serverNDList_.clear();

	for (NodeDescriptor::Body *&body = bodyFreeLink_; body != NULL;) {
		NodeDescriptor::Body *next = body->freeLink_;
		UTIL_OBJECT_POOL_DELETE(bodyPool_, body);
		body = next;
	}
}
catch (...) {
}

NodeDescriptor EventEngine::NDPool::getServerND(NodeDescriptorId ndId) {
	LockGuard guard(mutex_);

	if (ndId < 0 || static_cast<size_t>(ndId) >= serverNDList_.size()) {
		return NodeDescriptor();
	}

	return serverNDList_[static_cast<size_t>(ndId)];
}

NodeDescriptor EventEngine::NDPool::getServerND(
		const util::SocketAddress &address) {
	LockGuard guard(mutex_);

	NDMap::iterator it = serverNDMap_.find(address);
	if (it == serverNDMap_.end()) {
		return NodeDescriptor();
	}

	return it->second.nd_;
}

NodeDescriptor EventEngine::NDPool::resolveServerND(
		const util::SocketAddress &address) {
	LockGuard guard(mutex_);

	if (serverNDList_.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Server ND is not allowed ("
				"serverNDCount=" << serverNDList_.size() << ")");
	}

	NDMap::iterator it = serverNDMap_.find(address);
	if (it != serverNDMap_.end() && !it->second.nd_.isEmpty()) {
		return it->second.nd_;
	}

	if (!config_.serverNDAutoNumbering_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Server ND auto numbering is not allowed");
	}

	bool found;
	const NodeDescriptorId ndId =
			static_cast<NodeDescriptorId>(serverNDList_.size());

	return putServerND(address, ndId, false, found);
}

bool EventEngine::NDPool::setServerNodeId(
		const util::SocketAddress &address, NodeDescriptorId ndId,
		bool modifiable) {
	LockGuard guard(mutex_);

	if (serverNDList_.empty() || config_.serverNDAutoNumbering_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Server ND is not allowed or auto numbering is activated ("
				"serverNDCount=" << serverNDList_.size() <<
				", autoNumbering=" << config_.serverNDAutoNumbering_ << ")");
	}

	if (ndId <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Invalid ND ID (specified=" << ndId << ")");
	}

	bool found;
	putServerND(address, ndId, modifiable, found);

	return found;
}

NodeDescriptor EventEngine::NDPool::getSelfServerND() {
	return selfServerND_;
}

NodeDescriptor EventEngine::NDPool::getMulticastND() {
	return multicastND_;
}

void EventEngine::NDPool::setUserDataType(
		UserDataConstructor &constructor, UserDataDestructor &destructor,
		size_t dataSize) {
	LockGuard guard(userDataMutex_);

	if (userDataPool_.get() != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"User data type has already been set");
	}

	userDataPool_.reset(UTIL_NEW util::FixedSizeAllocator<>(
			util::AllocatorInfo(config_.allocatorGroupId_, "ndUserData"), dataSize));
	userDataConstructor_ = &constructor;
	userDataDestructor_ = &destructor;
}

NodeDescriptor EventEngine::NDPool::allocateClientND() {
	static const util::SocketAddress emptyAddress;

	if (!ee_.config_->clientNDEnabled_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Client ND is not allowed");
	}

	NodeDescriptor::Body *ndBody;
	{
		LockGuard guard(mutex_);
		ndBody = &allocateBody(
				--lastClientNDId_, NodeDescriptor::ND_TYPE_CLIENT, false);
	}
	ndBody->address_ = &emptyAddress;

	return NodeDescriptor(*ndBody);
}

int64_t& EventEngine::NDPool::prepareLastEventTime(
		TimeMap *&map, EventType type) {
	TimeMap::iterator it;
	if (map == NULL || (it = map->find(type)) == map->end()) {
		LockGuard guard(poolMutex_);
		if (map == NULL) {
			map = UTIL_OBJECT_POOL_NEW(timeMapPool_)
					TimeMap(TimeMap::key_compare(), varAllocator_);
		}
		it = map->insert(TimeMapEntry(
				type, std::numeric_limits<int64_t>::min())).first;
	}

	return it->second;
}

NodeDescriptor EventEngine::NDPool::putServerND(
		const util::SocketAddress &address, NodeDescriptorId ndId,
		bool modifiable, bool &found) {
	found = false;

	if (ndId < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Invalid ID (specified=" << ndId << ")");
	}

	if (address.isEmpty()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_EE_PARAMETER_INVALID, "Empty address");
	}

	if (static_cast<size_t>(ndId) >= serverNDList_.size()) {
		serverNDList_.resize(static_cast<size_t>(ndId) + 1);
	}


	NodeDescriptor nd = serverNDList_[static_cast<size_t>(ndId)];
	if (nd.isEmpty()) {
		nd = NodeDescriptor(allocateBody(
				ndId, NodeDescriptor::ND_TYPE_SERVER, ndId == 0));
	}
	else if (modifiable) {
		found = true;
	}
	else {
		ee_.stats_->increment(Stats::ND_MODIFY_ERROR_COUNT);
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"ID conflicted (ndId=" << ndId <<
				", orgAddress=" << nd.getAddress() <<
				", newAddress=" << address <<
				", modifiable=" << modifiable << ")");
	}


	std::pair<NDMap::iterator, bool> mapResult =
			serverNDMap_.insert(std::make_pair(address, NDMapValue()));
	NDMapValue &value = mapResult.first->second;

	if (mapResult.second || value.nd_.isEmpty()) {
		if (value.address_ == NULL) {
			value.address_ =
					UTIL_OBJECT_POOL_NEW(addressPool_) util::SocketAddress(address);
		}
	}
	else if (modifiable) {
		found = true;
	}
	else {
		ee_.stats_->increment(Stats::ND_MODIFY_ERROR_COUNT);
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Address conflicted (address=" << address <<
				", orgNdId=" << value.nd_.getId() <<
				", newNdId=" << ndId <<
				", modifiable=" << modifiable << ")");
	}

	NodeDescriptor::Body &ndBody = Manipulator::getNDBody(nd);
	const util::SocketAddress *orgAddress = ndBody.address_;
	const util::SocketAddress *newAddress = value.address_;

	if (orgAddress != newAddress) {
		if (orgAddress != NULL) {
			serverNDMap_[*orgAddress].nd_ = NodeDescriptor();
		}

		ndBody.address_ = newAddress;
	}

	serverNDList_[static_cast<size_t>(ndId)] = nd;

	value.nd_ = nd;

	if (found) {
		ee_.stats_->increment(Stats::ND_MODIFY_COUNT);
	}

	return nd;
}

/*!
	@brief Allocates body objects
*/
NodeDescriptor::Body& EventEngine::NDPool::allocateBody(
		NodeDescriptorId ndId, NodeDescriptor::Type type, bool self) {
	LockGuard guard(poolMutex_);

	switch (type) {
	case NodeDescriptor::ND_TYPE_CLIENT:
		ee_.stats_->increment(Stats::ND_CLIENT_CREATE_COUNT);
		break;
	case NodeDescriptor::ND_TYPE_SERVER:
		ee_.stats_->increment(Stats::ND_SERVER_CREATE_COUNT);
		break;
	case NodeDescriptor::ND_TYPE_MULTICAST:
		ee_.stats_->increment(Stats::ND_MCAST_CREATE_COUNT);
		break;
	default:
		assert(false);
	}

	if (type == NodeDescriptor::ND_TYPE_CLIENT && bodyFreeLink_ != NULL) {
		NodeDescriptor::Body *body = bodyFreeLink_;

		bodyFreeLink_ = body->freeLink_;
		body->freeLink_ = NULL;

		return *body;
	}

	return *(UTIL_OBJECT_POOL_NEW(bodyPool_) NodeDescriptor::Body(
			ndId, type, self , ee_));
}

/*!
	@brief Allocates user data
*/
void* EventEngine::NDPool::allocateUserData(UserDataConstructor *constructor) {
	LockGuard guard(userDataMutex_);

	if (userDataPool_.get() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"User data type not specified");
	}

	if (constructor != NULL && constructor != userDataConstructor_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Invalid user data type");
	}

	void* userData = userDataPool_->allocate();
	try {
		(*userDataConstructor_)(userData);
	}
	catch (...) {
		userDataPool_->deallocate(userData);
		throw;
	}

	ee_.stats_->increment(Stats::ND_USER_DATA_CREATE_COUNT);

	return userData;
}



bool EventEngine::NDPool::SocketAddressLess::operator()(
		const util::SocketAddress &left,
		const util::SocketAddress &right) const {
	if (left.getFamily() != right.getFamily()) {
		return left.getFamily() < right.getFamily();
	}

	if (left.getFamily() == util::SocketAddress::FAMILY_INET) {
		util::SocketAddress::Inet leftAddr;
		util::SocketAddress::Inet rightAddr;
		uint16_t leftPort;
		uint16_t rightPort;
		left.getIP(&leftAddr, &leftPort);
		right.getIP(&rightAddr, &rightPort);

		if (leftPort != rightPort) {
			return leftPort < rightPort;
		}

		return memcmp(&leftAddr, &rightAddr, sizeof(leftAddr)) < 0;
	}
	else if (left.getFamily() == util::SocketAddress::FAMILY_INET6) {
		util::SocketAddress::Inet6 leftAddr;
		util::SocketAddress::Inet6 rightAddr;
		uint16_t leftPort;
		uint16_t rightPort;
		left.getIP(&leftAddr, &leftPort);
		right.getIP(&rightAddr, &rightPort);

		if (leftPort != rightPort) {
			return leftPort < rightPort;
		}

		return memcmp(&leftAddr, &rightAddr, sizeof(leftAddr)) < 0;
	}
	else {
		return false;
	}
}


EventEngine::NDPool::NDMapValue::NDMapValue() : address_(NULL) {
}


EventEngine::SocketPool::SocketPool(EventEngine &ee) :
		ee_(ee),
		ioConcurrency_(Manipulator::getIOConcurrency(*ee.config_)),
		base_(util::AllocatorInfo(ee.config_->allocatorGroupId_, "eeSocket")),
		workerSelectionSeed_(0) {
}

EventEngine::SocketPool::~SocketPool() {
}

EventEngine::EventSocket* EventEngine::SocketPool::allocate() {
	assert(ioConcurrency_ > 0);

	LockGuard guard(mutex_);

	const int64_t baseLimit = ee_.config_->connectionCountLimit_;
	if (baseLimit >= 0) {
		const uint64_t current = base_.base().getTotalElementCount() -
				base_.base().getFreeElementCount();
		const uint64_t limit = static_cast<uint64_t>(baseLimit) +
				(ee_.config_->multicastAddress_.isEmpty() ? 0 : 1);

		if (current >= limit) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_FD_LIMIT_EXCEEDED,
					"Limit exceeded (socketCount=" << current <<
					", socketLimit=" << limit <<
					", connectionLimit=" << baseLimit << ")");
		}
	}

	if (++workerSelectionSeed_ >= ioConcurrency_) {
		workerSelectionSeed_ = 0;
	}

	EventSocket *socket = UTIL_OBJECT_POOL_NEW(base_) EventSocket(
			ee_, ee_.ioWorkerList_[workerSelectionSeed_]);

	ee_.stats_->increment(Stats::SOCKET_CREATE_COUNT);
	return socket;
}

void EventEngine::SocketPool::deallocate(
		EventSocket *socket, bool workerAlive, LockGuard *ndGuard) {
	assert(socket != NULL);

	socket->closeLocal(workerAlive, ndGuard);

	{
		LockGuard guard(mutex_);
		UTIL_OBJECT_POOL_DELETE(base_, socket);

		ee_.stats_->increment(Stats::SOCKET_REMOVE_COUNT);
	}
}


EventEngine::Dispatcher::Dispatcher(EventEngine &ee) :
		ee_(ee),
		localHandler_(NULL),
		disconnectHandler_(NULL),
		unknownEventHandler_(NULL),
		threadErrorHandler_(NULL),
		eventCoder_(NULL),
		closeHandlerCount_(0) {

	static DefaultEventCoder defaultEventCoder;
	eventCoder_ = &defaultEventCoder;
}

EventEngine::Dispatcher::~Dispatcher() {
}

void EventEngine::Dispatcher::setHandler(
		EventType type, EventHandler &handler) {

	HandlerEntry entry;
	entry.handler_ = &handler;

	setHandlerEntry(type, entry);
}

void EventEngine::Dispatcher::setHandlingMode(
		EventType type, EventHandlingMode mode) {

	HandlerEntry *entry = findHandlerEntry(type, true);

	if (entry == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Unknown event type (type=" << type << ")");
	}

	entry->mode_ = mode;
}

void EventEngine::Dispatcher::setUnknownEventHandler(EventHandler &handler) {
	unknownEventHandler_ = &handler;
}

void EventEngine::Dispatcher::setThreadErrorHandler(
		ThreadErrorHandler &handler) {
	threadErrorHandler_ = &handler;
}

void EventEngine::Dispatcher::setLocalHandler(EventHandler &handler) {
	localHandler_ = &handler;
}

void EventEngine::Dispatcher::setCloseEventHandler(
		EventType type, EventHandler &handler) {

	HandlerEntry entry;
	entry.handler_ = &handler;
	entry.closeMode_ = true;

	setHandlerEntry(type, entry);

	closeHandlerCount_++;
}

void EventEngine::Dispatcher::setDisconnectHandler(EventHandler &handler) {
	disconnectHandler_ = &handler;
}


void EventEngine::Dispatcher::setEventCoder(EventCoder &coder) {
	eventCoder_ = &coder;
}


EventEngine::Dispatcher::DispatchResult EventEngine::Dispatcher::dispatch(
		EventContext &ec, Event &ev, bool queueingSuspended,
		const EventRequestOption &option) {

	Stats &stats = Manipulator::getStats(ec);

	HandlerEntry *entry = findHandlerEntry(ev.getType(), true);

	stats.increment(Stats::WORKER_DISPATCH_COUNT);

	int32_t timeoutMillis = option.timeoutMillis_;
	if (timeoutMillis != 0) {
		NodeDescriptor nd = ev.getSenderND();
		assert(!nd.isEmpty());

		NodeDescriptor::Body &body = Manipulator::getNDBody(nd);
		LockGuard guard(body.getLock());

		timeoutMillis =
				body.acceptEventTimeout(guard, ev.getType(), timeoutMillis);

		stats.increment(Stats::WORKER_TIMED_DISPATCH_COUNT);
		stats.updateMax(Stats::WORKER_DISPATCH_TIMEOUT_MAX, timeoutMillis);
		stats.updateMin(Stats::WORKER_DISPATCH_TIMEOUT_MIN, timeoutMillis);
	}

	if (entry == NULL) {
		handleUnknownEvent(ec, ev);
		return DISPATCH_RESULT_DONE;
	}
	else if (entry->closeMode_) {
		handleEvent(ec, ev, *entry);

		stats.increment(Stats::WORKER_CLOSING_DISPATCH_COUNT);
		return DISPATCH_RESULT_CLOSED;
	}

	if (entry->mode_ == HANDLING_IMMEDIATE) {
		if (ev.getPartitionId() >= ee_.pgConfig_->getPartitionCount()) {
			filterImmediatePartitionId(ev);
		}

		handleEvent(ec, ev, *entry);

		stats.increment(Stats::WORKER_IMMEDIATE_DISPATCH_COUNT);
		return DISPATCH_RESULT_DONE;
	}
	else if (queueingSuspended) {
		return DISPATCH_RESULT_CANCELED;
	}
	else {
		selectWorker(ev, &entry->mode_).add(ev, timeoutMillis, 0);
		return DISPATCH_RESULT_DONE;
	}
}

void EventEngine::Dispatcher::requestAndWait(EventContext &ec, Event &ev) {
	Stats &stats = Manipulator::getStats(ec);
	const uint32_t intervalMillis = 10 * 1000;

	if (ev.getProgressWatcher() != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Progress has already been watched ("
				"watcherEngine=" << ec.getEngine().getName() <<
				", watchedEngine=" << ee_.getName() <<
				", eventType=" << ev.getType() << ")");
	}

	if (&ec.getEngine() == &ee_) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"Watching itself (engine=" << ee_.getName() <<
				", eventType=" << ev.getType() << ")");
	}

	EventWorker &worker = selectWorker(ev, NULL);

	EventProgressWatcher &watcher = ec.getProgressWatcher();
	watcher.clear();

	ev.setProgressWatcher(&watcher);
	try {
		const int64_t queueingTime = ee_.clockGenerator_->getMonotonicTime();
		worker.add(ev, 0, 0);

		stats.increment(Stats::WORKER_WAIT_START_COUNT);

		stats.decrement(Stats::WORKER_HANDLING_THREAD_COUNT);
		stats.increment(Stats::WORKER_WAITING_THREAD_COUNT);

		util::LockGuard<util::Condition> guard(watcher.condition_);
		for (uint32_t i = 0; !watcher.completed_; i++) {
			if (i > 0) {
				const int64_t now = ee_.clockGenerator_->getMonotonicTime();
				int64_t queueingElapsed;
				int64_t handlerElapsed;
				if (watcher.handlerStartTime_ < 0) {
					queueingElapsed = now - queueingTime;
					handlerElapsed = 0;
				}
				else {
					queueingElapsed = watcher.handlerStartTime_ - queueingTime;
					handlerElapsed = now - watcher.handlerStartTime_;
				}
				GS_TRACE_WARNING(EVENT_ENGINE, GS_TRACE_EE_WAIT_COMPLETION,
						"(queueingElapsed=" << queueingElapsed <<
						", handlerElapsed=" << handlerElapsed <<
						", watcherEngine=" << ec.getEngine().getName() <<
						", watchedEngine=" << ee_.getName() <<
						", eventType=" << ev.getType() << ")");
			}
			else {
				watcher.condition_.wait(0);
				if (watcher.completed_) {
					break;
				}
			}
			watcher.condition_.wait(intervalMillis);
		}

		const int64_t waitTime = ee_.clockGenerator_->getMonotonicTime() -
				queueingTime;
		stats.updateMax(Stats::WORKER_WAIT_TIME_MAX, waitTime);
		stats.merge(Stats::WORKER_WAIT_TIME_TOTAL, waitTime);

		if (watcher.lastException_ != NULL) {
			throw *watcher.lastException_;
		}
	}
	catch (std::exception &e) {
		ev.setProgressWatcher(NULL);
		worker.removeWatcher(watcher);

		stats.increment(Stats::WORKER_WAIT_ERROR_COUNT);

		stats.increment(Stats::WORKER_HANDLING_THREAD_COUNT);
		stats.decrement(Stats::WORKER_WAITING_THREAD_COUNT);

		GS_RETHROW_USER_OR_SYSTEM(e,
				"Failed to request and wait event ("
				"watcherEngine=" << ec.getEngine().getName() <<
				", watchedEngine=" << ee_.getName() <<
				", eventType=" << ev.getType() <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}

	stats.increment(Stats::WORKER_HANDLING_THREAD_COUNT);
	stats.decrement(Stats::WORKER_WAITING_THREAD_COUNT);

	ev.setProgressWatcher(NULL);
	worker.removeWatcher(watcher);
}


void EventEngine::Dispatcher::execute(EventContext &ec, Event &ev) {
	HandlerEntry *entry = findHandlerEntry(ev.getType(), true);

	if (entry == NULL) {
		handleUnknownEvent(ec, ev);
		return;
	}

	handleEvent(ec, ev, *entry);
}

void EventEngine::Dispatcher::handleLocalEvent(
		EventContext &ec, LocalEventType eventType) {
	if (localHandler_ == NULL) {
		return;
	}

	Event ev(ec, eventType,
			ee_.pgConfig_->getGroupBeginPartitionId(ec.getWorkerId()));

	try {
		util::StackAllocator::Scope scope(ec.getAllocator());

		(*localHandler_)(ec, ev);

		Manipulator::getStats(ec).increment(Stats::WORKER_LOCAL_EVENT_COUNT);
	}
	catch (...) {
		std::exception e;
		handleThreadError(ec, e, false);
	}

	ec.getAllocator().trim();
}

void EventEngine::Dispatcher::handleDisconnection(
		EventContext &ec, Event &ev) {
	if (disconnectHandler_ == NULL) {
		return;
	}

	HandlerEntry entry;
	entry.handler_ = disconnectHandler_;

	handleEvent(ec, ev, entry);
}

EventEngine::EventWorker& EventEngine::Dispatcher::selectWorker(
		const Event &ev, const EventHandlingMode *mode) {
	const PartitionId pId = ev.getPartitionId();
	const PartitionGroupConfig &pgConfig = *ee_.pgConfig_;

	const EventHandlingMode *resolvedMode = mode;
	if (resolvedMode == NULL) {
		HandlerEntry *entry = findHandlerEntry(ev.getType(), true);
		if (entry != NULL) {
			resolvedMode = &entry->mode_;
		}
	}

	if (pId == UNDEF_PARTITIONID) {
		if (resolvedMode != NULL &&
				*resolvedMode == HANDLING_PARTITION_SERIALIZED) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Partition ID unspecified ("
					"eventType=" << ev.getType() <<
					", handlingMode=" <<
					static_cast<int32_t>(*resolvedMode) << ")");
		}

		return ee_.eventWorkerList_[
				++workerSelectionSeed_ % pgConfig.getPartitionGroupCount()];
	}
	else {
		if (resolvedMode != NULL && *resolvedMode == HANDLING_QUEUEING) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Partition ID must be undefined ("
					"eventType=" << ev.getType() <<
					", pId=" << ev.getPartitionId() <<
					", handlingMode=" <<
					static_cast<int32_t>(*resolvedMode) << ")");
		}

		const PartitionGroupId pgId =
				pgConfig.getPartitionGroupId(ev.getPartitionId());

		if (pgId >= pgConfig.getPartitionGroupCount()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Partition group ID out of range ("
					"eventType=" << ev.getType() <<
					", pId=" << ev.getPartitionId() << ", pgId=" << pgId <<
					", groupCount=" << pgConfig.getPartitionGroupCount() << ")");
		}

		return ee_.eventWorkerList_[pgId];
	}
}

void EventEngine::Dispatcher::handleThreadError(
		EventContext &ec, std::exception &e, bool internalError) throw() {
	if (threadErrorHandler_ == NULL) {
		UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
	}
	else {
		if (internalError) {
			UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
		}
		try {
			(*threadErrorHandler_)(ec, e);
		}
		catch (...) {
			std::exception e;
			UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
		}
	}

	try {
		ee_.shutdown();
	}
	catch (...) {
		std::exception e;
		UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
	}

	ee_.stats_->set(Stats::MAIN_THREAD_ERROR_COUNT, ++threadErrorCount_);
}

void EventEngine::Dispatcher::handleUnexpectedShutdownError(
		std::exception &e, const NodeDescriptor &nd) {
	util::TraceOption::Level level;
	if (closeHandlerCount_ > 0 &&
			!nd.isEmpty() && nd.getType() == NodeDescriptor::ND_TYPE_CLIENT) {
		level = util::TraceOption::LEVEL_WARNING;
	}
	else {
		level = util::TraceOption::LEVEL_INFO;
	}

	UTIL_TRACER_PUT_BASE_CODED(
			EVENT_ENGINE, level,
			GS_TRACE_NAMED_CODE(GS_TRACE_EE_UNEXPECTED_SHUTDOWN),
			"Unexpected shutdown occurred (engine=" << ee_.getName() <<
			", nd=" << nd << ")", &e);
}

EventEngine::EventCoder& EventEngine::Dispatcher::getEventCoder() {
	assert(eventCoder_ != NULL);

	return *eventCoder_;
}


void EventEngine::Dispatcher::handleEvent(
		EventContext &ec, Event &ev, HandlerEntry &entry) {

	try {
		ec.setHandlerStartTime(ee_.clockGenerator_->getCurrent());
		ec.setHandlerStartMonotonicTime(
				ee_.clockGenerator_->getMonotonicTime());
	}
	catch (...) {
		if (!ee_.clockGenerator_->isShutdownRequested()) {
			throw;
		}
	}

	EventProgressWatcher *watcher = ev.getProgressWatcher();

	try {
		if (watcher != NULL) {
			EventProgressWatcher::setHandlerStartTime(
					watcher, ec.getHandlerStartMonotonicTime());
		}

		util::StackAllocator::Scope scope(ec.getAllocator());

		(*entry.handler_)(ec, ev);

		EventProgressWatcher::setCompleted(watcher);

		Manipulator::getStats(ec).increment(Stats::WORKER_HANDLED_EVENT_COUNT);
	}
	catch (...) {
		std::exception e;
		handleThreadError(ec, e, false);
		EventProgressWatcher::setLastException(watcher, e);
	}

	if (watcher != NULL) {
		EventProgressWatcher::release(ev);
	}

	ec.getAllocator().trim();
}

void EventEngine::Dispatcher::handleUnknownEvent(EventContext &ec, Event &ev) {
	if (unknownEventHandler_ == NULL) {
		GS_TRACE_WARNING(EVENT_ENGINE, GS_TRACE_EE_UNKNOWN_EVENT,
				"(type=" << ev.getType() <<
				", engine=" << ee_.getName() << ")");
		Manipulator::getStats(ec).increment(Stats::WORKER_HANDLED_EVENT_COUNT);
	}
	else {
		HandlerEntry entry;
		entry.handler_ = unknownEventHandler_;

		handleEvent(ec, ev, entry);
	}

	Manipulator::getStats(ec).increment(Stats::WORKER_UNKNOWN_EVENT_COUNT);
}

void EventEngine::Dispatcher::setHandlerEntry(
		EventType type, HandlerEntry entry) {

	HandlerEntry *destEntry = findHandlerEntry(type, false);

	if (destEntry == NULL) {
		if (type < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Event type out of range (type=" << type << ")");
		}
		handlerTable_.resize(static_cast<size_t>(type) + 1);

		destEntry = findHandlerEntry(type, false);
	}

	*destEntry = entry;
}

EventEngine::Dispatcher::HandlerEntry*
EventEngine::Dispatcher::findHandlerEntry(
		EventType type, bool handlerRequired) {

	if (type < 0 || type >= static_cast<EventType>(handlerTable_.size())) {
		return NULL;
	}

	HandlerEntry &entry = handlerTable_[static_cast<size_t>(type)];
	if (handlerRequired && entry.handler_ == NULL) {
		return NULL;
	}

	return &entry;
}

void EventEngine::Dispatcher::filterImmediatePartitionId(Event &ev) {
	const PartitionId filteredId = ee_.config_->filteredPartitionId_;

	if (filteredId == UNDEF_PARTITIONID) {
		if (ev.getPartitionId() >= ee_.pgConfig_->getPartitionCount()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Protocol error by invalid partition ID ("
					"partitionId=" << ev.getPartitionId() <<
					", type=" << ev.getType() <<
					", nd=" << ev.getSenderND() << ")");
		}
	}
	else {
		ev.setPartitionId(filteredId);
	}
}



EventEngine::Dispatcher::HandlerEntry::HandlerEntry() :
		handler_(NULL),
		mode_(HANDLING_PARTITION_SERIALIZED),
		closeMode_(false) {
}


EventEngine::Limitter::Limitter(EventEngine &ee) :
		ee_(ee),
		ioBufferLimit_(maskLimitValue(ee_.config_->sendBufferSizeLimit_)),
		eventBufferLimit_(maskLimitValue(ee_.config_->eventBufferSizeLimit_)) {
}

int64_t EventEngine::Limitter::getEventBufferLimit() const {
	return eventBufferLimit_;
}

void EventEngine::Limitter::reportIOBufferDiff(int64_t diff) {
	apply(ioBufferSize_, diff, ioBufferLimit_);
}

void EventEngine::Limitter::reportEventBufferDiff(int64_t diff) {
	apply(eventBufferSize_, diff, eventBufferLimit_);
}

void EventEngine::Limitter::apply(
		util::Atomic<int64_t> &value, int64_t diff, int64_t limit) {

	const int64_t cur = (value += diff);
	const int64_t prev = cur - diff;

	const bool prevExceeded = (prev > limit);
	const bool curExceeded = (cur > limit);

	if (prevExceeded ^ curExceeded) {
		const uint32_t ioConcurrency =
				Manipulator::getIOConcurrency(*ee_.config_);

		for (uint32_t i = 0; i < ioConcurrency; i++) {
			ee_.ioWorkerList_[i].suspend(curExceeded);
		}
	}
}

int64_t EventEngine::Limitter::maskLimitValue(int64_t value) {
	return (value < 0 ? std::numeric_limits<int64_t>::max() : value);
}


NodeDescriptor::Body& EventEngine::Manipulator::getNDBody(
		const NodeDescriptor &nd) {
	assert(nd.body_ != NULL);

	return *nd.body_;
}

util::SocketAddress EventEngine::Manipulator::resolveListenAddress(
		const Config &config) {
	if (config.listenAddress_.isEmpty()) {
		if (config.serverAddress_.isEmpty()) {
			return config.listenAddress_;
		}

		util::SocketAddress address = config.listenAddress_;
		address.assign(NULL,
				config.serverAddress_.getPort(),
				config.serverAddress_.getFamily());

		return address;
	}

	return config.listenAddress_;
}

bool EventEngine::Manipulator::isListenerEnabled(const Config &config) {
	if (!config.listenAddress_.isEmpty() && config.serverAddress_.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Server address is empty ("
				"listenAddress=" << config.listenAddress_ << ")");
	}

	return !config.serverAddress_.isEmpty();
}

uint32_t EventEngine::Manipulator::getIOConcurrency(const Config &config) {
	if (!isListenerEnabled(config) && config.multicastAddress_.isEmpty()) {
		return 0;
	}

	return std::max<uint32_t>(1, static_cast<uint32_t>(
			config.ioConcurrencyRate_ * config.concurrency_));
}

EventEngine::Stats& EventEngine::Manipulator::getStats(
		const EventContext &ec) {
	return ec.workerStats_;
}


void EventEngine::Manipulator::mergeAllocatorStats(
		Stats &stats, VariableSizeAllocator &alloc) {

	stats.merge(Stats::MAIN_TOTAL_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getTotalElementSize()));
	stats.merge(Stats::MAIN_FREE_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getFreeElementSize()));

	stats.merge(Stats::MAIN_HUGE_ALLOC_COUNT,
			static_cast<int64_t>(alloc.getHugeElementCount()));
	stats.merge(Stats::MAIN_HUGE_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getHugeElementSize()));
}

void EventEngine::Manipulator::mergeAllocatorStats(
		Stats &stats, util::StackAllocator &alloc) {

	stats.merge(Stats::MAIN_TOTAL_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getTotalSize()));
	stats.merge(Stats::MAIN_FREE_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getFreeSize()));

	stats.merge(Stats::MAIN_HUGE_ALLOC_COUNT,
			static_cast<int64_t>(alloc.getHugeCount()));
	stats.merge(Stats::MAIN_HUGE_ALLOC_SIZE,
			static_cast<int64_t>(alloc.getHugeSize()));
}

void EventEngine::Manipulator::prepareMulticastSocket(
	const Config &config, NDPool &ndPool, SocketPool &socketPool) {
	NodeDescriptor multicastND = ndPool.getMulticastND();
	if (multicastND.isEmpty()) {
		return;
	}

	NodeDescriptor::Body &body = getNDBody(multicastND);
	LockGuard guard(body.getLock());

	SocketReference multicastSocket(socketPool.allocate(), &guard);
	if (!multicastSocket.get()->openAsMulticast(config.multicastAddress_)) {
		return;
	}

	EventSocket *socket = multicastSocket.release();
	body.setSocket(guard, socket, NodeDescriptor::Body::ND_SOCKET_RECEIVER);
}

size_t EventEngine::Manipulator::getHandlingEventCount(
		const EventContext &ec, const Event *ev, bool includesStarted) {
	const EventList *list = ec.eventList_;
	if (list == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED, "");
	}

	size_t count = list->size();

	if (!includesStarted) {
		assert(ev != NULL);

		for (EventList::const_iterator it = list->begin();
				it != list->end(); ++it) {
			count--;

			if (*it == ev) {
				break;
			}
		}
	}

	return count;
}


EventEngine::ClockGenerator::ClockGenerator(EventEngine &ee) :
		ee_(ee),
		started_(false),
		runnable_(true) {
	const uint32_t interval = ee_.config_->clockIntervalMillis_;
	if (interval <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Too small clock interval (interval=" << interval << ")");
	}
}

EventEngine::ClockGenerator::~ClockGenerator() {
}

void EventEngine::ClockGenerator::run() {
	const uint32_t interval = ee_.config_->clockIntervalMillis_;
	const std::pair<int64_t, int64_t> threshold(0, interval * 1000);
	const bool trimMilliseconds = false;

	EventEngine::VariableSizeAllocator varAllocator(
			util::AllocatorInfo(ee_.config_->allocatorGroupId_, "eeClockVar"));
	util::StackAllocator allocator(
			util::AllocatorInfo(ee_.config_->allocatorGroupId_, "eeClockStack"),
			ee_.fixedAllocator_);
	Stats stats;
	EventContext ec(EventContext::Source(varAllocator, allocator, stats));

	try {
		while (runnable_) {
			generateCorrectedTime();

			while (runnable_) {
				const util::DateTime last = correctedTime_.load();
				util::Thread::sleep(interval);

				const util::DateTime now = util::DateTime::now(trimMilliseconds);
				const int64_t diff = now.getUnixTime() - last.getUnixTime();
				if (now == util::DateTime() ||
						diff < threshold.first || threshold.second < diff) {
					GS_TRACE_ERROR(EVENT_ENGINE, GS_TRACE_EE_TIME_DIFF_ERROR,
							"(now=" << now.getUnixTime() << ", "
							"last=" << last.getUnixTime() << ", "
							"diff=" << diff << ")");

					ee_.stats_->increment(Stats::CLOCK_UPDATE_ERROR_COUNT);

					break;
				}

				util::LockGuard<util::Condition> guard(condition_);

				correctedTime_ = now.getUnixTime();
				monotonicTime_ += interval;
				condition_.signal();
			}
		}
	}
	catch (std::exception &e) {
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}
	catch (...) {
		std::exception e;
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		shutdown();
	}
	catch (...) {
		std::exception e;
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}
}

void EventEngine::ClockGenerator::start() {
	{
		util::LockGuard<util::Condition> guard(condition_);

		if (started_) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
					"Already started");
		}

		started_ = true;
		condition_.signal();
	}

	thread_.start(this);
}

void EventEngine::ClockGenerator::shutdown() {
	util::LockGuard<util::Condition> guard(condition_);

	runnable_ = false;
	condition_.signal();
}

void EventEngine::ClockGenerator::waitForShutdown() {
	thread_.join();
}

bool EventEngine::ClockGenerator::isShutdownRequested() {
	util::LockGuard<util::Condition> guard(condition_);
	return (started_ && !runnable_);
}

util::DateTime EventEngine::ClockGenerator::getCurrent() {
	util::DateTime time = correctedTime_.load();

	if (time == util::DateTime()) {
		const uint32_t timeout = 1000;

		util::LockGuard<util::Condition> guard(condition_);

		while ((time = correctedTime_.load()) == util::DateTime()) {
			if (!started_ || !runnable_) {
				GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED, "");
			}

			condition_.wait(timeout);
		}
	}

	return time;
}

EventMonotonicTime EventEngine::ClockGenerator::getMonotonicTime() {
	return monotonicTime_;
}

void EventEngine::ClockGenerator::generateCorrectedTime() {
	const int32_t maxTrial = ee_.config_->clockCorrectionMaxTrial_;
	const uint32_t interval = ee_.config_->clockIntervalMillis_;
	const std::pair<int64_t, int64_t> threshold(0, interval * 1000);
	const int64_t duration = ee_.config_->clockCorrectionMillis_;
	const int64_t checkCount = duration / interval;
	const bool trimMilliseconds = false;

	util::DateTime newCorrectedTime;

	for (int32_t trial = 1; runnable_; trial++) {
		util::DateTime last = util::DateTime::now(trimMilliseconds);

		try {
			for (int64_t i = 0; runnable_; i++) {
				util::Thread::sleep(interval);

				const util::DateTime now = util::DateTime::now(trimMilliseconds);
				const int64_t diff = now.getUnixTime() - last.getUnixTime();

				if (now == util::DateTime() ||
						diff < threshold.first || threshold.second < diff) {

					ee_.stats_->increment(Stats::CLOCK_INITIALIZE_ERROR_COUNT);

					GS_THROW_USER_ERROR(
							GS_ERROR_EE_OPERATION_NOT_ALLOWED,
							"Initial time diff error ("
							"now=" << now.getUnixTime() << ", "
							"last=" << last.getUnixTime() << ", "
							"diff=" << diff << ", "
							"limit=" << threshold.second << ", "
							"trial=" << trial << ", "
							"count=" << i << ")");
				}

				if (i >= checkCount) {
					newCorrectedTime = now;
					break;
				}
				last = now;
			}
			break;
		}
		catch (std::exception &e) {
			if (trial > maxTrial) {
				throw;
			}
			UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
		}
	}

	ee_.stats_->increment(Stats::CLOCK_INITIALIZE_COUNT);

	util::LockGuard<util::Condition> guard(condition_);

	correctedTime_ = newCorrectedTime.getUnixTime();
	condition_.signal();
}


EventEngine::Listener::Listener(EventEngine &ee) :
		ee_(ee),
		runnable_(true) {
	poll_.setInterruptible();
}

EventEngine::Listener::~Listener() {
}

void EventEngine::Listener::run() {
	const uint32_t maxRetryCount = 100;
	const uint32_t retryIntervalMillis = 1000;

	const uint32_t pollTimeout = 10 * 1000;

	EventEngine::VariableSizeAllocator varAllocator(
			util::AllocatorInfo(ee_.config_->allocatorGroupId_, "eeListenerVar"));
	util::StackAllocator allocator(
			util::AllocatorInfo(ee_.config_->allocatorGroupId_, "eeListenerStack"),
			ee_.fixedAllocator_);
	Stats stats;
	EventContext ec(EventContext::Source(varAllocator, allocator, stats));

	try {
		const util::SocketAddress address =
				Manipulator::resolveListenAddress(*ee_.config_);

		listenerSocket_.open(
				address.getFamily(), util::Socket::TYPE_STREAM);

		listenerSocket_.setBlockingMode(false);
		listenerSocket_.setReuseAddress(true);

		for (uint32_t i = 1; runnable_; i++) {
			try {
				listenerSocket_.bind(address);
				break;
			}
			catch (std::exception &e) {
				ee_.stats_->increment(Stats::LISTENER_BIND_ERROR_COUNT);

				if (i <= maxRetryCount) {
					if (i == 0) {
						UTIL_TRACE_EXCEPTION_WARNING(
								EVENT_ENGINE, e, "Retrying socket bind ("
								"engine=" << ee_.getName() <<
								", address=" << address <<
								", maxRetryCount=" << maxRetryCount <<
								", retryIntervalMillis=" << retryIntervalMillis <<
								", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
					}
				}
				else {
					GS_RETHROW_USER_OR_SYSTEM(e, "Socket bind failed ("
							"address=" << address << ", repeat=" << i <<
							", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}
			}

			util::Thread::sleep(retryIntervalMillis);
		}

		ee_.stats_->increment(Stats::LISTENER_BIND_COUNT);

		listenerSocket_.listen(ee_.config_->backlogSize_);

		poll_.add(this, util::IOPollEvent::TYPE_READ);

		while (runnable_) {
			poll_.dispatch(pollTimeout);
		}

		listenerSocket_.listen(0);
		poll_.remove(this);
		listenerSocket_.close();
	}
	catch (std::exception &e) {
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}
	catch (...) {
		std::exception e;
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		shutdown();
	}
	catch (...) {
		std::exception e;
		ee_.dispatcher_->handleThreadError(ec, e, true);
	}
}

void EventEngine::Listener::handlePollEvent(
		util::IOPollBase*, util::IOPollEvent) {

	util::Socket socket;
	util::SocketAddress address;

	for (;;) {
		try {
			if (!listenerSocket_.accept(&socket, &address)) {
				break;
			}

			SocketReference socketRef(ee_.socketPool_->allocate(), NULL);

			if (!socketRef.get()->openAsServer(socket, address)) {
				break;
			}

			socketRef.release();
		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e,
					"Listener error (engine=" << ee_.getName() <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			ee_.stats_->increment(Stats::IO_ERROR_OTHER_COUNT);
		}
	}
}

util::File& EventEngine::Listener::getFile() {
	return listenerSocket_;
}

void EventEngine::Listener::start() {
	thread_.start(this);
}

void EventEngine::Listener::shutdown() {
	runnable_ = false;
	poll_.interrupt();
}

void EventEngine::Listener::waitForShutdown() {
	thread_.join();
}


EventEngine::IOWorker::IOWorker() :
		ee_(NULL),
		id_(0),
		runnable_(true),
		suspended_(false),
		suspendedLocal_(false) {
	poll_.setInterruptible();
	stats_.set(Stats::WORKER_STOPPED_THREAD_COUNT, 1);
}

EventEngine::IOWorker::~IOWorker() try {
	discardAllSockets();
}
catch (...) {
}

void EventEngine::IOWorker::initialize(EventEngine &ee, uint32_t id) {
	assert(ee_ == NULL);

	sharedVarAllocator_.reset(UTIL_NEW VariableSizeAllocator(
			util::AllocatorInfo(ee.config_->allocatorGroupId_, "ioSharedVar")));
	localVarAllocator_.reset(UTIL_NEW VariableSizeAllocator(
			util::AllocatorInfo(ee.config_->allocatorGroupId_, "ioLocalVar")));

	allocator_.reset(UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ee.config_->workAllocatorGroupId_, "ioStack"),
			ee.fixedAllocator_));
	allocator_->setFreeSizeLimit(ee.fixedAllocator_->getElementSize());

	EventContext::Source source(*localVarAllocator_, *allocator_, stats_);
	source.ee_ = &ee;
	source.workerId_ = id;
	source.onIOWorker_ = true;
	localEventContext_.reset(UTIL_NEW EventContext(source));

	operationMap_.reset(UTIL_NEW OperationMap(
			OperationMap::key_compare(), *sharedVarAllocator_));

	workingOperationMap_.reset(UTIL_NEW OperationMap(
			OperationMap::key_compare(), *sharedVarAllocator_));

	socketMap_.reset(UTIL_NEW SocketMap(
			SocketMap::key_compare(), *localVarAllocator_));

	bufferQueuePool_.reset(UTIL_NEW util::ObjectPool<BufferQueue>(
			util::AllocatorInfo(ee.config_->allocatorGroupId_, "ioBufferQueue")));

	ee_ = &ee;
	id_ = id;
}

void EventEngine::IOWorker::run() {
	assert(ee_ != NULL);
	assert(localEventContext_.get() != NULL);

	const uint32_t maxPollTimeout = 10 * 1000;

	stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 1);

	EventContext &ec = *localEventContext_;

	try {
		ee_->dispatcher_->handleLocalEvent(ec, LOCAL_IO_WORKER_STARTED);

		OperationMap &operationMap = *workingOperationMap_;

		SocketList discardingList(*localVarAllocator_);

		SocketMap reconnectingMap(
				SocketMap::key_compare(), *localVarAllocator_);
		SocketMap suspendedMap(
				SocketMap::key_compare(), *localVarAllocator_);

		for (;;) {
			{
				LockGuard guard(mutex_);

				if (!runnable_) {
					break;
				}

				operationMap.clear();
				operationMap.swap(*operationMap_);
				suspendedLocal_ = suspended_;
			}

			for (SocketList::iterator it = discardingList.begin();
					it != discardingList.end();) {
				operationMap.erase(*it);
				socketMap_->erase(*it);

				discardSocket(*it, false);
				it = discardingList.erase(it);
			}

			for (OperationMap::iterator it = operationMap.begin();
					it != operationMap.end(); ++it) {
				EventSocket *socket = it->first;
				const SocketOperationEntry &entry = it->second;

				bool discarding = false;
				try {
					applyPolling(socket, entry.second);
					reconnectingMap.erase(socket);
					suspendedMap.erase(socket);

					switch (entry.first) {
					case SOCKET_OPERATION_RECONNECT:
						reconnectingMap[socket] = true;
						break;
					case SOCKET_OPERATION_ADD:
						break;
					case SOCKET_OPERATION_REMOVE:
						discarding = true;
						break;
					case SOCKET_OPERATION_MODIFY:
						break;
					case SOCKET_OPERATION_SUSPEND:
						suspendedMap[socket] = true;
						break;
					case SOCKET_OPERATION_NONE:
					default:
						assert(false);
						break;
					}
				}
				catch (std::exception &e) {
					UTIL_TRACE_EXCEPTION(EVENT_ENGINE, e, "");
					discarding = true;
				}

				if (discarding) {
					socket->handleDisconnectionLocal();

					applyPolling(socket, util::IOPollEvent());
					socket->closeLocal(true, NULL);

					reconnectingMap.erase(socket);
					suspendedMap.erase(socket);

					discardingList.push_back(socket);
				}
			}
			stats_.set(Stats::IO_MANAGED_SOCKET_COUNT, socketMap_->size());

			if (!suspendedLocal_) {
				for (SocketMap::iterator it = suspendedMap.begin();
						it != suspendedMap.end(); ++it) {
					EventSocket *socket = it->first;
					applyPolling(socket, util::IOPollEvent::TYPE_READ);
					socket->resumeLocal();
				}
				suspendedMap.clear();
			}
			stats_.set(
					Stats::IO_SUSPENDING_SOCKET_COUNT, suspendedMap.size());

			uint32_t pollTimeout = maxPollTimeout;
			if (!reconnectingMap.empty()) {
				SocketMap &map = reconnectingMap;

				for (SocketMap::iterator it = map.begin(); it != map.end();) {
					const bool polling = (*socketMap_)[it->first];

					if (it->first->reconnectLocal(polling)) {
						map.erase(it++);
					}
					else {
						++it;
					}
				}
				pollTimeout = ee_->config_->reconnectIntervalMillis_;
			}
			stats_.set(
					Stats::IO_CONNECTING_SOCKET_COUNT, reconnectingMap.size());

			stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 0);
			stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 1);

			poll_.dispatch(pollTimeout);

			stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 1);
			stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 0);

			stats_.increment(Stats::IO_CYCLE_COUNT);
		}
	}
	catch (std::exception &e) {
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		ee_->dispatcher_->handleLocalEvent(ec, LOCAL_IO_WORKER_TERMINATING);
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		stats_.set(Stats::WORKER_STOPPED_THREAD_COUNT, 1);
		stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 0);
		stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 0);
		stats_.set(Stats::WORKER_WAITING_THREAD_COUNT, 0);

		discardAllSockets();
	}
	catch (std::exception &e) {
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}
}

void EventEngine::IOWorker::start() {
	assert(ee_ != NULL);

	thread_.start(this);
}

void EventEngine::IOWorker::shutdown() {
	LockGuard guard(mutex_);
	runnable_ = false;
	poll_.interrupt();
}

void EventEngine::IOWorker::waitForShutdown() {
	thread_.join();
}

void EventEngine::IOWorker::suspend(bool enabled) {
	assert(ee_ != NULL);

	LockGuard guard(mutex_);
	suspended_ = enabled;
	poll_.interrupt();
}

bool EventEngine::IOWorker::isShutdown() {
	LockGuard guard(mutex_);
	return !runnable_;
}

bool EventEngine::IOWorker::isSuspendedLocal() {
	assert(ee_ != NULL);

	return suspendedLocal_;
}

EventContext& EventEngine::IOWorker::getLocalEventContext() {
	return *localEventContext_;
}

EventEngine::VariableSizeAllocator&
EventEngine::IOWorker::getVariableSizeAllocator() {
	return *localVarAllocator_;
}

EventEngine::Buffer& EventEngine::IOWorker::pushBufferQueue(
		BufferQueue *&queue, size_t size) {
	LockGuard guard(mutex_);

	if (queue == NULL) {
		queue = UTIL_OBJECT_POOL_NEW(*bufferQueuePool_)
				BufferQueue(*sharedVarAllocator_);
	}

	queue->push_back(Buffer(*sharedVarAllocator_));

	Buffer &buffer = queue->back();
	buffer.getWritableXArray().reserve(size);

	return buffer;
}

void EventEngine::IOWorker::popBufferQueue(BufferQueue &queue) {
	LockGuard guard(mutex_);

	queue.pop_front();
}

void EventEngine::IOWorker::releaseBufferQueue(BufferQueue *&queue) {
	LockGuard guard(mutex_);

	UTIL_OBJECT_POOL_DELETE(*bufferQueuePool_, queue);
	queue = NULL;
}

bool EventEngine::IOWorker::reconnectSocket(
		EventSocket *socket, util::IOPollEvent pollEvent) {
	LockGuard guard(mutex_);

	if (!runnable_) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return false;
	}

	SocketOperationEntry &entry = (*operationMap_)[socket];
	if (entry.first != SOCKET_OPERATION_NONE &&
			entry.first != SOCKET_OPERATION_RECONNECT) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Socket already exists");
	}

	entry = SocketOperationEntry(SOCKET_OPERATION_RECONNECT, pollEvent);

	poll_.interrupt();
	stats_.increment(Stats::IO_REQUEST_CONNECT_COUNT);
	return true;
}

bool EventEngine::IOWorker::addSocket(
		EventSocket *socket, util::IOPollEvent pollEvent) {
	LockGuard guard(mutex_);

	if (!runnable_) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return false;
	}

	SocketOperationEntry &entry = (*operationMap_)[socket];
	if (entry.first != SOCKET_OPERATION_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Socket already exists");
	}

	entry = SocketOperationEntry(SOCKET_OPERATION_ADD, pollEvent);

	poll_.interrupt();
	stats_.increment(Stats::IO_REQUEST_ADD_COUNT);
	return true;
}

void EventEngine::IOWorker::removeSocket(EventSocket *socket) {
	LockGuard guard(mutex_);

	if (!runnable_) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return;
	}

	(*operationMap_)[socket] = SocketOperationEntry(
			SOCKET_OPERATION_REMOVE, util::IOPollEvent());

	poll_.interrupt();
	stats_.increment(Stats::IO_REQUEST_REMOVE_COUNT);
}

void EventEngine::IOWorker::modifySocket(
		EventSocket *socket, util::IOPollEvent pollEvent) {
	LockGuard guard(mutex_);

	if (!runnable_) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return;
	}

	SocketOperationEntry &entry = (*operationMap_)[socket];
	if (entry.first == SOCKET_OPERATION_REMOVE) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return;
	}

	if (entry.first == SOCKET_OPERATION_RECONNECT ||
			entry.first == SOCKET_OPERATION_ADD) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Socket not polled");
	}

	entry = SocketOperationEntry(SOCKET_OPERATION_MODIFY, pollEvent);

	poll_.interrupt();
	stats_.increment(Stats::IO_REQUEST_MODIFY_COUNT);
}

void EventEngine::IOWorker::suspendSocket(
		EventSocket *socket, util::IOPollEvent pollEvent) {
	LockGuard guard(mutex_);

	SocketOperationEntry &entry = (*operationMap_)[socket];
	if (entry.first == SOCKET_OPERATION_RECONNECT ||
			entry.first == SOCKET_OPERATION_ADD) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Socket not polled");
	}

	if (entry.first == SOCKET_OPERATION_REMOVE ||
			entry.first == SOCKET_OPERATION_MODIFY) {
		stats_.increment(Stats::IO_REQUEST_CANCEL_COUNT);
		return;
	}

	entry = SocketOperationEntry(SOCKET_OPERATION_SUSPEND, pollEvent);

	poll_.interrupt();
	stats_.increment(Stats::IO_REQUEST_SUSPEND_COUNT);
}

const EventEngine::Stats& EventEngine::IOWorker::getStats() const {
	return stats_;
}

void EventEngine::IOWorker::mergeExtraStats(Stats &stats) {
	Manipulator::mergeAllocatorStats(stats, *sharedVarAllocator_);
	Manipulator::mergeAllocatorStats(stats, *localVarAllocator_);
	Manipulator::mergeAllocatorStats(stats, *allocator_);
}

void EventEngine::IOWorker::updateReceiveBufferSize(
		const Buffer &buffer, bool adding) {
	const int64_t diff = static_cast<int64_t>(buffer.getCapacity());
	const int64_t size = stats_.get(Stats::IO_RECEIVE_BUFFER_SIZE_CURRENT) +
			(adding ? diff : -diff);

	stats_.set(Stats::IO_RECEIVE_BUFFER_SIZE_CURRENT, size);
	stats_.updateMax(Stats::IO_RECEIVE_BUFFER_SIZE_MAX, size);
}

void EventEngine::IOWorker::updateSendBufferSize(
		const Buffer &buffer, bool adding) {
	const int64_t diff = static_cast<int64_t>(buffer.getCapacity()) *
			(adding ? 1 : -1);
	const int64_t size = (sendBufferSize_ += diff);

	stats_.set(Stats::IO_SEND_BUFFER_SIZE_CURRENT, size);
	stats_.updateMax(Stats::IO_SEND_BUFFER_SIZE_MAX, size);

	ee_->limitter_->reportIOBufferDiff(diff);
}

void EventEngine::IOWorker::incrementErrorCount(Stats::Type type) {
	const int32_t begin = STATS_IO_ERROR_BEGIN;
	const int32_t end = STATS_IO_ERROR_END;

	if (!(begin <= type && type < end)) {
		assert(false);
		return;
	}

	stats_.set(type, ++errorCount_[type - begin]);
}

void EventEngine::IOWorker::applyPolling(
		EventSocket *socket, util::IOPollEvent pollEvent) {
	SocketMap::iterator it = socketMap_->find(socket);
	if (it == socketMap_->end()) {
		if (pollEvent != util::IOPollEvent()) {
			SocketMap::iterator it =
					socketMap_->insert(SocketMapEntry(socket, false)).first;
			poll_.add(socket, pollEvent);
			it->second = true;

			stats_.increment(Stats::IO_POLLING_SOCKET_COUNT);
		}
	}
	else if (it->second) {
		if (pollEvent == util::IOPollEvent()) {
			poll_.remove(socket);
			it->second = false;

			stats_.decrement(Stats::IO_POLLING_SOCKET_COUNT);
		}
		else {
			poll_.modify(socket, pollEvent);
		}
	}
	else {
		if (pollEvent != util::IOPollEvent()) {
			poll_.add(socket, pollEvent);
			it->second = true;

			stats_.increment(Stats::IO_POLLING_SOCKET_COUNT);
		}
	}
}

void EventEngine::IOWorker::discardAllSockets() {
	{
		LockGuard guard(mutex_);
		runnable_ = false;

	}

	for (OperationMap::iterator it = operationMap_->begin();
			it != operationMap_->end(); ++it) {
		EventSocket *socket = it->first;
		const SocketOperation operation = it->second.first;
		if (socketMap_->find(socket) == socketMap_->end() &&
				(operation == SOCKET_OPERATION_RECONNECT ||
				operation == SOCKET_OPERATION_ADD)) {
			discardSocket(socket, false);
		}
	}
	operationMap_->clear();

	for (SocketMap::iterator it = socketMap_->begin();
			it != socketMap_->end(); ++it) {
		EventEngine::EventSocket *socket = it->first;
		discardSocket(socket, it->second);
	}

	socketMap_->clear();

	stats_.set(Stats::IO_MANAGED_SOCKET_COUNT, 0);
}

void EventEngine::IOWorker::discardSocket(EventSocket *&socket, bool polled) {
	try {
		if (polled) {
			poll_.remove(socket);
			stats_.decrement(Stats::IO_POLLING_SOCKET_COUNT);
		}

		ee_->socketPool_->deallocate(socket, true, NULL);
		socket = NULL;
	}
	catch (...) {
		if (socket != NULL) {
			try {
				ee_->socketPool_->deallocate(socket, true, NULL);
			}
			catch (...) {
			}
			socket = NULL;
		}
		throw;
	}
}


EventEngine::EventWorker::EventWorker() :
		ee_(NULL), id_(0), started_(false), runnable_(true),
		bufferSizeSoftLimit_(std::numeric_limits<int64_t>::max()) {
}

EventEngine::EventWorker::~EventWorker() {
	if (activeQueue_.get() != NULL) {
		clearEvents(*sharedVarAllocator_, *activeQueue_);
	}
	if (pendingQueue_.get() != NULL) {
		clearEvents(*sharedVarAllocator_, *pendingQueue_);
	}
	if (conditionalQueue_.get() != NULL) {
		clearEvents(*sharedVarAllocator_, *conditionalQueue_);
	}
}

void EventEngine::EventWorker::initialize(EventEngine &ee, uint32_t id) {
	assert(ee_ == NULL);

	ee_ = &ee;
	id_ = id;

	sharedVarAllocator_.reset(UTIL_NEW VariableSizeAllocator(
			util::AllocatorInfo(ee.config_->allocatorGroupId_, "workerSharedVar")));
	localVarAllocator_.reset(UTIL_NEW VariableSizeAllocator(
			util::AllocatorInfo(ee.config_->allocatorGroupId_, "workerLocalVar")));

	allocator_.reset(UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ee.config_->workAllocatorGroupId_, "workerStack"),
			ee_->fixedAllocator_));

	activeQueue_.reset(UTIL_NEW ActiveQueue(*sharedVarAllocator_));
	pendingQueue_.reset(UTIL_NEW EventQueue(*sharedVarAllocator_));
	conditionalQueue_.reset(UTIL_NEW EventQueue(*sharedVarAllocator_));
	progressWatcher_.reset(UTIL_NEW EventProgressWatcher);
	watcherList_.reset(UTIL_NEW WatcherList(*sharedVarAllocator_));

	pendingPartitionCheckList_.resize(
			ee.pgConfig_->getGroupPartitonCount(id));

	assert(ee.config_->concurrency_ > 0);
	bufferSizeSoftLimit_ = ee.limitter_->getEventBufferLimit() /
			ee.config_->concurrency_;
}

void EventEngine::EventWorker::run() {
	assert(ee_ != NULL);

	const int64_t conditionTimeout = 10 * 1000;

	stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 1);

	EventList eventList(*localVarAllocator_);
	util::StackAllocator &allocator(*allocator_);
	allocator.setFreeSizeLimit(ee_->fixedAllocator_->getElementSize());
	EventContext ec(createContextSource(allocator, &eventList));

	try {
		ee_->dispatcher_->handleLocalEvent(ec, LOCAL_EVENT_WORKER_STARTED);

		for (;;) {
			{
				util::LockGuard<util::Condition> guard(condition_);

				for (;;) {

					if (!runnable_) {
						break;
					}

					int64_t diff = conditionTimeout;
					if (popActiveEvents(eventList, diff)) {
						break;
					}

					stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 0);
					stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 1);

					condition_.wait(
							static_cast<uint32_t>(std::min(conditionTimeout, diff)));

					stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 1);
					stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 0);
				}
			}

			if (eventList.empty()) {
				break;
			}

			for (EventList::iterator it = eventList.begin();
					it != eventList.end(); ++it) {
				ee_->dispatcher_->execute(ec, **it);
			}
			clearEvents(*localVarAllocator_, eventList);

			for (size_t lastSize = 0;;) {
				{
					util::LockGuard<util::Condition> guard(condition_);
					if (!popPendingEvents(ec, eventList, lastSize, true)) {
						break;
					}
				}

				for (EventList::iterator it = eventList.begin();
						it != eventList.end(); ++it) {
					ee_->dispatcher_->execute(ec, **it);
				}
				clearEvents(*localVarAllocator_, eventList);
			}
			stats_.increment(Stats::EVENT_CYCLE_COUNT);
		}
	}
	catch (std::exception &e) {
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		ee_->dispatcher_->handleLocalEvent(ec, LOCAL_EVENT_WORKER_TERMINATING);
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}

	try {
		clearEvents(*localVarAllocator_, eventList);

		stats_.set(Stats::WORKER_STOPPED_THREAD_COUNT, 1);
		stats_.set(Stats::WORKER_HANDLING_THREAD_COUNT, 0);
		stats_.set(Stats::WORKER_IDLE_THREAD_COUNT, 0);
		stats_.set(Stats::WORKER_WAITING_THREAD_COUNT, 0);

		shutdown();
	}
	catch (...) {
		std::exception e;
		ee_->dispatcher_->handleThreadError(ec, e, true);
	}
}

void EventEngine::EventWorker::start() {
	assert(ee_ != NULL);

	thread_.start(this);

	util::LockGuard<util::Condition> guard(condition_);
	started_ = true;
}

void EventEngine::EventWorker::shutdown() {
	assert(ee_ != NULL);

	util::LockGuard<util::Condition> guard(condition_);
	runnable_ = false;
	condition_.signal();

	if (!watcherList_->empty()) {
		try {
			GS_THROW_USER_ERROR(GS_ERROR_EE_REQUEST_SYNC_FAILED,
					"Event canceled by shutdown");
		}
		catch (std::exception &e) {
			for (WatcherList::iterator it = watcherList_->begin();
					it != watcherList_->end(); ++it) {
				EventProgressWatcher::setLastException(*it, e);
			}
		}
		watcherList_->clear();
	}
}

void EventEngine::EventWorker::waitForShutdown() {
	thread_.join();
}

void EventEngine::EventWorker::add(
		const Event &ev, int32_t timeoutMillis, int32_t periodicInterval) {
	assert(ee_ != NULL);

	if (ev.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Event is empty");
	}

	if (periodicInterval < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Invalid periodic interval (value=" << periodicInterval << ")");
	}

	const EventMonotonicTime queuedTime =
			ee_->clockGenerator_->getMonotonicTime();

	const int64_t time = queuedTime + timeoutMillis;

	util::LockGuard<util::Condition> guard(condition_);

	EventProgressWatcher *watcher = ev.getProgressWatcher();
	if (watcher != NULL) {
		if (!started_ || !runnable_) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
					"Watcher specified while not running");
		}

		if (watcher == progressWatcher_.get()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Wathing itself");
		}

		if (std::find(watcherList_->begin(), watcherList_->end(), watcher) !=
				watcherList_->end()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
					"Already wathing");
		}

		watcherList_->push_back(watcher);
	}

	if (!runnable_) {
		stats_.increment(Stats::EVENT_CANCELED_ADD_COUNT);
		return;
	}

	addDirect(time, ev, periodicInterval, &queuedTime);

	condition_.signal();
}

void EventEngine::EventWorker::addPending(
		const Event &ev, PendingResumeCondition resumeCondition) {
	if (ev.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Event is empty");
	}

	util::LockGuard<util::Condition> guard(condition_);
	if (!runnable_) {
		stats_.increment(Stats::EVENT_CANCELED_ADD_COUNT);
		return;
	}

	EventQueue *queue;
	switch (resumeCondition) {
	case RESUME_ALWAYS:
		queue = pendingQueue_.get();
		break;
	case RESUME_AT_PARTITION_CHANGE:
		queue = conditionalQueue_.get();
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
				"Unknown resume condition");
	}
	addEvent(*sharedVarAllocator_, *queue, ev);

	condition_.signal();

	stats_.increment(Stats::EVENT_PENDING_QUEUE_SIZE_CURRENT);
	stats_.updateMax(Stats::EVENT_PENDING_QUEUE_SIZE_MAX,
			stats_.get(Stats::EVENT_PENDING_QUEUE_SIZE_CURRENT));

	stats_.increment(Stats::EVENT_PENDING_ADD_COUNT);
}


bool EventEngine::EventWorker::removeWatcher(
		EventProgressWatcher &watcher) {
	util::LockGuard<util::Condition> guard(condition_);

	WatcherList::iterator it =
			std::find(watcherList_->begin(), watcherList_->end(), &watcher);
	if (it == watcherList_->end()) {
		return false;
	}

	watcherList_->erase(it);
	return true;
}

const EventEngine::Stats& EventEngine::EventWorker::getStats() const {
	return stats_;
}

void EventEngine::EventWorker::mergeExtraStats(Stats &stats) {
	Manipulator::mergeAllocatorStats(stats, *sharedVarAllocator_);
	Manipulator::mergeAllocatorStats(stats, *localVarAllocator_);
	Manipulator::mergeAllocatorStats(stats, *allocator_);
}

bool EventEngine::EventWorker::getLiveStats(
		Stats::Type type, EventMonotonicTime now, int64_t &value) {
	switch (type) {
	case Stats::EVENT_ACTIVE_EXECUTABLE_COUNT:
		value = static_cast<int64_t>(getExecutableActiveEventCount(
				now, util::LockGuard<util::Condition>(condition_)));
		break;
	default:
		return false;
	}
	return true;
}

EventContext::Source EventEngine::EventWorker::createContextSource(
		util::StackAllocator &allocator, const EventList *eventList) {
	EventContext::Source source(*localVarAllocator_, allocator, stats_);

	source.ee_ = ee_;
	source.beginPartitionId_ = ee_->pgConfig_->getGroupBeginPartitionId(id_);
	source.pendingPartitionCheckList_ = &pendingPartitionCheckList_;
	source.progressWatcher_ = progressWatcher_.get();
	source.workerId_ = id_;
	source.eventList_ = eventList;

	return source;
}



bool EventEngine::EventWorker::popActiveEvents(
		EventList &eventList, int64_t &nextTimeDiff) {
	const int64_t now = ee_->clockGenerator_->getMonotonicTime();

	bool found = false;
	size_t size = activeQueue_->size();
	eventList.reserve(size);
	for (; size > 0; size--) {
		ActiveEntry &entry = activeQueue_->front();

		nextTimeDiff = entry.time_ - now;
		if (nextTimeDiff > 0) {
			break;
		}

		eventList.push_back(ALLOC_VAR_SIZE_NEW(*localVarAllocator_)
				Event(*localVarAllocator_, *entry.ev_));
		found = true;

		updateBufferSize<false>(*entry.ev_);
		ALLOC_VAR_SIZE_DELETE(*sharedVarAllocator_, entry.ev_);

		const int32_t periodicInterval = entry.periodicInterval_;
		activeQueue_->pop_front();

		if (periodicInterval > 0) {
			addDirect(
					now + periodicInterval,
					*eventList.back(),
					periodicInterval,
					NULL);
		}
	}

	stats_.set(Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT, size);
	return found;
}

bool EventEngine::EventWorker::popPendingEvents(
		EventContext &ec, EventList &eventList,
		size_t &lastSize, bool checkFirst) {
	size_t curSize = 0;

	eventList.reserve(pendingQueue_->size() + conditionalQueue_->size());

	for (EventQueue::iterator it = pendingQueue_->begin();
			it != pendingQueue_->end();) {
		curSize++;
		if (!checkFirst) {
			eventList.push_back(ALLOC_VAR_SIZE_NEW(*localVarAllocator_)
					Event(*localVarAllocator_, **it));
			ALLOC_VAR_SIZE_DELETE(*sharedVarAllocator_, *it);
			it = pendingQueue_->erase(it);
			continue;
		}
		++it;
	}

	for (EventQueue::iterator it = conditionalQueue_->begin();
			it != conditionalQueue_->end();) {

		if (ec.isPendingPartitionChanged((*it)->getPartitionId())) {
			curSize++;
			if (!checkFirst) {
				eventList.push_back(ALLOC_VAR_SIZE_NEW(*localVarAllocator_)
						Event(*localVarAllocator_, **it));
				ALLOC_VAR_SIZE_DELETE(*sharedVarAllocator_, *it);
				it = conditionalQueue_->erase(it);
				continue;
			}
		}
		++it;
	}

	if (checkFirst) {
		if (lastSize != curSize) {
			return popPendingEvents(ec, eventList, lastSize, false);
		}

		ec.resetPendingPartitionStatus();
		return false;
	}

	lastSize = curSize;
	ec.resetPendingPartitionStatus();

	stats_.merge(Stats::EVENT_PENDING_QUEUE_SIZE_CURRENT,
			-static_cast<int64_t>(curSize));

	return (curSize > 0);
}

inline void EventEngine::EventWorker::addDirect(
		int64_t monotonicTime, const Event &ev, int32_t periodicInterval,
		const EventMonotonicTime *queuedTime) {
	const size_t LINEAR_SEARCH_LIMIT = 10;
	ActiveEntry entry(monotonicTime, periodicInterval);

	typedef ActiveQueue::iterator Iterator;
	Iterator it = activeQueue_->end();
	if (it != activeQueue_->begin()) {
		size_t remaining = LINEAR_SEARCH_LIMIT;
		do {
			if (monotonicTime >= (--it)->time_ ) {
				++it;
				break;
			}
			if (--remaining == 0) {
				it = std::equal_range(
						activeQueue_->begin(), activeQueue_->end(),
						entry, ActiveEntryLess()).second;
				break;
			}
		}
		while (it != activeQueue_->begin());
	}

	it = activeQueue_->insert(it, entry);
	try {
		it->ev_ = ALLOC_VAR_SIZE_NEW(*sharedVarAllocator_)
				Event(*sharedVarAllocator_, ev);
	}
	catch (...) {
		activeQueue_->erase(it);
		throw;
	}

	it->ev_->incrementQueueingCount();

	if (queuedTime != NULL) {
		it->ev_->setQueuedMonotonicTime(*queuedTime);
	}

	stats_.increment(Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT);
	stats_.updateMax(Stats::EVENT_ACTIVE_QUEUE_SIZE_MAX,
			stats_.get(Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));

	stats_.increment(Stats::EVENT_ACTIVE_ADD_COUNT);
	updateBufferSize<true>(*it->ev_);
}

template<bool Adding>
inline void EventEngine::EventWorker::updateBufferSize(const Event &ev) {
	const int64_t diff = static_cast<int64_t>(
			ev.getMessageBuffer().getCapacity()) * (Adding ? 1 : -1);
	const int64_t size =
			stats_.get(Stats::EVENT_ACTIVE_BUFFER_SIZE_CURRENT) + diff;

	stats_.set(Stats::EVENT_ACTIVE_BUFFER_SIZE_CURRENT, size);
	stats_.updateMax(Stats::EVENT_ACTIVE_BUFFER_SIZE_MAX, size);

	if (size > bufferSizeSoftLimit_) {
		if (size - diff > bufferSizeSoftLimit_) {
			ee_->limitter_->reportEventBufferDiff(diff);
		}
		else {
			ee_->limitter_->reportEventBufferDiff(size);
		}
	}
	else {
		if (size - diff > bufferSizeSoftLimit_) {
			ee_->limitter_->reportEventBufferDiff(-(size - diff));
		}
	}
}

template<typename EventContainer>
inline void EventEngine::EventWorker::addEvent(
		VariableSizeAllocator &allocator, EventContainer &eventContainer,
		const Event &ev) {
	Event *addingEv = ALLOC_VAR_SIZE_NEW(allocator) Event(allocator, ev);
	try {
		eventContainer.push_back(addingEv);
	}
	catch (...) {
		ALLOC_VAR_SIZE_DELETE(allocator, addingEv);
		throw;
	}
}

template<typename EventContainer>
void EventEngine::EventWorker::clearEvents(
		VariableSizeAllocator &allocator, EventContainer &eventContainer) {
	for (typename EventContainer::iterator it = eventContainer.begin();
			it != eventContainer.end(); ++it) {
		ALLOC_VAR_SIZE_DELETE(allocator, *it);
	}
	eventContainer.clear();
}

void EventEngine::EventWorker::clearEvents(
		VariableSizeAllocator &allocator, ActiveQueue &activeQueue) {
	for (ActiveQueue::iterator it = activeQueue.begin();
			it != activeQueue.end(); ++it) {
		ALLOC_VAR_SIZE_DELETE(allocator, it->ev_);
	}
	activeQueue.clear();
}

size_t EventEngine::EventWorker::getExecutableActiveEventCount(
		EventMonotonicTime now, const util::LockGuard<util::Condition>&) {
	size_t count = 0;
	for (ActiveQueue::const_iterator it = activeQueue_->begin();
			it != activeQueue_->end(); ++it) {
		const int64_t nextTimeDiff = it->time_ - now;
		if (nextTimeDiff > 0) {
			break;
		}
		count++;
	}
	return count;
}


EventEngine::EventWorker::ActiveEntry::ActiveEntry(
		int64_t time, int32_t periodicInterval) :
		time_(time),
		periodicInterval_(periodicInterval),
		ev_(NULL) {
}


bool EventEngine::EventWorker::ActiveEntryLess::operator()(
		const ActiveEntry &left, const ActiveEntry &right) const {
	return left.time_ < right.time_;
}


const size_t EventEngine::EventSocket::MULTICAST_MAX_PACKET_SIZE = 64 * 1024;
const size_t EventEngine::EventSocket::INITIAL_RECEIVE_BUFFER_SIZE = 2 * 1024;

EventEngine::EventSocket::EventSocket(
		EventEngine &ee, IOWorker &parentWorker) :
		ee_(ee),
		parentWorker_(parentWorker),
		eventSource_(parentWorker.getLocalEventContext()),
		eventCoder_(ee_.dispatcher_->getEventCoder()),
		multicast_(false),
		firstEventSent_(false),
		receiveBuffer_(NULL),
		sendBufferQueue_(NULL),
		nextReceiveSize_(0),
		pendingReceiveSize_(0),
		connectStartTime_(-1),
		lastConnectTime_(-1) {
}

EventEngine::EventSocket::~EventSocket() try {
	assert(base_.isClosed());
	assert(receiveEvent_.isEmpty());
	assert(receiveBuffer_ == NULL);
}
catch (...) {
}

void EventEngine::EventSocket::handlePollEvent(
		util::IOPollBase*, util::IOPollEvent ev) {
	assert(!base_.isClosed());

	try {
		if (ev & util::IOPollEvent::TYPE_READ) {
			for (;;) {
				EventRequestOption option;
				if (receiveLocal(receiveEvent_, option)) {
					dispatchEvent(receiveEvent_, option);

					if (pendingReceiveSize_ > 0) {
						continue;
					}
				}
				break;
			}
		}
		else if (ev & util::IOPollEvent::TYPE_WRITE) {
			sendLocal();
		}
	}
	catch (...) {
		try {
			throw;
		}
		catch (UserException &e) {
			if (e.getErrorCode() == GS_ERROR_EE_OPERATION_NOT_ALLOWED) {
				UTIL_TRACE_EXCEPTION_INFO(EVENT_ENGINE, e,
						"Failed to handle socket IO (engine=" << ee_.getName() <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				return;
			}
		}
		catch (...) {
		}

		std::exception e;
		UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
				"Failed to handle socket IO (engine=" << ee_.getName() <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

util::File& EventEngine::EventSocket::getFile() {
	return base_;
}

EventEngine::SocketPool& EventEngine::EventSocket::getParentPool() {
	return *ee_.socketPool_;
}

bool EventEngine::EventSocket::openAsClient(
		LockGuard &ndGuard, const util::SocketAddress &address) {
	assert(base_.isClosed());
	(void) ndGuard;

	try {
		return connect(&address, false);
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_OPEN_COUNT);
		GS_RETHROW_USER_OR_SYSTEM(e,
				"Failed to connect to remote (address=" << address <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

bool EventEngine::EventSocket::openAsServer(
		util::Socket &acceptedSocket,
		const util::SocketAddress &acceptedAddress) {
	assert(base_.isClosed());

	try {
		base_.attach(acceptedSocket.detach());
		address_ = acceptedAddress;

		base_.setBlockingMode(false);
		base_.setNoDelay(ee_.config_->tcpNoDelay_);

		if (ee_.config_->keepaliveEnabled_) {
			base_.setKeepAlive(true);
			base_.setKeepAlive(
					ee_.config_->keepaliveIdle_,
					ee_.config_->keepaliveInterval_,
					ee_.config_->keepaliveCount_);
		}

		return parentWorker_.addSocket(this, util::IOPollEvent::TYPE_READ);
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_OPEN_COUNT);
		GS_RETHROW_USER_OR_SYSTEM(e,
				"Failed to accept remote connection ("
				"address=" << acceptedAddress <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}

	return true;
}

bool EventEngine::EventSocket::openAsMulticast(
		const util::SocketAddress &address) {
	assert(base_.isClosed());

	try {
		base_.open(address.getFamily(), util::Socket::TYPE_DATAGRAM);

		base_.setBlockingMode(false);
		base_.setReuseAddress(true);

		base_.bind(
				util::SocketAddress(NULL, address.getPort(), address.getFamily()));
		base_.joinMulticastGroup(address);
		base_.setMulticastLoopback(true);

		address_ = address;

		multicast_ = true;

		return parentWorker_.addSocket(this, util::IOPollEvent::TYPE_READ);
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_OPEN_COUNT);
		GS_RETHROW_USER_OR_SYSTEM(e,
				"Failed to open multicast socket (address=" << address <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void EventEngine::EventSocket::send(
		LockGuard &ndGuard, Event &ev, const EventRequestOption *option) {
	(void) ndGuard;
	assert(!base_.isClosed());

	try {
		if (ev.getExtraMessageCount() > 0 && (multicast_ || option != NULL)) {
			ev = Event(ev);
		}

		int32_t index = 0;
		size_t nextPos = 0;

		const bool ready = (connectStartTime_ < 0);
		if (ready &&
				(sendBufferQueue_ == NULL || sendBufferQueue_->empty())) {
			for (;; index++) {
				const void *ptr;
				size_t size;
				const int32_t remaining = eventCoder_.encode(
						ee_, ev, ptr, size, index, option, firstEventSent_);

				const int64_t sentSize = multicast_ ?
						base_.sendTo(ptr, size, address_) :
						base_.send(ptr, size);
				if (sentSize < 0) {
					break;
				}
				else if (sentSize < static_cast<int64_t>(size)) {
					nextPos = static_cast<size_t>(sentSize);
					break;
				}

				if (remaining == 0) {
					index = -1;
					break;
				}
			}
		}

		if (index >= 0) {
			int32_t remaining;
			do {
				const void *ptr;
				size_t size;
				remaining = eventCoder_.encode(
						ee_, ev, ptr, size, index, option, firstEventSent_);

				appendToSendBuffer(ptr, size, nextPos);

				index++;
				nextPos = 0;
			}
			while (remaining > 0);

			if (ready) {
				parentWorker_.modifySocket(
						this, util::IOPollEvent::TYPE_READ_WRITE);
			}
		}

		if (!firstEventSent_ && !multicast_) {
			firstEventSent_ = true;
		}
	}
	catch (std::exception &e) {
		if (handleIOError(e)) {
			return;
		}

		parentWorker_.incrementErrorCount(Stats::IO_ERROR_SEND_COUNT);

		if (!parentWorker_.isShutdown()) {
			GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(e,
					"Failed to send"));
		}
	}
}

void EventEngine::EventSocket::shutdown(LockGuard &ndGuard) {
	(void) ndGuard;

	if (!base_.isClosed()) {
		requestShutdown();
	}
}

bool EventEngine::EventSocket::reconnectLocal(bool polling) {
	assert(!base_.isClosed());
	assert(!nd_.isEmpty());

	try {
		LockGuard guard(Manipulator::getNDBody(nd_).getLock());
		return connect(NULL, polling);
	}
	catch (std::exception &e) {
		handleIOError(e);
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_CONNECT_COUNT);
		UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to connect"));
		return false;
	}
}

void EventEngine::EventSocket::resumeLocal() {
	assert(!base_.isClosed());

	try {
		suspendLocal(false, util::IOPollEvent::TYPE_READ);

		if (!pendingEvent_.isEmpty()) {
			Event ev = pendingEvent_;
			EventRequestOption option = pendingOption_;

			pendingEvent_ = Event();
			pendingOption_ = EventRequestOption();

			dispatchEvent(ev, option);
		}
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_OTHER_COUNT);
		UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to resume"));
	}

	if (sendBufferQueue_ != NULL && !sendBufferQueue_->empty()) {
		parentWorker_.modifySocket(this, util::IOPollEvent::TYPE_READ_WRITE);
	}
}

void EventEngine::EventSocket::handleDisconnectionLocal() {
	assert(!base_.isClosed());

	if (!nd_.isEmpty()) {
		try {
			Event ev(eventSource_, 0, 0);
			ev.resetAttributes(EventType(), UNDEF_PARTITIONID, nd_);
			ee_.dispatcher_->handleDisconnection(
					parentWorker_.getLocalEventContext(), ev);
		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e,
					"Failed to handle disconnection (engine=" <<
					ee_.getName() << ")");
		}
	}
}

void EventEngine::EventSocket::closeLocal(
		bool workerAlive, LockGuard *ndGuard) {

	if (!nd_.isEmpty()) {
		{
			NodeDescriptor::Body &ndBody = Manipulator::getNDBody(nd_);
			if (ndGuard == NULL) {
				LockGuard guard(ndBody.getLock());
				ndBody.removeSocket(guard, this);
			}
			else {
				ndBody.removeSocket(*ndGuard, this);
			}
		}
		nd_ = NodeDescriptor();
	}

	if (workerAlive) {
		if (receiveBuffer_ != NULL) {
			parentWorker_.updateReceiveBufferSize(*receiveBuffer_, false);
		}

		if (sendBufferQueue_ != NULL) {
			for (BufferQueue::iterator it = sendBufferQueue_->begin();
					it != sendBufferQueue_->end(); ++it) {
				parentWorker_.updateSendBufferSize(*it, false);
			}

			parentWorker_.releaseBufferQueue(sendBufferQueue_);
		}
	}

	receiveEvent_ = Event();
	receiveBuffer_ = NULL;

	base_.close();
}

void EventEngine::EventSocket::initializeND(
		LockGuard &ndGuard, const NodeDescriptor &nd) {
	(void) ndGuard;

	if (!nd_.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_OPERATION_NOT_ALLOWED,
				"ND has already been set");
	}

	nd_ = nd;
}

void EventEngine::EventSocket::dispatchEvent(
		Event &ev, const EventRequestOption &option) {

	EventContext &ec = parentWorker_.getLocalEventContext();
	const bool suspended = parentWorker_.isSuspendedLocal();

	switch (ee_.dispatcher_->dispatch(ec, ev, suspended, option)) {
	case Dispatcher::DISPATCH_RESULT_DONE:
		break;
	case Dispatcher::DISPATCH_RESULT_CANCELED:
		if (!multicast_) {
			pendingEvent_ = ev;
			pendingOption_ = option;

			suspendLocal(true, util::IOPollEvent::TYPE_READ);
		}
		break;
	case Dispatcher::DISPATCH_RESULT_CLOSED:
		requestShutdown();
		break;
	default:
		assert(false);
	}
}

bool EventEngine::EventSocket::connect(
		const util::SocketAddress *address, bool polling) {
	assert(!nd_.isEmpty());

	const int64_t current = ee_.clockGenerator_->getMonotonicTime();
	int64_t elapsed;
	if (connectStartTime_ < 0) {
		if (address == NULL) {
			return false;
		}
		connectStartTime_ = current;
		elapsed = 0;
	}
	else {
		elapsed = current - connectStartTime_;

		if (elapsed > ee_.config_->connectTimeoutMillis_) {
			parentWorker_.incrementErrorCount(Stats::IO_ERROR_CONNECT_COUNT);
			GS_THROW_USER_ERROR(GS_ERROR_EE_CONNECT_TIMEOUT,
					"Connection timed out (address=" << address_ <<
					", elapsedMillis=" << elapsed <<
					", timeout=" << ee_.config_->connectTimeoutMillis_ << ")");
		}

		if (current - lastConnectTime_ < ee_.config_->reconnectIntervalMillis_) {
			return false;
		}
		else if (polling) {
			return parentWorker_.reconnectSocket(this, util::IOPollEvent());
		}
	}
	lastConnectTime_ = current;
	assert(!polling);

	if (address != NULL) {
		if (address->isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID,
					"Empty address");
		}
		address_ = *address;
	}
	assert(!address_.isEmpty());

	base_.close();
	base_.open(address_.getFamily(), util::Socket::TYPE_STREAM);

	try {
		base_.setBlockingMode(false);
		base_.setNoDelay(ee_.config_->tcpNoDelay_);

		if (!base_.connect(address_)) {
			return parentWorker_.reconnectSocket(
					this, util::IOPollEvent::TYPE_READ_WRITE);
		}

		connectStartTime_ = -1;
		lastConnectTime_ = -1;

		return parentWorker_.addSocket(this,
				(sendBufferQueue_ == NULL || sendBufferQueue_->empty()) ?
				util::IOPollEvent::TYPE_READ :
				util::IOPollEvent::TYPE_READ_WRITE);
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_CONNECT_COUNT);
		base_.close();

		UTIL_TRACE_EXCEPTION_INFO(EVENT_ENGINE, e,
				"Retrying connection (address=" << address_ <<
				", elapsedMillis=" << elapsed <<
				", timeout=" << ee_.config_->connectTimeoutMillis_ <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

		return parentWorker_.reconnectSocket(this, util::IOPollEvent());
	}
}

bool EventEngine::EventSocket::receiveLocal(
		Event &ev, EventRequestOption &option) {
	bool unexpectedShutdown = false;
	try {
		if (!pendingEvent_.isEmpty()) {
			suspendLocal(true, util::IOPollEvent());
			return false;
		}

		if (receiveBuffer_ == NULL) {
			ev = Event(eventSource_, 0, 0);
			receiveBuffer_ = &ev.getMessageBuffer();
			receiveBuffer_->getWritableXArray().reserve(
					INITIAL_RECEIVE_BUFFER_SIZE);
			receiveBuffer_->clear();
		}

		struct SizeUpdator {
			SizeUpdator(EventSocket &socket) : socket_(socket) {
				socket_.parentWorker_.updateReceiveBufferSize(
						*socket_.receiveBuffer_, false);
			}

			~SizeUpdator() try {
				socket_.parentWorker_.updateReceiveBufferSize(
						*socket_.receiveBuffer_, true);
			}
			catch (...) {
			}

			EventSocket &socket_;
		} updator(*this);

		if (multicast_) {
			nextReceiveSize_ = MULTICAST_MAX_PACKET_SIZE;
			receiveBuffer_->clear();
			nd_ = NodeDescriptor();
		}
		else {
			receiveBuffer_->setOffset(0);
		}

		Buffer::XArray &storage = receiveBuffer_->getWritableXArray();
		size_t bufferSize;
		if (pendingReceiveSize_ == 0) {
			const size_t orgSize = (nextReceiveSize_ == 0 ? 0 : storage.size());
			storage.reserve(orgSize + nextReceiveSize_);
			nextReceiveSize_ = storage.capacity() - orgSize;

			const int64_t receivedSize =
					base_.receive(storage.data() + orgSize, nextReceiveSize_);
			if (receivedSize < 0) {
				storage.resize(orgSize);
				return false;
			}
			else if (receivedSize > 0) {
				bufferSize = orgSize + static_cast<size_t>(receivedSize);
			}
			else {
				unexpectedShutdown = true;
				GS_THROW_USER_ERROR(GS_ERROR_EE_IO_READ_FAILED,
						"Unexpected shutdown occurred");
			}
		}
		else {
			assert(pendingReceiveSize_ <= storage.capacity() - storage.size());
			uint8_t *data = storage.data();
			memmove(data, data + storage.size(), pendingReceiveSize_);
			bufferSize = pendingReceiveSize_;
		}
		storage.resize(bufferSize);

		nextReceiveSize_ = eventCoder_.decode(
				ee_, *receiveBuffer_, ev, nd_, address_, option);
		pendingReceiveSize_ = bufferSize - storage.size();

		if (nd_.isEmpty() && !ev.getSenderND().isEmpty() && !multicast_) {
			NodeDescriptor::Body &ndBody =
					Manipulator::getNDBody(ev.getSenderND());

			LockGuard guard(ndBody.getLock());
			ndBody.setSocket(
					guard, this, NodeDescriptor::Body::ND_SOCKET_RECEIVER);
		}

		if (nextReceiveSize_ == 0) {
			return true;
		}
	}
	catch (std::exception &e) {
		do {
			if (nd_.isEmpty() || shutdownRequested_) {
				break;
			}

			{
				LockGuard guard(Manipulator::getNDBody(nd_).getLock());
				if (connectStartTime_ < 0) {
					break;
				}

				try {
					parentWorker_.reconnectSocket(this, util::IOPollEvent());
				}
				catch (...) {
				}
			}

			const int64_t current = ee_.clockGenerator_->getMonotonicTime();
			UTIL_TRACE_EXCEPTION_INFO(EVENT_ENGINE, e,
					"Failed to connect (address=" << address_ <<
					", elapsedMillis=" << (current - connectStartTime_) <<
					", elapsedFromLastConnect=" <<
							(current - lastConnectTime_) <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

			parentWorker_.incrementErrorCount(Stats::IO_ERROR_CONNECT_COUNT);
			return false;
		}
		while (false);

		if (handleIOError(e)) {
			return false;
		}
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_RECEIVE_COUNT);

		if (!parentWorker_.isShutdown()) {
			if (!multicast_ && unexpectedShutdown) {
				ee_.dispatcher_->handleUnexpectedShutdownError(e, nd_);
			}
			else {
				GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Failed to receive"));
			}
		}
	}
	return false;
}

void EventEngine::EventSocket::sendLocal() {
	try {
		if (nd_.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_IO_WRITE_FAILED,
					"Trying to send while ND is empty");
		}

		LockGuard guard(Manipulator::getNDBody(nd_).getLock());

		if (connectStartTime_ >= 0) {
			connectStartTime_ = -1;
			lastConnectTime_ = -1;
			parentWorker_.modifySocket(
					this, util::IOPollEvent::TYPE_READ_WRITE);
		}

		for (;;) {
			if (sendBufferQueue_ == NULL || sendBufferQueue_->empty()) {
				parentWorker_.modifySocket(this, util::IOPollEvent::TYPE_READ);
				break;
			}

			Buffer &buffer = sendBufferQueue_->front();

			bool pending = false;
			for (size_t i = 0; i < 2; i++) {
				size_t offset;
				size_t size;
				int64_t sentSize;
				if (i == 0) {
					offset = buffer.getOffset();
					size = buffer.getSize();
					if (size == 0) {
						continue;
					}

					const void *data = buffer.getXArray().data() + offset;
					sentSize = multicast_ ?
							base_.sendTo(data, size, address_) :
							base_.send(data, size);
				}
				else {
					const ExternalBuffer *ext = buffer.getExternal();
					if (ext == NULL) {
						continue;
					}

					offset = buffer.getExternalOffset();
					size = ext->getSize();
					if (offset >= size) {
						continue;
					}
					size -= offset;

					ExternalBuffer::Reader reader(*ext);

					const void *data =
							static_cast<const uint8_t*>(reader.data()) + offset;
					sentSize = multicast_ ?
							base_.sendTo(data, size, address_) :
							base_.send(data, size);
				}

				if (sentSize < 0 || multicast_) {
					pending = true;
					break;
				}

				size_t nextOffset = offset;
				if (sentSize < static_cast<int64_t>(size)) {
					pending = true;
					nextOffset += static_cast<size_t>(sentSize);
				}
				else {
					nextOffset += size;
				}

				if (i == 0) {
					buffer.setOffset(nextOffset);

					if (pending) {
						break;
					}
				}
				else {
					if (pending) {
						buffer.setExternalOffset(nextOffset);
						break;
					}
					else {
						buffer.prepareExternal(NULL).clear();
					}
				}
			}

			if (pending) {
				break;
			}

			parentWorker_.updateSendBufferSize(buffer, false);
			parentWorker_.popBufferQueue(*sendBufferQueue_);
		}
	}
	catch (std::exception &e) {
		if (handleIOError(e)) {
			return;
		}
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_SEND_COUNT);

		if (!parentWorker_.isShutdown()) {
			GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(e,
					"Failed to send"));
		}
	}
}

void EventEngine::EventSocket::suspendLocal(
		bool enabled, util::IOPollEvent extraPollEvent) {

	util::IOPollEvent pollEvent = extraPollEvent;
	{
		NodeDescriptor::Body &ndBody = Manipulator::getNDBody(nd_);
		LockGuard guard(ndBody.getLock());

		if (sendBufferQueue_ != NULL && !sendBufferQueue_->empty()) {
			pollEvent = (pollEvent | util::IOPollEvent::TYPE_WRITE);
		}
	}

	if (enabled) {
		parentWorker_.suspendSocket(this, pollEvent);
	}
	else {
		parentWorker_.modifySocket(this, pollEvent);
	}
}

void EventEngine::EventSocket::requestShutdown() {
	try {
		bool orgState = false;
		if (shutdownRequested_.compareExchange(orgState, true)) {
			parentWorker_.removeSocket(this);
		}
	}
	catch (std::exception &e) {
		parentWorker_.incrementErrorCount(Stats::IO_ERROR_OTHER_COUNT);
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Failed to shutdown"));
	}
}

bool EventEngine::EventSocket::handleIOError(std::exception&) throw() {
	try {
		throw;
	}
	catch (util::PlatformException&) {
		if (shutdownRequested_) {
			return true;
		}
	}
	catch (...) {
	}

	try {
		if (!multicast_) {
			requestShutdown();
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(EVENT_ENGINE, e, "");
	}

	return false;
}

void EventEngine::EventSocket::appendToSendBuffer(
		const void *data, size_t size, size_t offset) {
	const bool extEnabled = false;

	typedef VariableSizeAllocator::TraitsType Traits;
	const size_t threshold =
			Traits::getFixedSize(Traits::FIXED_ALLOCATOR_COUNT - 1) / 2;

	size_t nextOffset = offset;

	bool forExternal = false;
	while (nextOffset < size) {
		Buffer *buffer = NULL;
		ExternalBuffer *ext = NULL;
		do {
			if (forExternal) {
				break;
			}

			if (sendBufferQueue_ == NULL || sendBufferQueue_->empty()) {
				break;
			}

			buffer = &sendBufferQueue_->back();

			if (buffer->getExternal() == NULL &&
					(buffer->getXArray().size() < threshold ||
					buffer->getCapacity() > buffer->getXArray().size())) {
				if (!(extEnabled && sendBufferQueue_->size() > 1)) {
					break;
				}
			}

			if (!extEnabled) {
				buffer = NULL;
				break;
			}

			ext = &buffer->prepareExternal(ee_.bufferManager_);
			if (ext->getSize() >= ext->getCapacity()) {
				ext = NULL;
			}
			forExternal = true;
		}
		while (false);

		if (buffer == NULL) {
			buffer = &parentWorker_.pushBufferQueue(
					sendBufferQueue_, (forExternal ? 0 : threshold));

			if (buffer->getCapacity() > 0) {
				parentWorker_.updateSendBufferSize(*buffer, true);
			}
		}

		if (forExternal) {
			if (ext == NULL) {
				ext = &buffer->prepareExternal(ee_.bufferManager_);
			}

			ExternalBuffer::Writer writer(*ext);

			const size_t wroteSize = writer.tryAppend(
					static_cast<const uint8_t*>(data) + nextOffset,
					size - nextOffset);
			nextOffset += wroteSize;
		}
		else {
			const size_t writeSize = std::min(
					buffer->getCapacity() - buffer->getXArray().size(),
					size - nextOffset);
			buffer->getOutStream().writeAll(
					static_cast<const uint8_t*>(data) + nextOffset,
					writeSize);
			nextOffset += writeSize;
		}
	}
}


EventEngine::SocketReference::SocketReference(
		EventSocket *socket, LockGuard *ndGuard) :
		socket_(socket),
		ndGuard_(ndGuard) {
}

EventEngine::SocketReference::~SocketReference() try {
	reset();
}
catch (...) {
}

EventEngine::SocketReference::SocketReference(SocketReference &another) :
		socket_(another.socket_),
		ndGuard_(another.ndGuard_) {
	another.socket_ = NULL;
	another.ndGuard_ = NULL;
}

EventEngine::SocketReference& EventEngine::SocketReference::operator=(
		SocketReference &another) {
	if (this == &another) {
		return *this;
	}

	reset();

	socket_ = another.socket_;
	ndGuard_ = another.ndGuard_;

	another.socket_ = NULL;
	another.ndGuard_ = NULL;

	return *this;
}

EventEngine::EventSocket* EventEngine::SocketReference::get() {
	return socket_;
}

EventEngine::EventSocket* EventEngine::SocketReference::release() {
	EventSocket *socket = socket_;
	socket_ = NULL;

	return socket;
}

void EventEngine::SocketReference::reset() {
	if (socket_ == NULL) {
		return;
	}

	socket_->getParentPool().deallocate(socket_, false, ndGuard_);
	socket_ = NULL;
	ndGuard_ = NULL;
}


EventEngine::EventProgressWatcher::EventProgressWatcher() :
		handlerStartTime_(-1), completed_(false), lastException_(NULL) {
}

EventEngine::EventProgressWatcher::~EventProgressWatcher() try {
	clear();
}
catch (...) {
}

void EventEngine::EventProgressWatcher::clear() {
	handlerStartTime_ = -1;
	completed_ = false;

	if (lastException_ != NULL) {
		lastException_->~Exception();
	}
	lastException_ = NULL;
}

EventEngine::EventProgressWatcher*
EventEngine::EventProgressWatcher::release(Event &ev) {
	EventProgressWatcher *watcher = ev.getProgressWatcher();

	if (watcher != NULL) {
		ev.setProgressWatcher(NULL);
	}

	return watcher;
}

void EventEngine::EventProgressWatcher::setHandlerStartTime(
		EventProgressWatcher *watcher, EventMonotonicTime time) {
	if (watcher == NULL) {
		return;
	}

	util::LockGuard<util::Condition> guard(watcher->condition_);
	watcher->handlerStartTime_ = time;
}

void EventEngine::EventProgressWatcher::setCompleted(
		EventProgressWatcher *watcher) {
	if (watcher == NULL) {
		return;
	}

	util::LockGuard<util::Condition> guard(watcher->condition_);
	watcher->completed_ = true;
	watcher->condition_.signal();
}

void EventEngine::EventProgressWatcher::setLastException(
		EventProgressWatcher *watcher, std::exception &e) {
	if (watcher == NULL) {
		return;
	}

	try {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	catch (...) {
		util::LockGuard<util::Condition> guard(watcher->condition_);

		try {
			throw;
		}
		catch (util::Exception &wrappedException) {
			if (watcher->lastException_ == NULL) {
				watcher->lastException_ =
						new (watcher->lastExceptionStorage_) util::Exception();
			}
			*watcher->lastException_ = wrappedException;
		}

		watcher->completed_ = true;
		watcher->condition_.signal();
	}
}


const EventEngine::DefaultEventCoder::CheckCode
		EventEngine::DefaultEventCoder::CODER_CHECK_CODE_VALUE = 65021048;

const EventEngine::DefaultEventCoder::HeaderType
		EventEngine::DefaultEventCoder::CODER_NODE_TYPE_SERVER = 0;

const EventEngine::DefaultEventCoder::HeaderType
		EventEngine::DefaultEventCoder::CODER_NODE_TYPE_CLIENT = -1;

const EventEngine::DefaultEventCoder::HeaderType
		EventEngine::DefaultEventCoder::CODER_NODE_TYPE_MASK = 0xff;

const EventEngine::DefaultEventCoder::HeaderType
		EventEngine::DefaultEventCoder::CODER_OPTION_CODE_MASK = 0xff00;

const EventEngine::DefaultEventCoder::HeaderType
		EventEngine::DefaultEventCoder::CODER_OPTION_CODE_VALUE = 0x0200;

const size_t EventEngine::DefaultEventCoder::CODER_OPTION_SIZE_BASE_BITS = 16;

const EventEngine::DefaultEventCoder::OptionType
		EventEngine::DefaultEventCoder::CODER_OPTION_TYPE_TIMEOUT = 1;

const EventEngine::DefaultEventCoder::BodySize
		EventEngine::DefaultEventCoder::CODER_MAX_BODY_SIZE =
		static_cast<BodySize>(std::numeric_limits<int32_t>::max());

const size_t EventEngine::DefaultEventCoder::CODER_BODY_COMMON_PART_SIZE =
		sizeof(EventType) + sizeof(PartitionId);

const size_t EventEngine::DefaultEventCoder::CODER_FIXED_COMMON_PART_SIZE =
		sizeof(CheckCode) + sizeof(PortNumber) +
		sizeof(HeaderType) + sizeof(BodySize) +
		CODER_BODY_COMMON_PART_SIZE;

EventEngine::DefaultEventCoder::~DefaultEventCoder() {
}

size_t EventEngine::DefaultEventCoder::decode(
		EventEngine &ee, Buffer &buffer, Event &ev,
		const NodeDescriptor &nd,
		const util::SocketAddress &socketAddress,
		EventRequestOption &option) {

	assert(buffer.getOffset() == 0);

	EventByteInStream in = buffer.getInStream();
	const size_t bufferSize = in.base().remaining();

	const size_t commonPartSize = getCommonPartSize(socketAddress, true);
	if (bufferSize < commonPartSize) {
		return commonPartSize - bufferSize;
	}

	NodeDescriptor senderND;
	HeaderType headerType;
	if (nd.isEmpty()) {
		CheckCode checkCode;
		in >> checkCode;
		if (checkCode != CODER_CHECK_CODE_VALUE) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Invalid check code");
		}

		util::SocketAddress ndAddress;
		headerType = decodeND(ee, in, &senderND, ndAddress, socketAddress);
	}
	else {
		in.base().position(
				commonPartSize - sizeof(HeaderType) - sizeof(BodySize) -
				CODER_BODY_COMMON_PART_SIZE);

		in >> headerType;
		senderND = nd;
	}

	BodySize bodySize;
	in >> bodySize;

	const size_t requiredSize =
			commonPartSize - CODER_BODY_COMMON_PART_SIZE + bodySize;
	if (bufferSize < requiredSize) {
		return requiredSize - bufferSize;
	}

	const size_t remaining = bufferSize - requiredSize;
	if (remaining > 0) {
		buffer.getWritableXArray().resize(requiredSize);

		const size_t orgPos = in.base().position();
		in = buffer.getInStream();
		in.base().position(orgPos);
	}

	buffer.setOffset(commonPartSize);

	EventType eventType;
	in >> eventType;

	PartitionId partitionId;
	in >> partitionId;

	ev.resetAttributes(eventType, partitionId, senderND);

	if (headerType != CODER_NODE_TYPE_SERVER &&
			headerType != CODER_NODE_TYPE_CLIENT) {
		size_t optionSize;
		decodeRequestOption(in, headerType, option, optionSize);
		ev.setTailMetaMessageSize(static_cast<uint16_t>(optionSize));
	}
	else {
		ev.setTailMetaMessageSize(0);
	}

	return 0;
}

int32_t EventEngine::DefaultEventCoder::encode(
		EventEngine &ee, Event &ev,
		const void *&ptr, size_t &size, int32_t index,
		const EventRequestOption *option,
		bool followingEventEncoding) {
	if (!ev.isMessageSpecified()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Message is empty");
	}

	assert(option == NULL || ev.getExtraMessageCount() == 0);

	const int32_t endIndex =
			static_cast<int32_t>(ev.getExtraMessageCount()) + 1;
	if (index != 0) {
		assert(0 < index && index < endIndex);
		ev.getExtraMessage(static_cast<size_t>(index) - 1, ptr, size);
		return endIndex - (index + 1);
	}

	Buffer &buffer = ev.getMessageBuffer();
	size_t totalOffset;
	if (ev.isPartitionIdSpecified()) {
		if (followingEventEncoding) {
			totalOffset =
					encodeCommonPart<true, true>(ev, buffer, ee.selfAddress_);
		}
		else {
			totalOffset =
					encodeCommonPart<true, false>(ev, buffer, ee.selfAddress_);
		}
	}
	else {
		if (followingEventEncoding) {
			totalOffset =
					encodeCommonPart<false, true>(ev, buffer, ee.selfAddress_);
		}
		else {
			totalOffset =
					encodeCommonPart<false, false>(ev, buffer, ee.selfAddress_);
		}
	}

	if (option != NULL || ev.getTailMetaMessageSize() > 0) {
		encodeRequestOption(ev, option);
	}

	const Buffer::XArray &bufferStorage = buffer.getXArray();
	ptr = bufferStorage.data() + totalOffset;
	size = bufferStorage.size() - totalOffset;

	return endIndex - 1;
}

void EventEngine::DefaultEventCoder::initializeMessageBuffer(Buffer &buffer) {
	assert(buffer.isEmpty() && buffer.getOffset() == 0);

	const size_t maxCommonPartSize = getMaxCommonPartSize();

	Buffer::XArray &storage = buffer.getWritableXArray();
	storage.resize(maxCommonPartSize);

	buffer.setOffset(maxCommonPartSize);
}

size_t EventEngine::DefaultEventCoder::getMaxCommonPartSize() {
	return (CODER_FIXED_COMMON_PART_SIZE + std::max(
			sizeof(util::SocketAddress::Inet),
			sizeof(util::SocketAddress::Inet6)));
}

size_t EventEngine::DefaultEventCoder::getCommonPartSize(
		const util::SocketAddress &socketAddress, bool pIdSpecified) {
	return (CODER_FIXED_COMMON_PART_SIZE -
			(pIdSpecified ? 0 : sizeof(PartitionId)) +
			getAddressSize(socketAddress));
}

size_t EventEngine::DefaultEventCoder::getAddressSize(
		const util::SocketAddress &socketAddress) {
	if (socketAddress.getFamily() == util::SocketAddress::FAMILY_INET) {
		return sizeof(util::SocketAddress::Inet);
	}
	else if (socketAddress.getFamily() == util::SocketAddress::FAMILY_INET6) {
		return sizeof(util::SocketAddress::Inet6);
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Empty or unknown address family");
	}
}

EventEngine::DefaultEventCoder::HeaderType
EventEngine::DefaultEventCoder::decodeND(
		EventEngine &ee, EventByteInStream &in, NodeDescriptor *nd,
		util::SocketAddress &ndAddress,
		const util::SocketAddress &baseAddress) {
	HeaderType headerType;

	if (nd != NULL && !nd->isEmpty()) {
		in.base().position(in.base().position() +
				getAddressSize(baseAddress) + sizeof(PortNumber));

		in >> headerType;
		return headerType;
	}

	const int family = baseAddress.getFamily();
	PortNumber port;
	if (family == util::SocketAddress::FAMILY_INET) {
		util::SocketAddress::Inet addressStorage;
		in.readAll(&addressStorage, sizeof(addressStorage));
		in >> port;
		ndAddress.assign(addressStorage, static_cast<uint16_t>(port));
	}
	else if (family == util::SocketAddress::FAMILY_INET6) {
		util::SocketAddress::Inet6 addressStorage;
		in.readAll(&addressStorage, sizeof(addressStorage));
		in >> port;
		ndAddress.assign(addressStorage, static_cast<uint16_t>(port));
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Empty or unknown address family");
	}

	in >> headerType;
	if (nd != NULL) {
		switch (headerType & CODER_NODE_TYPE_MASK) {
		case (CODER_NODE_TYPE_SERVER & CODER_NODE_TYPE_MASK):
			*nd = ee.resolveServerND(ndAddress);
			if (nd->getAddress().getFamily() != family) {
				GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
						"Address family unmatched (expected=" << family <<
						", actual=" << nd->getAddress().getFamily() << ")");
			}
			break;
		case (CODER_NODE_TYPE_CLIENT & CODER_NODE_TYPE_MASK):
			*nd = ee.ndPool_->allocateClientND();
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Unknown node type");
		}
	}

	return headerType;
}

template<bool PIdSpecified, bool FollowingEncoding>
size_t EventEngine::DefaultEventCoder::encodeCommonPart(
		Event &ev, Buffer &buffer, const util::SocketAddress &selfAddress) {

	const size_t addressSize = getAddressSize(selfAddress);
	const size_t commonPartSize = CODER_FIXED_COMMON_PART_SIZE -
			(PIdSpecified ? 0 : sizeof(PartitionId)) + addressSize;

	const size_t bufferOffset = buffer.getOffset();
	if (bufferOffset < commonPartSize) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Insufficient message size or invalid offset");
	}

	const size_t totalOffset = bufferOffset - commonPartSize;

	Buffer::XArray &storage = buffer.getWritableXArray();
	uint8_t *out = storage.data();
	out += totalOffset;

	putValueUnchecked(out, CODER_CHECK_CODE_VALUE);

	if (FollowingEncoding) {
		const size_t nodeInfoSize =
				addressSize + sizeof(PortNumber) + sizeof(HeaderType);
		memset(out, 0, nodeInfoSize);
		out += nodeInfoSize;
	}
	else {
		assert(!selfAddress.isEmpty());
		uint16_t port;
		if (selfAddress.getFamily() == util::SocketAddress::FAMILY_INET) {
			util::SocketAddress::Inet addressStorage;
			selfAddress.getIP(&addressStorage, &port);
			putValueUnchecked(out, addressStorage);
		}
		else if (selfAddress.getFamily() == util::SocketAddress::FAMILY_INET6) {
			util::SocketAddress::Inet6 addressStorage;
			selfAddress.getIP(&addressStorage, &port);
			putValueUnchecked(out, addressStorage);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Empty or unknown address family");
		}
		putValueUnchecked(out, static_cast<PortNumber>(port));
		putValueUnchecked(out, CODER_NODE_TYPE_SERVER);
	}

	size_t bodySize = CODER_BODY_COMMON_PART_SIZE -
			(PIdSpecified ? 0 : sizeof(PartitionId)) +
			(storage.size() - bufferOffset);
	size_t extraCount = ev.getExtraMessageCount();
	for (size_t i = 0; i < extraCount; i++) {
		const void *localPtr;
		size_t localSize;
		ev.getExtraMessage(i, localPtr, localSize);
		bodySize += localSize;
	}

	if (bodySize > CODER_MAX_BODY_SIZE) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Too large message body size (size=" << bodySize << ")");
	}
	putValueUnchecked(out, static_cast<BodySize>(bodySize));

	putValueUnchecked(out, ev.getType());

	if (PIdSpecified) {
		putValueUnchecked(out, ev.getPartitionId());
	}

	assert(out == storage.data() + bufferOffset);
	return totalOffset;
}

void EventEngine::DefaultEventCoder::decodeRequestOption(
		EventByteInStream &in, HeaderType headerType,
		EventRequestOption &option, size_t &optionSize) {
	optionSize = 0;

	if ((headerType & CODER_OPTION_CODE_MASK) !=
			CODER_OPTION_CODE_VALUE) {
		switch (headerType) {
		case CODER_NODE_TYPE_SERVER:
			break;
		case CODER_NODE_TYPE_CLIENT:
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Unknown header type found (value=" << headerType << ")");
			break;
		}
		return;
	}

	optionSize = static_cast<size_t>(headerType) >>
			CODER_OPTION_SIZE_BASE_BITS;

	const size_t remaining = in.base().remaining();
	if (remaining < optionSize) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
				"Too small message remaining size (required=" << optionSize <<
				", remaining=" << remaining << ")");
	}

	const size_t orgPos = in.base().position();
	in.base().position(orgPos + remaining - optionSize);

	while (in.base().remaining() > 0) {
		OptionType type;
		in >> type;
		switch (type) {
		case CODER_OPTION_TYPE_TIMEOUT:
			in >> option.timeoutMillis_;
			break;
		default:
			UTIL_TRACE_INFO(EVENT_ENGINE,
					"Unknown option type found (value=" << type << ")");
			in.base().position(in.base().position() + in.base().remaining());
			break;
		}
	}

	in.base().position(orgPos);
}

void EventEngine::DefaultEventCoder::encodeRequestOption(
		Event &ev, const EventRequestOption *option) {
	const uint16_t orgOptionSize = ev.getTailMetaMessageSize();
	if (option == NULL && orgOptionSize == 0) {
		return;
	}
	assert(ev.getExtraMessageCount() == 0);

	Buffer &buffer = ev.getMessageBuffer();

	const size_t bufferOffset = buffer.getOffset();

	const size_t backSize = sizeof(HeaderType) + sizeof(BodySize) +
			CODER_BODY_COMMON_PART_SIZE;
	assert(backSize < bufferOffset);
	const size_t startOffset = bufferOffset - backSize;

	HeaderType headerType;
	BodySize bodySize;
	{
		const Buffer::XArray &storage = buffer.getXArray();
		EventByteInStream in =
				util::ArrayByteInStream(util::ArrayInStream(
						storage.data() + startOffset,
						storage.size() - startOffset));

		in >> headerType;
		in >> bodySize;

		if (orgOptionSize > 0) {
			EventRequestOption lastOption;
			size_t optionSize = 0;
			decodeRequestOption(in, headerType, lastOption, optionSize);
			if (option != NULL && lastOption == *option) {
				return;
			}

			assert(bodySize >= orgOptionSize);
			bodySize -= orgOptionSize;
			ev.clearTailMetaMessage();
		}
	}

	if (option == NULL && orgOptionSize == 0) {
		return;
	}

	EventByteOutStream out = buffer.getOutStream();
	const size_t tailOffset = out.base().position();
	try {
		if (option != NULL) {
			if (option->timeoutMillis_ != 0) {
				out << CODER_OPTION_TYPE_TIMEOUT;
				out << option->timeoutMillis_;
			}
		}

		const size_t optionSize = out.base().position() - tailOffset;
		if (optionSize > std::numeric_limits<uint16_t>::max()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_MESSAGE_INVALID,
					"Too large event request option size (size=" <<
					optionSize << ")");
		}
		bodySize += static_cast<uint16_t>(optionSize);

		headerType = (headerType & CODER_NODE_TYPE_MASK) |
				CODER_OPTION_CODE_VALUE |
				static_cast<HeaderType>(
						optionSize << CODER_OPTION_SIZE_BASE_BITS);

		out.base().position(startOffset);
		out << headerType;
		out << bodySize;
		out.base().position(tailOffset + optionSize);

		ev.setTailMetaMessageSize(static_cast<uint16_t>(optionSize));
	}
	catch (...) {
		out.base().position(tailOffset);
		throw;
	}
}

template<typename T>
inline void EventEngine::DefaultEventCoder::putValueUnchecked(
		uint8_t *&out, const T &value) {
	memcpy(out, &value, sizeof(value));
	out += sizeof(value);
}


const char8_t *const EventEngine::Stats::STATS_TYPE_NAMES[] = {
	"MAIN_LISTENER_COUNT",
	"MAIN_IO_WORKER_COUNT",
	"MAIN_EVENT_WORKER_COUNT",
	"MAIN_THREAD_ERROR_COUNT",
	"MAIN_TOTAL_ALLOC_SIZE",
	"MAIN_FREE_ALLOC_SIZE",
	"MAIN_HUGE_ALLOC_COUNT",
	"MAIN_HUGE_ALLOC_SIZE",

	"ND_SERVER_CREATE_COUNT",
	"ND_SERVER_REMOVE_COUNT",
	"ND_CLIENT_CREATE_COUNT",
	"ND_CLIENT_REMOVE_COUNT",
	"ND_MCAST_CREATE_COUNT",
	"ND_MCAST_REMOVE_COUNT",
	"ND_MODIFY_COUNT",
	"ND_MODIFY_ERROR_COUNT",
	"ND_USER_DATA_CREATE_COUNT",
	"ND_USER_DATA_REMOVE_COUNT",

	"SOCKET_CREATE_COUNT",
	"SOCKET_REMOVE_COUNT",

	"CLOCK_INITIALIZE_COUNT",
	"CLOCK_INITIALIZE_ERROR_COUNT",
	"CLOCK_UPDATE_ERROR_COUNT",

	"LISTENER_BIND_COUNT",
	"LISTENER_BIND_ERROR_COUNT",

	"WORKER_DISPATCH_COUNT",
	"WORKER_TIMED_DISPATCH_COUNT",
	"WORKER_DISPATCH_TIMEOUT_MAX",
	"WORKER_DISPATCH_TIMEOUT_MIN",
	"WORKER_IMMEDIATE_DISPATCH_COUNT",
	"WORKER_CLOSING_DISPATCH_COUNT",
	"WORKER_WAIT_START_COUNT",
	"WORKER_WAIT_ERROR_COUNT",
	"WORKER_WAIT_TIME_MAX",
	"WORKER_WAIT_TIME_TOTAL",
	"WORKER_HANDLED_EVENT_COUNT",
	"WORKER_LOCAL_EVENT_COUNT",
	"WORKER_UNKNOWN_EVENT_COUNT",
	"WORKER_STOPPED_THREAD_COUNT",
	"WORKER_HANDLING_THREAD_COUNT",
	"WORKER_IDLE_THREAD_COUNT",
	"WORKER_WAITING_THREAD_COUNT",

	"IO_CYCLE_COUNT",
	"IO_MANAGED_SOCKET_COUNT",
	"IO_POLLING_SOCKET_COUNT",
	"IO_CONNECTING_SOCKET_COUNT",
	"IO_SUSPENDING_SOCKET_COUNT",
	"IO_REQUEST_CANCEL_COUNT",
	"IO_REQUEST_CONNECT_COUNT",
	"IO_REQUEST_ADD_COUNT",
	"IO_REQUEST_REMOVE_COUNT",
	"IO_REQUEST_MODIFY_COUNT",
	"IO_REQUEST_SUSPEND_COUNT",
	"IO_RECEIVE_BUFFER_SIZE_CURRENT",
	"IO_RECEIVE_BUFFER_SIZE_MAX",
	"IO_SEND_BUFFER_SIZE_CURRENT",
	"IO_SEND_BUFFER_SIZE_MAX",
	"IO_ERROR_OPEN_COUNT",
	"IO_ERROR_CONNECT_COUNT",
	"IO_ERROR_RECEIVE_COUNT",
	"IO_ERROR_SEND_COUNT",
	"IO_ERROR_OTHER_COUNT",

	"EVENT_CYCLE_COUNT",
	"EVENT_ACTIVE_ADD_COUNT",
	"EVENT_PENDING_ADD_COUNT",
	"EVENT_CANCELED_ADD_COUNT",
	"EVENT_ACTIVE_QUEUE_SIZE_CURRENT",
	"EVENT_ACTIVE_QUEUE_SIZE_MAX",
	"EVENT_ACTIVE_BUFFER_SIZE_CURRENT",
	"EVENT_ACTIVE_BUFFER_SIZE_MAX",
	"EVENT_PENDING_QUEUE_SIZE_CURRENT",
	"EVENT_PENDING_QUEUE_SIZE_MAX",

	"EVENT_ACTIVE_EXECUTABLE_COUNT",
	"EVENT_CYCLE_HANDLING_COUNT",
	"EVENT_CYCLE_HANDLING_AFTER_COUNT"
};

const EventEngine::Stats::Type
EventEngine::Stats::STATS_MAX_INITIAL_VALUE_TYPES[] = {
	WORKER_DISPATCH_TIMEOUT_MAX
};

const EventEngine::Stats::Type
EventEngine::Stats::STATS_MIN_INITIAL_VALUE_TYPES[] = {
	WORKER_DISPATCH_TIMEOUT_MIN
};

const EventEngine::Stats::Type EventEngine::Stats::STATS_MAX_TYPES[] = {
	WORKER_DISPATCH_TIMEOUT_MAX,
	WORKER_WAIT_TIME_MAX,
	IO_SEND_BUFFER_SIZE_MAX,
	EVENT_ACTIVE_QUEUE_SIZE_MAX,
	EVENT_ACTIVE_BUFFER_SIZE_MAX,
	EVENT_PENDING_QUEUE_SIZE_MAX
};

const EventEngine::Stats::Type EventEngine::Stats::STATS_MIN_TYPES[] = {
	WORKER_DISPATCH_TIMEOUT_MIN
};

EventEngine::Stats::Stats() {
	for (size_t i = 0; i < STATS_TYPE_MAX; i++) {
		const Type type = static_cast<Type>(i);

		if (findType(STATS_MAX_INITIAL_VALUE_TYPES, type)) {
			valueList_[i] = std::numeric_limits<int64_t>::min();
		}
		else if (findType(STATS_MIN_INITIAL_VALUE_TYPES, type)) {
			valueList_[i] = std::numeric_limits<int64_t>::max();
		}
		else {
			valueList_[i] = 0;
		}
	}
}

int64_t EventEngine::Stats::get(Type type) const {
	assert(0 <= type && type < STATS_TYPE_MAX);
	return valueList_[type];
}

void EventEngine::Stats::set(Type type, int64_t value) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	valueList_[type] = value;
}

void EventEngine::Stats::updateMax(Type type, int64_t value) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	valueList_[type] = std::max(valueList_[type], value);
}

void EventEngine::Stats::updateMin(Type type, int64_t value) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	valueList_[type] = std::min(valueList_[type], value);
}

void EventEngine::Stats::merge(Type type, int64_t value, bool sumOnly) {
	assert(0 <= type && type < STATS_TYPE_MAX);

	if (!sumOnly) {
		if (findType(STATS_MAX_TYPES, type)) {
			updateMax(type, value);
		}
		else if (findType(STATS_MIN_TYPES, type)) {
			updateMin(type, value);
		}
		return;
	}

	valueList_[type] += value;
}

void EventEngine::Stats::mergeAll(const Stats &stats, bool sumOnly) {
	for (size_t i = 0; i < STATS_TYPE_MAX; i++) {
		merge(static_cast<Type>(i), stats.valueList_[i], sumOnly);
	}
}

const char8_t* EventEngine::Stats::typeToString(Type type) {
	assert(0 <= type && type < STATS_TYPE_MAX);
	return STATS_TYPE_NAMES[type];
}

void EventEngine::Stats::dump(std::ostream &out) const {
	for (size_t i = 0; i < STATS_TYPE_MAX; i++) {
		const Type type = static_cast<Type>(i);

		out << std::endl << " " << typeToString(type) << "," << get(type);
	}
}

template<size_t N>
bool EventEngine::Stats::findType(const Type (&typeArray)[N], Type type) {
	return (std::find(typeArray, typeArray + N, type) != typeArray + N);
}


bool EventEngine::Tool::getLiveStats(
		EventEngine &ee, PartitionGroupId pgId, Stats::Type type,
		int64_t &value, const EventContext *ec, const Event *ev,
		const EventMonotonicTime *now) {
	value = std::numeric_limits<int64_t>::min();

	if(pgId >= ee.config_->concurrency_) {
		return false;
	}

	switch (type) {
	case Stats::EVENT_CYCLE_HANDLING_COUNT:
		if (ec == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "");
		}
		value = static_cast<int64_t>(
				Manipulator::getHandlingEventCount(*ec, NULL, true));
		return true;
	case Stats::EVENT_CYCLE_HANDLING_AFTER_COUNT:
		if (ec == NULL || ev == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "");
		}
		value = static_cast<int64_t>(
				Manipulator::getHandlingEventCount(*ec, ev, false));
		return true;
	default:
		break;
	}

	const EventMonotonicTime resolvedNow =
			(now == NULL ? ee.clockGenerator_->getMonotonicTime() : *now);
	return ee.eventWorkerList_[pgId].getLiveStats(type, resolvedNow, value);
}
