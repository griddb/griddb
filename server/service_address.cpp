﻿/*
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
	@brief Implementation of ServiceAddressResolver
*/
#include "service_address.h"
#include "http.h"
#include "gs_error_common.h"
#include "json.h"
#include "picojson.h"

#ifndef SERVICE_ADDRESS_CHECK_INSTANCE
#define SERVICE_ADDRESS_CHECK_INSTANCE 1
#endif

#if SERVICE_ADDRESS_CHECK_INSTANCE
template<typename T>
struct ClassInstanceChecker {
public:
	struct Scope;

	ClassInstanceChecker();

	void check();
	void update();

	template<size_t N>
	static void initialize(ClassInstanceChecker (&checkerList)[N], T *target);
	template<size_t N>
	static void destroy(ClassInstanceChecker (&checkerList)[N], T *target);

	static ClassInstanceChecker<T>* find(
			ClassInstanceChecker *checkerList, size_t count, const T *target);

	void format(std::ostream &os) const;

private:
	util::Atomic<int64_t> refCount_;
	T *target_;
	uint8_t data_[sizeof(T)];
};

template<typename T>
std::ostream& operator<<(
		std::ostream &os, const ClassInstanceChecker<T> &checker);

template<typename T>
struct ClassInstanceChecker<T>::Scope {
public:
	template<size_t N>
	Scope(
			ClassInstanceChecker (&checkerList)[N], T *target,
			bool enabled = true);
	~Scope();

	template<size_t N>
	void set(ClassInstanceChecker (&checkerList)[N], T *target);

	void clear();

private:
	ClassInstanceChecker<T> *checker_;
};

template<typename T>
ClassInstanceChecker<T>::ClassInstanceChecker() : target_(NULL) {
}

template<typename T>
void ClassInstanceChecker<T>::check() {
	if (memcmp(data_, target_, sizeof(T)) != 0) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION,
				"Inconsistent instance image (" << *this <<
				", trace=" << util::StackTraceUtils::getStackTrace << ")");
	}
}

template<typename T>
void ClassInstanceChecker<T>::update() {
	memcpy(data_, target_, sizeof(T));
}

template<typename T>
template<size_t N>
void ClassInstanceChecker<T>::initialize(
		ClassInstanceChecker (&checkerList)[N], T *target) {
	ClassInstanceChecker *checker = find(checkerList, N, target);
	if (checker != NULL) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION,
				"Inconsistent instance image (" <<
				"target=" << reinterpret_cast<uintptr_t>(target) <<
				", " << *checker <<
				", trace=" << util::StackTraceUtils::getStackTrace << ")");
	}

	for (size_t i = 0;; i++) {
		if (i >= N) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION,
					"Too many instance (" <<
					"target=" << reinterpret_cast<uintptr_t>(target) <<
					", trace=" << util::StackTraceUtils::getStackTrace << ")");
		}
		if (checkerList[i].target_ == NULL) {
			checker = &checkerList[i];
			break;
		}
	}

	assert(checker->refCount_ == 0);

	checker->refCount_ = 0;
	checker->target_ = target;
	checker->update();
}

template<typename T>
template<size_t N>
void ClassInstanceChecker<T>::destroy(
		ClassInstanceChecker (&checkerList)[N], T *target) {
	ClassInstanceChecker *checker = find(checkerList, N, target);
	if (checker != NULL) {
		assert(checker->refCount_ == 0);
		assert(memcmp(checker->data_, target, sizeof(T)) == 0);

		checker->refCount_ = 0;
		checker->target_ = NULL;
	}
	else {
		assert(false);
	}
}

template<typename T>
ClassInstanceChecker<T>* ClassInstanceChecker<T>::find(
		ClassInstanceChecker *checkerList, size_t count, const T *target) {
	for (size_t i = 0; i < count; i++) {
		if (checkerList[i].target_ == target) {
			return &checkerList[i];
		}
	}
	return NULL;
}

template<typename T>
void ClassInstanceChecker<T>::format(std::ostream &os) const {
	os << "addr=";
	os << reinterpret_cast<uintptr_t>(target_);

	os << ", refCount=";
	os << refCount_;

	const std::ios::fmtflags flags = os.setf(std::ios::hex, std::ios::basefield);
	const std::streamsize width = os.width();
	const std::ostream::char_type fill = os.fill();

	for (size_t i = 0; i < 2; i++) {
		if (memcmp(data_, target_, sizeof(T)) == 0) {
			if (i != 0) {
				break;
			}
			os << ", ";
			os << "image";
		}
		else {
			os << ", ";
			os << (i == 0 ? "expected" : "actual");
		}
		os << "=";

		const void *addr = (i == 0 ?
				static_cast<const void*>(data_) : target_);
		const uint8_t *begin = static_cast<const uint8_t*>(addr);
		const uint8_t *end = begin + sizeof(T);
		for (const uint8_t *it = begin; it != end; ++it) {
			os.width(2);
			os.fill('0');
			os << static_cast<uint32_t>(*it);
		}
	}

	os.flags(flags);
	os.width(width);
	os.fill(fill);
}

template<typename T>
std::ostream& operator<<(
		std::ostream &os, const ClassInstanceChecker<T> &checker) {
	checker.format(os);
	return os;
}

template<typename T>
template<size_t N>
ClassInstanceChecker<T>::Scope::Scope(
		ClassInstanceChecker (&checkerList)[N], T *target, bool enabled) :
		checker_(NULL) {
	if (!enabled) {
		assert(target == NULL);
		return;
	}

	set(checkerList, target);
}

template<typename T>
ClassInstanceChecker<T>::Scope::~Scope() {
	clear();
}

template<typename T>
template<size_t N>
void ClassInstanceChecker<T>::Scope::set(
		ClassInstanceChecker (&checkerList)[N], T *target) {
	clear();

	checker_ = find(checkerList, N, target);

	if (checker_ == NULL) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION,
				"Instance not found (" <<
				"target=" << reinterpret_cast<uintptr_t>(target) <<
				", trace=" << util::StackTraceUtils::getStackTrace << ")");
	}

	try {
		if (++checker_->refCount_ != 1) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION,
					"Multiple reference (" << *checker_ <<
					", trace=" << util::StackTraceUtils::getStackTrace << ")");
		}

		checker_->check();
	}
	catch (...) {
		--checker_->refCount_;
		throw;
	}
}

template<typename T>
void ClassInstanceChecker<T>::Scope::clear() {
	if (checker_ != NULL) {
		checker_->update();
		if (--checker_->refCount_ < 0) {
			assert(false);
		}
		checker_ = NULL;
	}
}
#endif 

struct ServiceAddressResolver::ProviderContext : public util::IOPollHandler {
	ProviderContext(ServiceAddressResolver &base);
	virtual ~ProviderContext();

	virtual void handlePollEvent(util::IOPollBase *io, util::IOPollEvent event);
	virtual util::File& getFile();

	void format(std::ostream &s) const;

#if SERVICE_ADDRESS_CHECK_INSTANCE
	typedef ClassInstanceChecker<ServiceAddressResolver> ResolverChecker;
	typedef ClassInstanceChecker<ProviderContext> ContextChecker;

	static ClassInstanceChecker<ServiceAddressResolver> resolverCheckers_[];
	static ClassInstanceChecker<ProviderContext> contextCheckers_[];
#endif

	ServiceAddressResolver &base_;
	HttpRequest request_;
	HttpResponse response_;
	util::SocketAddress address_;
	util::Socket socket_;
	util::IOPollEvent ioPollEvent_;
	bool connected_;
};

std::ostream& operator<<(
		std::ostream &s, const ServiceAddressResolver::ProviderContext &cxt);

#if SERVICE_ADDRESS_CHECK_INSTANCE
ClassInstanceChecker<ServiceAddressResolver>
ServiceAddressResolver::ProviderContext::resolverCheckers_[10];

ClassInstanceChecker<ServiceAddressResolver::ProviderContext>
ServiceAddressResolver::ProviderContext::contextCheckers_[10];
#endif

const char8_t ServiceAddressResolver::JSON_KEY_ADDRESS[] = "address";
const char8_t ServiceAddressResolver::JSON_KEY_PORT[] = "port";

ServiceAddressResolver::ServiceAddressResolver(
		const Allocator &alloc, const Config &config) :
		alloc_((initializeRaw(), alloc)),
		config_(config),
		providerURL_(alloc),
		typeList_(alloc),
		typeMap_(TypeMap::key_compare(), alloc),
		addressSet_(AddressSet::key_compare(), alloc),
		entryList_(alloc),
		initialized_(false),
		changed_(false),
		normalized_(false),
		providerCxt_(NULL) {

	checkConfig(alloc, config);

	if (config_.providerURL_ != NULL) {
		providerURL_ = config_.providerURL_;
		config_.providerURL_ = providerURL_.c_str();
	}

	util::StdAllocator<ProviderContext, void> cxtAlloc(alloc_);
	void *providerCxtAddr = cxtAlloc.allocate(1);
#if SERVICE_ADDRESS_CHECK_INSTANCE
	memset(providerCxtAddr, 0, sizeof(ProviderContext));
#endif
	providerCxt_ = new (providerCxtAddr) ProviderContext(*this);

#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::initialize(
			ProviderContext::resolverCheckers_, this);
	ProviderContext::ContextChecker::initialize(
			ProviderContext::contextCheckers_, providerCxt_);
#endif
}

ServiceAddressResolver::~ServiceAddressResolver() {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ContextChecker::destroy(
			ProviderContext::contextCheckers_, providerCxt_);
	ProviderContext::ResolverChecker::destroy(
			ProviderContext::resolverCheckers_, this);
#endif

	util::StdAllocator<ProviderContext, void> cxtAlloc(alloc_);
	cxtAlloc.destroy(providerCxt_);
	cxtAlloc.deallocate(providerCxt_, 1);
}

const ServiceAddressResolver::Config&
ServiceAddressResolver::getConfig() const {
	return config_;
}

void ServiceAddressResolver::checkConfig(
		const Allocator &alloc, const Config &config) {
	if (config.providerURL_ == NULL) {
		return;
	}

	HttpRequest request(alloc);
	request.acceptURL(config.providerURL_);

	if (HttpMessage::FieldParser::compareToken(
			request.getScheme(), "http") != 0) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INVALID_CONFIG,
				"Only HTTP is supported for provider URL"
				" (url=" << config.providerURL_ << ")");
	}

	if (strlen(request.getHost()) == 0) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INVALID_CONFIG,
				"No host specified in provider URL (url=" <<
				config.providerURL_ << ")");
	}

	util::SocketAddress(request.getHost(), request.getPort());
}

void ServiceAddressResolver::initializeType(
		const ServiceAddressResolver &another) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	if (initialized_) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION, "");
	}

	typeList_ = another.typeList_;
	typeMap_ = another.typeMap_;
}

void ServiceAddressResolver::initializeType(
		uint32_t type, const char8_t *name) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	if (initialized_) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION, "");
	}

	String nameStr(name, alloc_);
	if (nameStr.empty() || typeMap_.find(nameStr) != typeMap_.end()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
	}

	if (type > typeList_.max_size() ||
			(type < typeList_.size() && !typeList_[type].empty())) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
	}

	const size_t minSize = type + 1;
	typeList_.resize(std::max(minSize, typeList_.size()), String(alloc_));

	typeMap_.insert(std::make_pair(nameStr, type));
	typeList_[type].swap(nameStr);
}

uint32_t ServiceAddressResolver::getTypeCount() const {
	return static_cast<uint32_t>(typeMap_.size());
}

uint32_t ServiceAddressResolver::getType(const char8_t *name) const {
	const String nameStr(name, alloc_);
	TypeMap::const_iterator it = typeMap_.find(nameStr);

	if (it == typeMap_.end()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
	}

	return it->second;
}

bool ServiceAddressResolver::update() try {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
	ProviderContext::ContextChecker::Scope cxtCheckerScope(
			ProviderContext::contextCheckers_, providerCxt_);
#endif

	completeInit();

	if (providerURL_.empty()) {
		return true;
	}

	updated_.second = false;

	HttpRequest &request = providerCxt_->request_;
	HttpResponse &response = providerCxt_->response_;
	util::SocketAddress &address = providerCxt_->address_;
	util::Socket &socket = providerCxt_->socket_;
	util::IOPollEvent &ioPollEvent = providerCxt_->ioPollEvent_;
	bool &connected = providerCxt_->connected_;

	request.clear();
	response.clear();
	address.clear();
	socket.close();
	ioPollEvent = util::IOPollEvent();
	connected = false;

	request.getMessage().addHeader(
			HttpMessage::HEADER_ACCEPT, HttpMessage::CONTENT_TYPE_JSON, true);
	request.acceptURL(providerURL_.c_str());
	request.build();

	address.assign(request.getHost(), request.getPort());

	socket.open(address.getFamily(), util::Socket::TYPE_STREAM);
	socket.setBlockingMode(false);
	connected = socket.connect(address);

#if SERVICE_ADDRESS_CHECK_INSTANCE
	checkerScope.clear();
	cxtCheckerScope.clear();
#endif

	assert(!socket.isClosed());
	return (connected && checkUpdated());
}
catch (...) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ContextChecker::Scope cxtCheckerScope(
			ProviderContext::contextCheckers_, providerCxt_);
#endif

	providerCxt_->socket_.close();
	throw;
}

bool ServiceAddressResolver::checkUpdated(size_t *readSize) try {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
	ProviderContext::ContextChecker::Scope cxtCheckerScope(
			ProviderContext::contextCheckers_, providerCxt_);
#endif

	if (readSize != NULL) {
		*readSize = 0;
	}

	completeInit();

	if (providerURL_.empty() || updated_.second) {
		return true;
	}

	HttpRequest &request = providerCxt_->request_;
	HttpResponse &response = providerCxt_->response_;
	util::Socket &socket = providerCxt_->socket_;
	util::IOPollEvent &ioPollEvent = providerCxt_->ioPollEvent_;
	const bool &connected = providerCxt_->connected_;

	if (socket.isClosed()) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
		checkerScope.clear();
		cxtCheckerScope.clear();
#endif
		return update();
	}

	ioPollEvent = util::IOPollEvent::TYPE_READ_WRITE;
	if (!connected) {
		util::IOPollSelect select;
		select.add(&socket, util::IOPollEvent::TYPE_READ_WRITE);
		if (!select.dispatch(0)) {
			return false;
		}
	}

	if (!request.getMessage().isWrote() &&
			!request.getMessage().writeTo(socket)) {
		return false;
	}

	ioPollEvent = util::IOPollEvent::TYPE_READ;

	const bool eof = response.getMessage().readFrom(socket, readSize);
	if (!response.parse(eof)) {
		return false;
	}

	socket.close();
	ioPollEvent = util::IOPollEvent();

	response.checkSuccess();

#if SERVICE_ADDRESS_CHECK_INSTANCE
	checkerScope.clear();
#endif

	importFrom(response.getMessage().toJsonValue());

#if SERVICE_ADDRESS_CHECK_INSTANCE
	checkerScope.set(ProviderContext::resolverCheckers_, this);
#endif

	updated_ = std::make_pair(true, true);

	return true;
}
catch (...) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ContextChecker::Scope cxtCheckerScope(
			ProviderContext::contextCheckers_, providerCxt_);
#endif

	providerCxt_->socket_.close();

	std::exception e;
	GS_COMMON_RETHROW_USER_ERROR(
			e, GS_COMMON_EXCEPTION_MESSAGE(e) <<
			" (" << *providerCxt_ << ")");
}

bool ServiceAddressResolver::isChanged() const {
	return changed_;
}

bool ServiceAddressResolver::isAvailable() const {
	if (providerURL_.empty()) {
		return true;
	}

	return updated_.first;
}

bool ServiceAddressResolver::isSameEntries(
		const ServiceAddressResolver &another) const {
	return isSameEntries(
			entryList_, normalized_, another.entryList_, another.normalized_);
}

size_t ServiceAddressResolver::getEntryCount() const {
	return entryList_.size();
}

util::SocketAddress ServiceAddressResolver::getAddress(
		size_t index, uint32_t type) const {
	checkType(type);
	checkEntry(index);

	const util::SocketAddress &address = entryList_[index].list_[type];
	if (address.isEmpty()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION, "");
	}

	return address;
}

void ServiceAddressResolver::setAddress(
		size_t index, uint32_t type, const util::SocketAddress &addr) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	completeInit();
	checkType(type);

	AddressSet::iterator storedIt = addressSet_.end();
	if (index < entryList_.size()) {
		const util::SocketAddress &storedAddr = entryList_[index].list_[type];
		if (!storedAddr.isEmpty()) {
			storedIt = addressSet_.find(storedAddr);
			assert(storedIt != addressSet_.end());
		}
	}

	if (!addr.isEmpty()) {
		AddressSet::iterator it = addressSet_.find(addr);
		if (it != addressSet_.end()) {
			if (it == storedIt) {
				return;
			}
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_SA_ADDRESS_CONFLICTED,
					"Address conflicted (index=" << index <<
					", type=" << getTypeName(type) <<
					", address=" << addr << ")");
		}

		const int family = config_.addressFamily_;
		if (family != 0 && addr.getFamily() != family) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_SA_INVALID_ADDRESS,
					"Address family unmatched (index=" << index <<
					", type=" << getTypeName(type) <<
					", address=" << addr <<
					", expectedFamily=" <<
							(family == util::SocketAddress::FAMILY_INET ?
							"IPv4" : "IPv6") << ")");
		}
	}

	const size_t orgSize = entryList_.size();
	const size_t typeCount = getTypeCount();
	if (index >= orgSize) {
		if (index > entryList_.max_size()) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
		}
		entryList_.resize(index + 1, Entry(alloc_, typeCount));
	}

	if (!addr.isEmpty()) {
		try {
			addressSet_.insert(addr);
		}
		catch (...) {
			entryList_.resize(orgSize, Entry(alloc_, 0));
			throw;
		}
	}

	if (storedIt != addressSet_.end()) {
		addressSet_.erase(storedIt);
	}

	entryList_[index].list_[type] = addr;
	normalized_ = false;
}

void ServiceAddressResolver::importFrom(
		const picojson::value &value, bool strict) {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	completeInit();

	Config config = config_;
	config.providerURL_ = NULL;

	ServiceAddressResolver another(alloc_, config);
	another.initializeType(*this);

	const u8string addrName = JSON_KEY_ADDRESS;
	const u8string portName = JSON_KEY_PORT;

	const picojson::array &list = JsonUtils::as<picojson::array>(value);

	for (picojson::array::const_iterator entryIt = list.begin();
			entryIt != list.end(); ++entryIt) {
		const size_t index = entryIt - list.begin();
		JsonUtils::Path entryPath = JsonUtils::Path().indexed(index);

		for (TypeList::const_iterator typeIt = typeList_.begin();
				typeIt != typeList_.end(); ++typeIt) {

			JsonUtils::Path typePath = entryPath.child();
			const uint32_t type =
					static_cast<uint32_t>(typeIt - typeList_.begin());
			const char8_t *typeName = typeIt->c_str();

			const picojson::value *addrObj = (strict ?
					&JsonUtils::as<picojson::value>(
							*entryIt, typeName, &typePath) :
					JsonUtils::find<picojson::value>(
							*entryIt, typeName, &typePath));
			if (addrObj == NULL) {
				continue;
			}

			JsonUtils::Path addrPath = typePath.child();
			const u8string &host = JsonUtils::as<u8string>(
					*addrObj, addrName, &addrPath);
			const int64_t port = JsonUtils::asInt<int64_t>(
					*addrObj, portName, &addrPath);

			const util::SocketAddress &addr =
					makeSocketAddress(host.c_str(), port);

			another.setAddress(index, type, addr);
		}
	}

	if (strict) {
		another.normalize();
		another.validate();
	}

	changed_ = !isSameEntries(another);

#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope anotherCheckerScope(
			ProviderContext::resolverCheckers_, &another);
#endif

	std::swap(addressSet_, another.addressSet_);
	std::swap(entryList_, another.entryList_);
	std::swap(normalized_, another.normalized_);
}

bool ServiceAddressResolver::exportTo(picojson::value &value) const try {
	value = picojson::value();

	if (!isAvailable() || !initialized_) {
		return false;
	}

	const u8string addrName = JSON_KEY_ADDRESS;
	const u8string portName = JSON_KEY_PORT;

	value = picojson::value(picojson::array());
	picojson::array &list = value.get<picojson::array>();

	for (EntryList::const_iterator entryIt = entryList_.begin();
			entryIt != entryList_.end(); ++entryIt) {

		const Entry &entry = *entryIt;

		list.push_back(picojson::value(picojson::object()));
		picojson::object &entryObj = list.back().get<picojson::object>();

		for (TypeList::const_iterator typeIt = typeList_.begin();
				typeIt != typeList_.end(); ++typeIt) {

			const uint32_t type =
					static_cast<uint32_t>(typeIt - typeList_.begin());
			const char8_t *typeName = typeIt->c_str();

			const util::SocketAddress &addr = entry.list_[type];
			if (addr.isEmpty()) {
				continue;
			}

			picojson::value &addrValue = entryObj[typeName];
			addrValue = picojson::value(picojson::object());

			picojson::object &addrObj = addrValue.get<picojson::object>();

			u8string host;
			addr.getName(&host);
			addrObj[addrName] = picojson::value(host);

			addrObj[portName] =
					picojson::value(static_cast<double>(addr.getPort()));
		}
	}

	return true;
}
catch (...) {
	value = picojson::value();
	throw;
}

void ServiceAddressResolver::validate() {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	if (!isAvailable() || entryList_.empty()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_ADDRESS_NOT_ASSIGNED,
				"No available address found");
	}

	if (!(typeList_.size() * entryList_.size() == addressSet_.size())) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_ADDRESS_NOT_ASSIGNED,
				"One or more addresses are not assigned");
	}
}

void ServiceAddressResolver::normalize() {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	ProviderContext::ResolverChecker::Scope checkerScope(
			ProviderContext::resolverCheckers_, this);
#endif

	completeInit();

	if (!normalized_) {
		normalizeEntries(&entryList_);
		normalized_ = true;
	}
}

util::IOPollHandler* ServiceAddressResolver::getIOPollHandler() {
	if (updated_.second ||
			providerCxt_->socket_.isClosed() ||
			providerCxt_->ioPollEvent_ == util::IOPollEvent()) {
		return NULL;
	}

	return providerCxt_;
}

util::IOPollEvent ServiceAddressResolver::getIOPollEvent() {
	if (getIOPollHandler() == NULL) {
		return util::IOPollEvent();
	}

	return providerCxt_->ioPollEvent_;
}

util::SocketAddress ServiceAddressResolver::makeSocketAddress(
		const char8_t *host, int64_t port) {
	if (port < 0 || port >
			static_cast<int64_t>(std::numeric_limits<uint16_t>::max())) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INVALID_ADDRESS,
				"Port out of range (host=" << host << ", port=" << port << ")");
	}

	const uint16_t validPort = static_cast<uint16_t>(port);
	return util::SocketAddress(host, validPort, config_.addressFamily_);
}

void ServiceAddressResolver::completeInit() {
	if (initialized_) {
		return;
	}

	if (typeList_.empty() || typeList_.size() != typeMap_.size()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_OPERATION, "");
	}

	initialized_ = true;
}

void ServiceAddressResolver::initializeRaw() {
#if SERVICE_ADDRESS_CHECK_INSTANCE
	memset(this, 0, sizeof(*this));
#endif
}

void ServiceAddressResolver::checkEntry(size_t index) const {
	if (index >= entryList_.size()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
	}
}

void ServiceAddressResolver::checkType(uint32_t type) const {
	if (type >= typeList_.size() || typeList_[type].empty()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_SA_INTERNAL_ILLEGAL_PARAMETER, "");
	}
}

const char8_t* ServiceAddressResolver::getTypeName(uint32_t type) const {
	checkType(type);
	return typeList_[type].c_str();
}

bool ServiceAddressResolver::isSameEntries(
		const EntryList &list1, bool normalized1,
		const EntryList &list2, bool normalized2) {
	const size_t size = list1.size();
	if (size != list2.size()) {
		return false;
	}

	if (!normalized1) {
		const Allocator &alloc = list1.get_allocator();
		EntryList normalizedList(list1.begin(), list1.end(), alloc);
		normalizeEntries(&normalizedList);
		return isSameEntries(normalizedList, true, list2, normalized2);
	}

	if (!normalized2) {
		const Allocator &alloc = list1.get_allocator();
		EntryList normalizedList(list2.begin(), list2.end(), alloc);
		normalizeEntries(&normalizedList);
		return isSameEntries(list1, normalized1, normalizedList, true);
	}

	for (size_t i = 0; i < size; i++) {
		if (list1[i].compare(list2[i]) != 0) {
			return false;
		}
	}

	return true;
}

void ServiceAddressResolver::normalizeEntries(EntryList *entryList) {
	std::sort(entryList->begin(), entryList->end(), EntryLess());
}

ServiceAddressResolver::Config::Config() :
		providerURL_(NULL),
		addressFamily_(util::SocketAddress::FAMILY_INET) {
}

ServiceAddressResolver::Entry::Entry(const Allocator &alloc, size_t typeCount) :
		list_(alloc) {
	list_.resize(typeCount);
}

int32_t ServiceAddressResolver::Entry::compare(const Entry &another) const {
	const size_t size1 = list_.size();
	const size_t size2 = another.list_.size();
	if (size1 < size2) {
		return -1;
	}
	else if (size1 > size2) {
		return 1;
	}

	for (size_t i = 0; i < size1; i++) {
		const int32_t comp = list_[i].compare(another.list_[i]);
		if (comp != 0) {
			return comp;
		}
	}

	return 0;
}

bool ServiceAddressResolver::EntryLess::operator()(
		const Entry &entry1, const Entry &entry2) const {
	return entry1.compare(entry2) < 0;
}

ServiceAddressResolver::ProviderContext::ProviderContext(
		ServiceAddressResolver &base) :
		base_(base),
		request_(base.alloc_),
		response_(base.alloc_),
		ioPollEvent_(util::IOPollEvent()),
		connected_(false) {
}

ServiceAddressResolver::ProviderContext::~ProviderContext() {
}

void ServiceAddressResolver::ProviderContext::handlePollEvent(
		util::IOPollBase *io, util::IOPollEvent event) {
	static_cast<void>(io);
	static_cast<void>(event);
	base_.checkUpdated();
}

util::File& ServiceAddressResolver::ProviderContext::getFile() {
	return socket_;
}

void ServiceAddressResolver::ProviderContext::format(std::ostream &s) const {
	s << "address=" << address_;

	s << ", connectedImmediately=";
	s << (connected_ ? "true" : "false");

	s << ", requestSize=" << request_.getMessage().getMessageSize();
	s << ", requestSent=" <<
			(request_.getMessage().isWrote() ? "true" : "false");
	s << ", requestSentSize=" <<
			const_cast<HttpMessage&>(request_.getMessage()).getWroteSize();

	s << ", response=" << response_.formatter();
	s << ", request=" << request_.formatter();

#if SERVICE_ADDRESS_CHECK_INSTANCE
	ResolverChecker *resolverChecker = ResolverChecker::find(
			resolverCheckers_,
			sizeof(resolverCheckers_) / sizeof(*resolverCheckers_),
			&base_);
	if (resolverChecker != NULL) {
		s << ", resolver={" << *resolverChecker << "}";
	}

	ContextChecker *contextChecker = ContextChecker::find(
			contextCheckers_,
			sizeof(contextCheckers_) / sizeof(*contextCheckers_),
			this);
	if (contextChecker != NULL) {
		s << ", context={" << *contextChecker << "}";
	}
#endif
}

std::ostream& operator<<(
		std::ostream &s, const ServiceAddressResolver::ProviderContext &cxt) {
	cxt.format(s);
	return s;
}
