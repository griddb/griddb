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
	@brief Implementation of Auth class
*/
#include "authentication.h"
#include "gs_error_common.h"
#include "sha2.h"
#include "uuid_utils.h"



const Auth::Mode Auth::Challenge::DEFAULT_MODE = Auth::MODE_CHALLENGE;

const char8_t *const Auth::Challenge::DEFAULT_METHOD = "POST";
const char8_t *const Auth::Challenge::DEFAULT_REALM = "DB_Auth";
const char8_t *const Auth::Challenge::DEFAULT_URI = "/";
const char8_t *const Auth::Challenge::DEFAULT_QOP = "auth";
const char8_t *const Auth::Challenge::DIGEST_PREFIX = "#1#";

const size_t Auth::Challenge::BASE_SALT_SIZE = MAX_BASE_SALT_SIZE / 2;
const int32_t Auth::Challenge::CRYPT_SALT_FACTOR = 4;

bool Auth::Challenge::challengeEnabled_ = false;

Auth::Challenge::Challenge() {
	clear();
}

void Auth::Challenge::initialize(
		const char8_t *nonce, const char8_t *nc, const char8_t *opaque,
		const char8_t *baseSalt) {
	if (strlen(nonce) == 0 || strlen(nc) == 0 || strlen(opaque) == 0 ||
			strlen(baseSalt) == 0) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}

	clear();

	copyToFixedStr(nonce_, nonce);
	copyToFixedStr(nc_, nc);
	copyToFixedStr(opaque_, opaque);
	copyToFixedStr(baseSalt_, baseSalt);
}

bool Auth::Challenge::isEmpty() const {
	return strlen(nonce_) == 0 &&
			strlen(nc_) == 0 &&
			strlen(opaque_) == 0 &&
			strlen(baseSalt_) == 0 &&
			strlen(cnonce_) == 0;
}

void Auth::Challenge::clear() {
	initializeFixedStr(nonce_);
	initializeFixedStr(nc_);
	initializeFixedStr(opaque_);
	initializeFixedStr(baseSalt_);
	initializeFixedStr(cnonce_);
}

Auth::PasswordDigest Auth::Challenge::makeDigest(
		const Allocator &alloc,
		const char8_t *user, const char8_t *password) {

	String basicSecret(alloc);
	sha256Hash(String(alloc) + password, &basicSecret);

	String cryptBase(alloc);
	if (challengeEnabled_) {
		sha256Hash(String(alloc) + user + ":" + password, &cryptBase);
	}

	String challengeBase(alloc);
	if (challengeEnabled_) {
		md5Hash(
				String(alloc) + user + ":" + DEFAULT_REALM + ":" + password,
				&challengeBase);
	}

	return PasswordDigest(
			alloc,
			basicSecret.c_str(), cryptBase.c_str(), challengeBase.c_str());
}

void Auth::Challenge::makeStoredDigest(
		const char8_t *user, const char8_t *password, String *digestStr,
		bool compatible) {
	const Allocator &alloc = digestStr->get_allocator();
	const PasswordDigest &digest = makeDigest(alloc, user, password);

	if (!challengeEnabled_) {
		*digestStr += digest.basicSecret_;
		return;
	}

	String baseSalt(alloc);
	randomHexString(BASE_SALT_SIZE, &baseSalt);

	String salt(alloc);
	generateBcryptSalt(CRYPT_SALT_FACTOR, &salt);

	String cryptSecret(alloc);
	sha256Hash(
			String(alloc) + baseSalt + ":" + digest.cryptBase_, &cryptSecret);

	*digestStr += DIGEST_PREFIX + digest.challengeBase_ + "#" + baseSalt + "#";
	getBcryptHash(cryptSecret.c_str(), salt.c_str(), digestStr);
	*digestStr += "#";

	if (compatible) {
		*digestStr += digest.basicSecret_;
	}
}

void Auth::Challenge::getRequest(
		util::ArrayByteInStream &in, const Allocator &alloc,
		Mode &reqMode, Challenge &challenge, bool full) {
	reqMode = getMode(in);

	if (reqMode == MODE_NONE) {
		challenge.clear();
		return;
	}

	int8_t challenged;
	in >> challenged;

	if (challenged) {
		String opaque(alloc);
		String cnonce(alloc);

		in >> opaque;
		in >> cnonce;

		if (opaque != challenge.opaque_) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
		}

		copyToFixedStr(challenge.opaque_, opaque.c_str());
		copyToFixedStr(challenge.cnonce_, cnonce.c_str());

		if (full) {
			String nonce(alloc);
			String nc(alloc);
			String baseSalt(alloc);

			in >> nonce;
			in >> nc;
			in >> baseSalt;

			copyToFixedStr(challenge.nonce_, nonce.c_str());
			copyToFixedStr(challenge.nc_, nc.c_str());
			copyToFixedStr(challenge.baseSalt_, baseSalt.c_str());
		}
	}
}

bool Auth::Challenge::getResponse(
		util::ArrayByteInStream &in, const Allocator &alloc, Mode &mode,
		Challenge &challenge) {
	const Mode respMode = getMode(in);
	if (respMode != MODE_NONE && !challengeEnabled_) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}

	if (respMode != mode) {
		if (respMode == MODE_BASIC) {
			mode = MODE_BASIC;
			challenge.clear();
			return false;
		}
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}
	else if (respMode == MODE_NONE) {
		return true;
	}

	int8_t respChallenging = 0;
	in >> respChallenging;

	const bool challenging = (mode == MODE_CHALLENGE && !challenge.isEmpty());
	if (respMode != MODE_BASIC && !(!!respChallenging ^ challenging)) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}

	if (respChallenging) {
		String nonce(alloc);
		String nc(alloc);
		String opaque(alloc);
		String baseSalt(alloc);

		in >> nonce;
		in >> nc;
		in >> opaque;
		in >> baseSalt;

		challenge.initialize(
				nonce.c_str(), nc.c_str(), opaque.c_str(), baseSalt.c_str());
		return false;
	}
	else {
		challenge.clear();
		return true;
	}
}

bool Auth::Challenge::authenticate(
		const Allocator &alloc, const char8_t *user,
		const char8_t *reqDigest, const char8_t *storedDigest,
		bool plain, Mode &mode) {

	if (mode == MODE_CHALLENGE && plain) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}

	if (storedDigest == NULL) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INVALID_CREDENTIALS,
				"Unknown user (user=" << user << ")");
	}

	String reqChallengeDigest(alloc);
	String reqCryptSecret(alloc);
	String reqBasicSecret(alloc);
	String *reqList[] = { &reqChallengeDigest, &reqCryptSecret };
	parseDigest(
			reqDigest, reqList, sizeof(reqList) / sizeof(*reqList),
			&reqBasicSecret);

	String challengeBase(alloc);
	String baseSalt(alloc);
	String crypt(alloc);
	String basicSecret(alloc);
	String *storedList[] = { &challengeBase, &baseSalt, &crypt, &basicSecret };
	parseDigest(
			storedDigest, storedList, sizeof(storedList) / sizeof(*storedList),
			&basicSecret);

	if (mode == MODE_CHALLENGE && isEmpty()) {
		clear();

		if (challengeBase.empty() || baseSalt.empty() || crypt.empty()) {
			mode = MODE_BASIC;
		}
		else {
			String nonce(alloc);
			String nc(alloc);
			String opaque(alloc);

			randomHexString(MAX_NONCE_SIZE / 2, &nonce);
			randomHexString(MAX_NC_SIZE / 2, &nc);
			randomHexString(MAX_OPAQUE_SIZE / 2, &opaque);

			copyToFixedStr(nonce_, nonce.c_str());
			copyToFixedStr(nc_, nc.c_str());
			copyToFixedStr(opaque_, opaque.c_str());
			copyToFixedStr(baseSalt_, baseSalt.c_str());
		}
		return false;
	}
	else if (mode == MODE_CHALLENGE) {
		const PasswordDigest digest(
				alloc, basicSecret.c_str(), "", challengeBase.c_str());

		String challengeDigest(alloc);
		getChallengeDigest(digest, cnonce_, &challengeDigest);

		if (challengeDigest != reqChallengeDigest ||
				!compareBcryptPassword(
						reqCryptSecret.c_str(), crypt.c_str())) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_AUTH_INVALID_CREDENTIALS,
					"Invalid user or password "
					"(user=" << user << ", mode=CHALLENGE)");
		}

		clear();
		return true;
	}
	else {
		if (plain) {
			const String reqPlain = reqBasicSecret;
			reqBasicSecret = "";
			sha256Hash(reqPlain, &reqBasicSecret);
		}

		if (basicSecret != reqBasicSecret) {
			GS_COMMON_THROW_USER_ERROR(
					GS_ERROR_AUTH_INVALID_CREDENTIALS,
					"Invalid user or password "
					"(user=" << user << ", mode=BASIC)");
		}

		clear();
		return true;
	}
}

void Auth::Challenge::build(
		Mode mode, Challenge &challenge,
		const PasswordDigest &digest, String *challengeStr) {
	if (challenge.isEmpty()) {
		if (mode == MODE_CHALLENGE) {
			*challengeStr += "";
		}
		else {
			*challengeStr += digest.basicSecret_;
		}
		return;
	}

	challenge.build(digest, challengeStr);
}

void Auth::Challenge::parseDigest(
		const char8_t *digestStr, String **outList, size_t outCount,
		String *alternative) {
	if (strstr(digestStr, DIGEST_PREFIX) == digestStr) {
		const char8_t *it = digestStr + strlen(DIGEST_PREFIX);
		const char8_t *const end = it + strlen(it);
		const char8_t *delim;

		for (size_t i = 0; i < outCount; i++) {
			delim = ((delim = strchr(it, '#')) == NULL ? end : delim);
			outList[i]->append(it, delim);
			it = delim + (delim == end ? 0 : 1);
		}
	}
	else {
		alternative->append(digestStr);
	}
}

const char8_t* Auth::Challenge::getOpaque() const {
	return opaque_;
}

const char8_t* Auth::Challenge::getLastCNonce() const {
	return cnonce_;
}

void Auth::Challenge::generateCNonce(String *cnonce) {
	randomHexString(4, cnonce);
}

void Auth::Challenge::build(
		const PasswordDigest &digest, String *challengeStr) {
	const Allocator &alloc = challengeStr->get_allocator();

	String cnonce(alloc);
	generateCNonce(&cnonce);

	build(digest, cnonce.c_str(), challengeStr);
	copyToFixedStr(cnonce_, cnonce.c_str());
}

void Auth::Challenge::build(
		const PasswordDigest &digest, const char8_t *cnonce,
		String *challengeStr) const {

	*challengeStr += DIGEST_PREFIX;
	getChallengeDigest(digest, cnonce, challengeStr);

	*challengeStr += "#";
	getCryptSecret(digest, challengeStr);
}

void Auth::Challenge::getChallengeDigest(
		const PasswordDigest &digest, const char8_t *cnonce,
		String *challengeStr) const {
	const Allocator &alloc = challengeStr->get_allocator();
	struct {} cnonce_;
	static_cast<void>(cnonce_);

	String ha1(alloc);
	md5Hash(
			String(alloc) + digest.challengeBase_ + ":" + nonce_ + ":" +
			cnonce, &ha1);

	String ha2(alloc);
	md5Hash(
			String(alloc) + DEFAULT_METHOD + ":" + DEFAULT_URI, &ha2);

	md5Hash(
			ha1 + ":" + nonce_ + ":" + nc_ + ":" + cnonce + ":" +
			DEFAULT_QOP + ":" + ha2, challengeStr);
}

void Auth::Challenge::getCryptSecret(
		const PasswordDigest &digest, String *challengeStr) const {
	const Allocator &alloc = challengeStr->get_allocator();
	return sha256Hash(
			String(alloc) + baseSalt_ + ":" + digest.cryptBase_, challengeStr);
}

void Auth::Challenge::sha256Hash(const String &value, String *hash) {
	sha256Hash(value.c_str(), value.size(), hash);
}

void Auth::Challenge::sha256Hash(
		const void *value, size_t size, String *hash) {
	char8_t digest[SHA256_DIGEST_STRING_LENGTH + 1];
	std::fill(digest, digest + sizeof(digest), '\0');

	SHA256_Data(
			static_cast<const uint8_t*>(value), size, digest);
	digest[sizeof(digest) - 1] = '\0';

	hash->append(digest);
}

void Auth::Challenge::md5Hash(const String &value, String *hash) {
	md5Hash(value.c_str(), value.size(), hash);
}

void Auth::Challenge::md5Hash(const void *value, size_t size, String *hash) {
	static_cast<void>(value);
	static_cast<void>(size);
	static_cast<void>(hash);
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION, "");
}

void Auth::Challenge::generateBcryptSalt(int32_t factor, String *salt) {
	static_cast<void>(factor);
	static_cast<void>(salt);
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION, "");
}

void Auth::Challenge::getBcryptHash(
		const char8_t *password, const char8_t *salt, String *hash) {
	static_cast<void>(password);
	static_cast<void>(salt);
	static_cast<void>(hash);
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION, "");
}

bool Auth::Challenge::compareBcryptPassword(
		const char8_t *password, const char8_t *hash) {
	static_cast<void>(password);
	static_cast<void>(hash);
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_AUTH_INTERNAL_ILLEGAL_OPERATION, "");
}

void Auth::Challenge::randomHexString(size_t bytesSize, String *str) {
	uint8_t bytes[128];
	for (size_t rest = bytesSize; rest > 0;) {
		const size_t unit = std::min(rest, sizeof(bytes));
		getRandomBytes(bytes, unit);
		bytesToHex(bytes, unit, str);
		rest -= unit;
	}
}

void Auth::Challenge::bytesToHex(const void *bytes, size_t size, String *str) {
	const char8_t digits[] = "0123456789abcdef";

	const uint8_t *it = static_cast<const uint8_t*>(bytes);
	const uint8_t *const end = it + size;
	for (; it != end; ++it) {
		*str += digits[(*it & 0xf0) >> 4];
		*str += digits[*it & 0x0f];
	}
}

void Auth::Challenge::getRandomBytes(void *bytes, size_t size) {
	UTIL_STATIC_ASSERT(sizeof(UUIDValue) == 16);

	uint8_t *it = static_cast<uint8_t*>(bytes);
	const uint8_t *const end = it + size;
	while (it != end) {
		UUIDValue base;
		UUIDUtils::generate(base);

		for (size_t i = 0; i < 2; i++) {
			const void *src = base + (i == 0 ? 0 : 12);
			const size_t unit = std::min<size_t>(end - it, 4);

			memcpy(it, src, unit);

			if ((it += unit) == end) {
				break;
			}
		}
	}
}

Auth::Mode Auth::Challenge::getDefaultMode() {
	if (challengeEnabled_) {
		return DEFAULT_MODE;
	}
	else {
		return MODE_NONE;
	}
}

Auth::Mode Auth::Challenge::getMode(util::ArrayByteInStream &in) {
	if (in.base().remaining() > 0) {
		int8_t base;
		in >> base;
		return static_cast<Mode>(base);
	}

	return MODE_NONE;
}

template<size_t Size>
void Auth::Challenge::initializeFixedStr(char8_t (&str)[Size]) {
	copyToFixedStr(str, "");
}

template<size_t Size>
void Auth::Challenge::copyToFixedStr(
		char8_t (&dest)[Size], const char8_t *src) {
	const size_t srcSize = strlen(src) + 1;
	if (srcSize > Size) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_AUTH_INTERNAL_ILLEGAL_MESSAGE, "");
	}
	memcpy(dest, src, srcSize);
}

Auth::PasswordDigest::PasswordDigest(
		const Allocator &alloc,
		const char8_t *basicSecret,
		const char8_t *cryptBase,
		const char8_t *challengeBase) :
		basicSecret_(basicSecret, alloc),
		cryptBase_(cryptBase, alloc),
		challengeBase_(challengeBase, alloc) {
}

const char8_t* Auth::PasswordDigest::getBasicSecret() const {
	return basicSecret_.c_str();
}
