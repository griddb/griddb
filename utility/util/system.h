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
    @brief Definition of Utility of system functions
*/
#ifndef UTIL_SYSTEM_H_
#define UTIL_SYSTEM_H_

#include "util/type.h"
#include <string>
#include <list>

namespace util {

/*!
    @brief Status of using memory.
*/



class MemoryStatus {
public:

	static MemoryStatus getStatus();

	size_t getPeakUsage() const;
	size_t getLastUsage() const;

private:
	MemoryStatus();
	size_t peakUsage_;
	size_t lastUsage_;
};

class ProcessUtils {
public:
	static uint64_t getCurrentProcessId();

private:
	ProcessUtils();
};

#if UTIL_MINOR_MODULE_ENABLED


class SharedObject {
public:
	
	
	
	
	void open(const char8_t *path, int type);

	
	void close(void);

	
	
	
	void* getSymbol(const char8_t *symbol);

public:
	SharedObject();
	virtual ~SharedObject();

private:
	SharedObject(const SharedObject&);
	SharedObject& operator=(const SharedObject&);

private:
	void *data_;
};


class System {
public:
	
	
	
	static std::string& getCurrentDirectory(std::string &out);

	
	
	
	
	static char* getCurrentDirectory(char *buf, size_t blen);










	
	
	
	
	
	static void getLibraryVersion(int *major, int *minor, int *patch, int *rel);

	
	
	static size_t getCPUCount(void);

	
	
	static size_t getOnlineCPUCount(void);

private:
	System();
	System(const System&);
	inline System& operator=(const System&);
};

class Group;


class User {
public:
	
	
	
	void getUser(uid_t uid);

	
	
	
	void getUser(const char *id);

	
	
	inline const char* getID(void) const {
		return id_.c_str();
	}

	
	
	inline const char* getPassword(void) const {
		return password_.c_str();
	}

	
	
	inline uid_t getUID(void) const {
		return uid_;
	}

	
	
	inline gid_t getGID(void) const {
		return gid_;
	}

	
	
	inline const char* getName(void) const {
		return name_.c_str();
	}

	
	
	inline const char* getHomeDirectory(void) const {
		return homedir_.c_str();
	}

	
	
	inline const char* getShell(void) const {
		return shell_.c_str();
	}

public:
	User();
	virtual ~User();

private:
	std::string id_;
	std::string password_;
	uid_t uid_;
	gid_t gid_;
	std::string name_;
	std::string homedir_;
	std::string shell_;
};


class Group {
public:
	
	typedef std::list<std::string> user_cont;

	
	typedef user_cont::iterator user_itr;

	
	typedef user_cont::const_iterator user_citr;

public:
	
	
	
	void getGroup(gid_t gid);

	
	
	
	void getGroup(const char *id);

	
	
	inline const char* getID(void) const {
		return id_.c_str();
	}

	
	
	inline const char* getPassword(void) const {
		return password_.c_str();
	}

	
	
	inline uid_t getGID(void) const {
		return gid_;
	}

	
	
	inline user_cont& getUsers(void) {
		return users_;
	}

	
	
	inline const user_cont& getUsers(void) const {
		return users_;
	}

public:
	Group();
	virtual ~Group();

private:
	std::string id_;
	std::string password_;
	gid_t gid_;
	user_cont users_;
};

#endif 

} 

#endif
