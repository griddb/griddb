/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs.experimental;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.toshiba.mwcloud.gs.GSException;

public class DatabaseInfo {

	private String name = null;
	private Map<String, PrivilegeInfo> privilegeInfoMap = null;

	public DatabaseInfo() throws GSException {
		privilegeInfoMap = new HashMap<String, PrivilegeInfo>();
	}

	public DatabaseInfo(String name, Map<String, PrivilegeInfo> privilegeInfoMap) {
		this.name = name;
		this.privilegeInfoMap = privilegeInfoMap;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, PrivilegeInfo> getPrivileges() {
		if (privilegeInfoMap != null) {
			return Collections.unmodifiableMap(privilegeInfoMap);
		}
		return null;
	}
}
