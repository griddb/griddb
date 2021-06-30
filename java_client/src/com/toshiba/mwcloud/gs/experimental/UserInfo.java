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


public class UserInfo {

	private String name = null;
	private String password = null;
	private String hashPassword = null;
	private boolean isSuperUser = false;
	private boolean isRole = false;
	private boolean isGroupMapping = false;
	private String roleName = null;

	public UserInfo() {
	}

	public UserInfo(String name, String hashPassword, boolean isSuperUser) {
		this.name = name;
		this.password = null;
		this.hashPassword = hashPassword;
		this.isSuperUser = isSuperUser;
	}
	public UserInfo(String name, String hashPassword, boolean isSuperUser, boolean isGroupMapping, String roleName) {
		this.name = name;
		this.password = null;
		this.hashPassword = hashPassword;
		this.isSuperUser = isSuperUser;
		this.isGroupMapping = isGroupMapping;
		this.roleName = roleName;
		if (roleName.length() > 0) {
			isRole = true;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHashPassword() {
		return hashPassword;
	}

	public void setHashPassword(String hashPassword) {
		this.hashPassword = hashPassword;
	}

	public boolean isSuperUser() {
		return isSuperUser;
	}
	
	public boolean isRole() {
		return isRole;
	}

	public boolean isGroupMapping() {
		return isGroupMapping;
	}

	public String getRoleName() {
		return roleName;
	}
}
