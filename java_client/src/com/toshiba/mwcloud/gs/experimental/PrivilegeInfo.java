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

public class PrivilegeInfo {

	public enum RoleType {
		ALL,
		READ
	}

	RoleType role = RoleType.READ;

	public enum PrivilegeType {
		ALL,
		READ
	}
	
	PrivilegeType priv = PrivilegeType.READ;
	
	public PrivilegeInfo() {
	}

	public PrivilegeInfo(RoleType role) {
		if (role == RoleType.READ) {
			this.priv = PrivilegeType.READ;
		} else {
			this.priv = PrivilegeType.ALL;
		}
	}

	public PrivilegeInfo(PrivilegeType priv) {
		this.priv = priv;
	}

	public RoleType getRole() {
		if (priv == PrivilegeType.READ) {
			return RoleType.READ;
		} else {
			return RoleType.ALL;
		}
	}

	public void setRole(RoleType role) {
		if (role == RoleType.READ) {
			this.priv = PrivilegeType.READ;
		} else {
			this.priv = PrivilegeType.ALL;
		}
	}

	public PrivilegeType get() {
		return priv;
	}

	public void set(PrivilegeType priv) {
		this.priv = priv;
	}
}
