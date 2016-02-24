/*
   Copyright (c) 2012 TOSHIBA CORPORATION.

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

import java.util.List;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;

public class ExtendedContainerInfo extends ContainerInfo {

	private ContainerAttribute attribute;

	public ExtendedContainerInfo(String name, ContainerType type,
			List<ColumnInfo> columnInfoList, boolean rowKeyAssigned,
			ContainerAttribute attribute) {
		super(name, type, columnInfoList, rowKeyAssigned);
		this.attribute = attribute;
	}

	public ExtendedContainerInfo() {
	}

	public ContainerAttribute getAttribute() {
		return attribute;
	}

	public void setAttribute(ContainerAttribute attribute) {
		this.attribute = attribute;
	}
}
