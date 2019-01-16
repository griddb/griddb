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
package com.toshiba.mwcloud.gs.stub;

import java.util.Properties;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.common.GridStoreFactoryProvider;

public class StubFactoryProvider extends GridStoreFactoryProvider {

	@Override
	public GridStoreFactory getFactory() {
		return new GridStoreFactory() {

			@Override
			public void setProperties(Properties properties) throws GSException {
				throw new Error("This is a stub version. Use the released version");
			}

			@Override
			public GridStore getGridStore(Properties properties) throws GSException {
				throw new Error("This is a stub version. Use the released version");
			}

			@Override
			public void close() throws GSException {
				throw new Error("This is a stub version. Use the released version");
			}
		};
	}

}
