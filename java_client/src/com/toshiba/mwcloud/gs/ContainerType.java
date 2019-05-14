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
package com.toshiba.mwcloud.gs;

/**
 * <div lang="ja">
 * コンテナの種別を表します。
 * </div><div lang="en">
 * Represents the type(s) of a Container.
 * </div>
 */
public enum ContainerType {

	/**
	 * <div lang="ja">
	 * 対象のコンテナがコレクションであることを示します。
	 * </div><div lang="en">
	 * Indicates the target Container is a Collection.
	 * </div>
	 */
	COLLECTION,

	/**
	 * <div lang="ja">
	 * 対象のコンテナが時系列であることを示します。
	 * </div><div lang="en">
	 * Indicates the target Container is a TimeSeries.
	 * </div>
	 */
	TIME_SERIES
}
