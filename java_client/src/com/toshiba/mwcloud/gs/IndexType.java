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
 * {@link Container}に設定する索引の種別を示します。
 * </div><div lang="en">
 * Represents the type(s) of indexes set on a {@link Container}.
 * </div>
 */
public enum IndexType {

	/**
	 * <div lang="ja">
	 * ツリー索引を示します。
	 *
	 * <p>この索引種別は、時系列におけるロウキーと対応するカラムを除く
	 * 任意の種別のコンテナにおける、次の型のカラムに対して使用できます。</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * <li>TIMESTAMP</li>
	 * </ul>
	 * </div><div lang="en">
	 * Indicates a tree index.
	 *
	 * <p>This index type can be applied to following types of Columns of any type of
	 * {@link Container}, except the Column corresponding to the Row key of
	 * TimeSeries.</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * <li>TIMESTAMP</li>
	 * </ul>
	 * </div>
	 */
	TREE,

	/**
	 * <div lang="ja">
	 * ハッシュ索引を示します。
	 *
	 * <p>この索引種別は、{@link Collection}における次の型のカラムに対して設定できます。</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * <li>TIMESTAMP</li>
	 * </ul>
	 * <p>{@link TimeSeries}に対して設定することはできません。</p>
	 * </div><div lang="en">
	 * Indicates a hash index.
	 *
	 * <p>This index type can be set on the following types of Columns in {@link Collection}</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * <li>TIMESTAMP</li>
	 * </ul>
	 * <p>It cannot be set on Columns in {@link TimeSeries}.
	 * </p>
	 * </div>
	 */
	HASH,

	/**
	 * <div lang="ja">
	 * 空間索引を示します。
	 *
	 * <p>この索引種別は、{@link Collection}におけるGEOMETRY型のカラムに
	 * 対してのみ使用できます。{@link TimeSeries}に対して設定することは
	 * できません。</p>
	 *
	 * </div><div lang="en">
	 * Indicates a spatial index.
	 *
	 * <p>This index type can be applied to only GEOMETRY type of Columns in
	 * {@link Collection}. It cannot be set on Columns in {@link TimeSeries}.
	 * </p>
	 *
	 * </div>
	 */
	SPATIAL,

	/**
	 * <div lang="ja">
	 * デフォルトの索引種別を示します。
	 *
	 * <p>この索引種別は、特定の種別を明示せずに索引の操作を行う必要が
	 * ある場合に用いられるものであり、実在する索引はこの種別以外の種別に分類
	 * されます。</p>
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Indicates the default index type.
	 *
	 * <p>This index type is used when it is necessary to manipulate the index
	 * without specifying a type, and the existing index is classified as another type</p>
	 *
	 * @since 3.5
	 * </div>
	 */
	DEFAULT

}
