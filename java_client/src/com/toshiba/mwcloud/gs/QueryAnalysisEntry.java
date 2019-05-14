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
 * クエリプランならびにクエリ処理解析結果を構成する一連の情報の一つを示します。
 *
 * <p>TQLのEXPLAIN文ならびEXPLAIN ANALYZE文の実行結果を保持するために
 * 使用します。1つの実行結果は、このエントリの列により表現されます。</p>
 * </div><div lang="en">
 * Represents one of information entries composing a query plan and the results
 * of analyzing a query operation.
 *
 * <p>It is used to hold the result of executing an EXPLAIN statement or
 * an EXPLAIN ANALYZE statement in TQL. One execution result is represented
 * by an array of entries. </p>
 * </div>
 */
public class QueryAnalysisEntry {

	private int id;

	private int depth;

	private String type;

	private String valueType;

	private String value;

	private String statement;

	/**
	 * <div lang="ja">
	 * 一連のエントリ列における、このエントリの位置を示すIDを取得します。
	 *
	 * <p>TQLを実行した結果を受け取る場合、IDは{@code 1}から順に割り当て
	 * られます。</p>
	 * </div><div lang="en">
	 * Returns the ID indicating the location of an entry in an array of entries.
	 *
	 * <p>In a result set of executing a TQL query, IDs are assigned serially
	 * starting from {@code 1}. </p>
	 * </div>
	 */
	@RowField(name="ID", columnNumber=0)
	public int getId() {
		return id;
	}

	/**
	 * <div lang="ja">
	 * 一連のエントリ列における、このエントリの位置を示すIDを設定します。
	 *
	 * @see #getId()
	 * </div><div lang="en">
	 * Sets the ID indicating the location of an entry in an array of entries.
	 *
	 * @see #getId()
	 * </div>
	 */
	@RowField(name="ID", columnNumber=0)
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * <div lang="ja">
	 * 他のエントリとの関係を表すための、深さを取得します。
	 *
	 * <p>このエントリより小さな値のIDを順にたどり、このエントリの深さより1だけ
	 * 浅いエントリが存在した場合、このエントリは、該当するエントリの内容を
	 * より詳しく説明するためのものであることを意味します。</p>
	 * </div><div lang="en">
	 * Returns the depth indicating the relation to other entries.
	 *
	 * <p>If there is found an entry whose depth is smaller than that of a target
	 * entry by one, through checking entries one by one whose IDs are
	 * smaller than that of the target entry, it means that the target entry
	 * describes the content of the found entry in more detail. </p>
	 * </div>
	 */
	@RowField(name="DEPTH", columnNumber=1)
	public int getDepth() {
		return depth;
	}

	/**
	 * <div lang="ja">
	 * 他のエントリとの関係を表すための、深さを設定します。
	 *
	 * @see #getDepth()
	 * </div><div lang="en">
	 * Sets the depth indicating the relation to other entries.
	 *
	 * @see #getDepth()
	 * </div>
	 */
	@RowField(name="DEPTH", columnNumber=1)
	public void setDepth(int depth) {
		this.depth = depth;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報の種別を取得します。
	 *
	 * <p>実行時間といった解析結果の種別、クエリプランの構成要素の種別などを
	 * 表します。</p>
	 * </div><div lang="en">
	 * Returns the type of the information indicated by an entry.
	 *
	 * <p>A returned value indicates the type of an analysis result
	 * (e.g., execution time), the type of a component of a query plan, etc. </p>
	 * </div>
	 */
	@RowField(name="TYPE", columnNumber=2)
	public String getType() {
		return type;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報の種別を設定します。
	 *
	 * @param type 種別または{@code null}
	 *
	 * @see #getType()
	 * </div><div lang="en">
	 * Sets the type of the information indicated by an entry.
	 *
	 * @param type type or {@code null}
	 *
	 * @see #getType()
	 * </div>
	 */
	@RowField(name="TYPE", columnNumber=2)
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応付けられた値の型を取得します。
	 *
	 * <p>実行時間といった解析結果などに対応する値の型を取得します。
	 * 型の名称は、TQLで定義された基本型のうち次のものです。</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * </ul>
	 *
	 * <p>値が対応づけられていない場合は空文字列を取得します。</p>
	 * </div><div lang="en">
	 * Returns the value type of the information indicated by an entry.
	 *
	 * <p>It returns the value type of an analysis result (e.g., execution time) etc.
	 * The following types (primitive types defined by TQL) are supported:</p>
	 * <ul>
	 * <li>STRING</li>
	 * <li>BOOL</li>
	 * <li>BYTE</li>
	 * <li>SHORT</li>
	 * <li>INTEGER</li>
	 * <li>LONG</li>
	 * <li>FLOAT</li>
	 * <li>DOUBLE</li>
	 * </ul>
	 *
	 * <p>It returns an empty string if no value is assigned. </p>
	 * </div>
	 */
	@RowField(name="VALUE_TYPE", columnNumber=3)
	public String getValueType() {
		return valueType;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応付けられた値の型を設定します。
	 *
	 * @param valueType 値の型または{@code null}
	 *
	 * @see #getValueType()
	 * </div><div lang="en">
	 * Sets the value type of the information indicated by an entry.
	 *
	 * @param valueType value type or {@code null}
	 *
	 * @see #getValueType()
	 * </div>
	 */
	@RowField(name="VALUE_TYPE", columnNumber=3)
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応付けられた値の文字列表現を取得します。
	 *
	 * <p>値が対応づけられていない場合は空文字列を取得します。</p>
	 *
	 * @see #getValueType()
	 * </div><div lang="en">
	 * Returns a character string representing the value of the information
	 * indicated by an entry.
	 *
	 * <p>It returns an empty string if no value is assigned. </p>
	 *
	 * @see #getValueType()
	 * </div>
	 */
	@RowField(name="VALUE", columnNumber=4)
	public String getValue() {
		return value;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応付けられた値の文字列表現を設定します。
	 *
	 * @param value 値の文字列表現または{@code null}
	 *
	 * @see #getValue()
	 * </div><div lang="en">
	 * Sets a character string representing the value of the information
	 * indicated by an entry.
	 *
	 * @param value A string representation of the value or {@code null}
	 *
	 * @see #getValue()
	 * </div>
	 */
	@RowField(name="VALUE", columnNumber=4)
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応するTQL文の一部を取得します。
	 *
	 * <p>対応関係を持たない場合は空文字列を取得します。</p>
	 * </div><div lang="en">
	 * Returns a part of a TQL statement corresponding to the information
	 * indicated by an entry.
	 *
	 * <p>Returns an empty string if no correspondence is found. </p>
	 * </div>
	 */
	@RowField(name="STATEMENT", columnNumber=5)
	public String getStatement() {
		return statement;
	}

	/**
	 * <div lang="ja">
	 * このエントリが示す情報に対応するTQL文の一部を設定します。
	 *
	 * @param statement TQL文の一部または{@code null}
	 *
	 * @see #getStatement()
	 * </div><div lang="en">
	 * Sets a part of a TQL statement corresponding to the information
	 * indicated by an entry.
	 *
	 * @param statement Part of the TQL statement or {@code null}
	 *
	 * @see #getStatement()
	 * </div>
	 */
	@RowField(name="STATEMENT", columnNumber=5)
	public void setStatement(String statement) {
		this.statement = statement;
	}

	/**
	 * <div lang="ja">
	 * このオブジェクトのハッシュコード値を返します。
	 *
	 * <p>このメソッドは、{@link Object#hashCode()}にて定義されている汎用規約に
	 * 準拠します。したがって、等価なオブジェクトのハッシュコード値は等価です。</p>
	 *
	 * @return このオブジェクトのハッシュコード値
	 *
	 * @see #equals(Object)
	 * </div><div lang="en">
	 * Returns hash code of this object.
	 *
	 * <p>This method maintain the general contract for the {@link Object#hashCode()} method,
	 * which states that equal objects must have equal hash codes.</p>
	 *
	 * @return hash code of this object
	 *
	 * @see #equals(Object)
	 * </div>
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + depth;
		result = prime * result + id;
		result = prime * result
				+ ((statement == null) ? 0 : statement.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		result = prime * result
				+ ((valueType == null) ? 0 : valueType.hashCode());
		return result;
	}

	/**
	 * <div lang="ja">
	 * このオブジェクトと他のオブジェクトが等しいかどうかを示します。
	 *
	 * <p>{@link #getId()}や{@link #getDepth()}などにより得られる
	 * 各値がすべて等価である場合のみ、指定のオブジェクトと等価であると
	 * みなされます。</p>
	 *
	 * <p>このメソッドは、{@link Object#hashCode()}にて定義されている汎用規約に
	 * 準拠します。したがって、等価なオブジェクトは等価なハッシュコードを保持します。</p>
	 *
	 * @param obj 比較対象の参照オブジェクトまたは{@code null}
	 *
	 * @return このオブジェクトが{@code obj}引数と同じである場合は{@code true}、
	 * それ以外の場合は{@code false}
	 *
	 * @see #hashCode()
	 * </div><div lang="en">
	 * Indicates whether some other object is "equal to" this one.
	 *
	 * <p>Returns true if each values (e.g. value returned by {@link #getId()}
	 * or {@link #getDepth()}) of this object and the specified object are
	 * equivalent.</p>
	 *
	 * <p>This method maintain the general contract for the {@link Object#hashCode()} method,
	 * which states that equal objects must have equal hash codes.</p>
	 *
	 * @param obj the reference object with which to compare or {@code null}
	 *
	 * @return {@code true} if this object is the same as the {@code obj} argument;
	 * {@code false} otherwise.
	 *
	 * @see #hashCode()
	 * </div>
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryAnalysisEntry other = (QueryAnalysisEntry) obj;
		if (depth != other.depth)
			return false;
		if (id != other.id)
			return false;
		if (statement == null) {
			if (other.statement != null)
				return false;
		} else if (!statement.equals(other.statement))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		if (valueType == null) {
			if (other.valueType != null)
				return false;
		} else if (!valueType.equals(other.valueType))
			return false;
		return true;
	}

}
