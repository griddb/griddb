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
 * Represents one of information entries composing a query plan and the results
 * of analyzing a query operation.
 *
 * <p>It is used to hold the result of executing an EXPLAIN statement or
 * an EXPLAIN ANALYZE statement in TQL. One execution result is represented
 * by an array of entries. </p>
 */
public class QueryAnalysisEntry {

	private int id;

	private int depth;

	private String type;

	private String valueType;

	private String value;

	private String statement;

	/**
	 * Returns the ID indicating the location of an entry in an array of entries.
	 *
	 * <p>In a result set of executing a TQL query, IDs are assigned serially
	 * starting from {@code 1}. </p>
	 */
	@RowField(name="ID", columnNumber=0)
	public int getId() {
		return id;
	}

	/**
	 * Sets the ID indicating the location of an entry in an array of entries.
	 *
	 * @see #getId()
	 */
	@RowField(name="ID", columnNumber=0)
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * Returns the depth indicating the relation to other entries.
	 *
	 * <p>If there is found an entry whose depth is smaller than that of a target
	 * entry by one, through checking entries one by one whose IDs are
	 * smaller than that of the target entry, it means that the target entry
	 * describes the content of the found entry in more detail. </p>
	 */
	@RowField(name="DEPTH", columnNumber=1)
	public int getDepth() {
		return depth;
	}

	/**
	 * Sets the depth indicating the relation to other entries.
	 *
	 * @see #getDepth()
	 */
	@RowField(name="DEPTH", columnNumber=1)
	public void setDepth(int depth) {
		this.depth = depth;
	}

	/**
	 * Returns the type of the information indicated by an entry.
	 *
	 * <p>A returned value indicates the type of an analysis result
	 * (e.g., execution time), the type of a component of a query plan, etc. </p>
	 */
	@RowField(name="TYPE", columnNumber=2)
	public String getType() {
		return type;
	}

	/**
	 * Sets the type of the information indicated by an entry.
	 *
	 * @see #getType()
	 */
	@RowField(name="TYPE", columnNumber=2)
	public void setType(String type) {
		this.type = type;
	}

	/**
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
	 */
	@RowField(name="VALUE_TYPE", columnNumber=3)
	public String getValueType() {
		return valueType;
	}

	/**
	 * Sets the value type of the information indicated by an entry.
	 *
	 * @see #getValueType()
	 */
	@RowField(name="VALUE_TYPE", columnNumber=3)
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	/**
	 * Returns a character string representing the value of the information
	 * indicated by an entry.
	 *
	 * <p>It returns an empty string if no value is assigned. </p>
	 *
	 * @see #getValueType()
	 */
	@RowField(name="VALUE", columnNumber=4)
	public String getValue() {
		return value;
	}

	/**
	 * Sets a character string representing the value of the information
	 * indicated by an entry.
	 *
	 * @see #getValue()
	 */
	@RowField(name="VALUE", columnNumber=4)
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Returns a part of a TQL statement corresponding to the information
	 * indicated by an entry.
	 *
	 * <p>Returns an empty string if no correspondence is found. </p>
	 */
	@RowField(name="STATEMENT", columnNumber=5)
	public String getStatement() {
		return statement;
	}

	/**
	 * Sets a part of a TQL statement corresponding to the information
	 * indicated by an entry.
	 *
	 * @see #getStatement()
	 */
	@RowField(name="STATEMENT", columnNumber=5)
	public void setStatement(String statement) {
		this.statement = statement;
	}

	/**
	 * Returns hash code of this object.
	 *
	 * <p>This method maintain the general contract for the {@link Object#hashCode()} method,
	 * which states that equal objects must have equal hash codes.</p>
	 *
	 * @return hash code of this object
	 *
	 * @see #equals(Object)
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
	 * Indicates whether some other object is "equal to" this one.
	 *
	 * <p>Returns true if each values (e.g. valuse returned by {@link #getId()}
	 * or {@link #getDepth()}) of this object and the specified object are
	 * equivalent.</p>
	 *
	 * <p>This method maintain the general contract for the {@link Object#hashCode()} method,
	 * which states that equal objects must have equal hash codes.</p>
	 *
	 * @param obj the reference object with which to compare.
	 *
	 * @return {@code true} if this object is the same as the {@code obj} argument;
	 * {@code false} otherwise.
	 *
	 * @see #hashCode()
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
