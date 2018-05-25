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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Represents the information about the schema of a Column.
 *
 * <p>It does not guarantee the validity of values e.g. the Column name and
 * the type of index for the type of a Column.</p>
 */
public class ColumnInfo {

	private final String name;

	private final GSType type;

	private final Set<IndexType> indexTypes;

	/**
	 * Creates the information with the specified Column name and type of Column.
	 *
	 * @param name Column name. {@code null} indicates no specification of Column name.
	 * @param type Column type. {@code null} indicates no specification of Column type.
	 */
	public ColumnInfo(String name, GSType type) {
		this(name, type, null);
	}

	/**
	 * Creates the information with the specified Column name and type of Column
	 * and set of Index type.
	 *
	 * <p>Creates a copy of a specified non-empty set of Index type copied.</p>
	 *
	 * @param name Column name. {@code null} indicates no specification of Column name.
	 * @param type Column type. {@code null} indicates no specification of Column type.
	 * @param indexTypes Set of index type. {@code null} indicates no specification.
	 * No index type is set if it is an empty set.
	 */
	public ColumnInfo(String name, GSType type, Set<IndexType> indexTypes) {
		this.name = name;
		this.type = type;

		if (indexTypes == null) {
			this.indexTypes = null;
		}
		else if (indexTypes.isEmpty()) {
			this.indexTypes = Collections.emptySet();
		}
		else {
			this.indexTypes =
					Collections.unmodifiableSet(EnumSet.copyOf(indexTypes));
		}
	}

	/**
	 * Returns a name of Column.
	 *
	 * @return Name of Column, or {@code null} if unspecified.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the type of a Column, i.e., the type of each field value
	 * corresponding to a Column.
	 *
	 * @return Type of Column, or {@code null} if unspecified.
	 */
	public GSType getType() {
		return type;
	}

	/**
	 * Returns all of set of index type.
	 *
	 * <p>{@link UnsupportedOperationException} can occur when value of the
	 * returned object is updated. And value of the returned object is not changed by
	 * updating this object.</p>
	 *
	 * @return Set of index type, or {@code null} if unspecified.
	 */
	public Set<IndexType> getIndexTypes() {
		return indexTypes;
	}

}
