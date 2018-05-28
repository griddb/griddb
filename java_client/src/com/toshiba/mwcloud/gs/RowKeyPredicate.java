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
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * Represents the condition that a {@link RowKey} satisfies.
 *
 * <p>This is used as the search condition in {@link GridStore#multiGet(java.util.Map)}</p>
 *
 * <p>There are two types of conditions, range condition and individual condition.
 * The two types of conditions cannot be specified at the same time.
 * If the condition is not specified, it means that the condition is satisfied in all the target row keys.</p>
 *
 * @param <K> type of {@link RowKey}
 *
 */
public class RowKeyPredicate<K> {

	private static boolean stringRangeRestricted = false;

	private final Class<?> keyClass;

	private K start;

	private K finish;

	private Set<K> distinctKeys;

	private RowKeyPredicate(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	/**
	 * Creates an instance of {@link RowKeyPredicate} with the specified {@link GSType} as the {@link RowKey} type.
	 *
	 * <p>The target Container must have a {@link RowKey}, and the type of the {@link RowKey} must be
	 * the specified {@link GSType}</p>
	 *
	 * <p>Unlike {@link #create(Class)}, this method is used when the type of {@link RowKey} is not specified
	 * when the application is compiled. However, the criteria for checking the RowKey type when setting
	 * the condition is the same as {@link #create(Class)}.</p>
	 *
	 * <p>The type of {@link RowKey} that can be set is only that allowed
	 * by either one of the subinterfaces of {@link Container}.</p>
	 *
	 * @param keyType type of {@link RowKey} used as a search condition
	 *
	 * @return {@link RowKeyPredicate} newly created
	 *
	 * @throws GSException if the specified type is not always supported as a {@link RowKey}
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 *
	 * @see Container
	 */
	public static RowKeyPredicate<Object> create(
			GSType keyType) throws GSException {
		try {
			switch (keyType) {
			case STRING:
				return new RowKeyPredicate<Object>(String.class);
			case INTEGER:
				return new RowKeyPredicate<Object>(Integer.class);
			case LONG:
				return new RowKeyPredicate<Object>(Long.class);
			case TIMESTAMP:
				return new RowKeyPredicate<Object>(Date.class);
			default:
				throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
						"Unsupported key type (type=" + keyType + ")");
			}
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(keyType, "keyType", e);
		}
	}

	/**
	 * Creates an instance of {@link RowKeyPredicate} with the {@link GSType} corresponding to
	 * the specified {@link Class} as the {@link RowKey} type.
	 *
	 * <p>The target Container must have a {@link RowKey}, and the type of the {@link RowKey} must be
	 * the specified {@link GSType}</p>
	 *
	 * <p>The type of {@link RowKey} that can be set is only that allowed
	 * by either one of the subinterfaces of {@link Container}.
	 * For the correspondence of {@link Class} to {@link GSType},
	 * see the definition of {@link Container}.</p>
	 *
	 * @param keyType {@link Class} corresponding to a {@link RowKey} used as a search condition
	 *
	 * @return {@link RowKeyPredicate} newly created
	 *
	 * @throws GSException if the specified type is not always supported as a {@link RowKey}
	 * @throws NullPointerException if {@code null} is specified in the argument.
	 *
	 * @see Container
	 */
	public static <K> RowKeyPredicate<K> create(
			Class<K> keyType) throws GSException {
		if (keyType != String.class &&
				keyType != Integer.class &&
				keyType != Long.class &&
				keyType != Date.class) {

			if (keyType == null) {
				throw GSErrorCode.checkNullParameter(
						keyType, "keyType", null);
			}

			throw new GSException(GSErrorCode.UNSUPPORTED_KEY_TYPE,
					"Unsupported key type (type=" + keyType + ")");
		}

		return new RowKeyPredicate<K>(keyType);
	}

	/**
	 * Sets the value of the {@link RowKey} at the starting positionof the range condition.
	 *
	 * <p>A {@link RowKey} with a value smaller than the specified value is deemed as non-conforming.</p>
	 *
	 * <p>A type with an undefined magnitude relationship can be set as a condition
	 * but cannot be used in the actual judgment, e.g. STRING type</p>
	 *
	 * @param startKey value of {@link RowKey} at the starting position or {@code null}.
	 * For {@code null}, the setting is cancelled.
	 *
	 * @throws GSException if an individual condition had been set already
	 * @throws ClassCastException the specified RowKey is not NULL or the type is not supported as {@link RowKey}
	 */
	public void setStart(K startKey) throws GSException {
		if (distinctKeys != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Distinct key has already been specified");
		}

		if (keyClass == String.class && startKey != null &&
				stringRangeRestricted) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"String range is not supported");
		}

		keyClass.cast(startKey);
		start = startKey;
	}

	/**
	 * Sets the value of the {@link RowKey} at the last position of the range condition.
	 *
	 * <p>A {@link RowKey} with a value larger than the specified value is deemed as non-conforming.</p>
	 *
	 * <p>A type with an undefined magnitude relationship can be set as a condition
	 * but cannot be used in the actual judgment e.g. STRING type</p>
	 *
	 * @param finishKey the value of {@link RowKey} at the last position or {@code null}.
	 * For {@code null}, the setting is cancelled.
	 *
	 * @throws GSException if an individual condition had been set already
	 * @throws ClassCastException the value of specified key is not NULL
	 * or the type is not supported as {@link RowKey}
	 */
	public void setFinish(K finishKey) throws GSException {
		if (distinctKeys != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Distinct key has already been specified");
		}

		if (keyClass == String.class && finishKey != null &&
				stringRangeRestricted) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"String range is not supported");
		}

		keyClass.cast(finishKey);
		finish = finishKey;
	}

	/**
	 * Appends the value of the {@link RowKey} as one of the elements of the individual condition.
	 *
	 * <p>A {@link RowKey} with the same value as the added value is deemed as conforming.</p>
	 *
	 * @param key value of {@link RowKey} to be appended as one of the elements
	 * of the individual condition. Must not be a {@code null} value.
	 *
	 * @throws GSException if a range condition had already been set
	 * @throws ClassCastException the value of the specified key is not NULL or the type is not supported as {@link RowKey}
	 * @throws NullPointerException when {@code null} is specified as an argument
	 */
	public void add(K key) throws GSException {
		if (start != null || finish != null) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Start or finish key has already been specified");
		}

		keyClass.cast(key);

		if (key == null) {
			throw GSErrorCode.checkNullParameter(key, "key", null);
		}

		if (distinctKeys == null) {
			distinctKeys = new HashSet<K>();
		}

		distinctKeys.add(key);
	}

	/**
	 * Returns the type of {@link RowKey} used as a search condition.
	 *
	 * @return the type of {@link RowKey} used as a search condition.
	 */
	public GSType getKeyType() {
		if (keyClass == String.class) {
			return GSType.STRING;
		}
		else if (keyClass == Integer.class) {
			return GSType.INTEGER;
		}
		else if (keyClass == Long.class) {
			return GSType.LONG;
		}
		else if (keyClass == Date.class) {
			return GSType.TIMESTAMP;
		}
		else {
			throw new Error("Internal error by unknown key class (class=" +
					keyClass + ")");
		}
	}

	/**
	 * Returns the value of the {@link RowKey} at the starting position of the range condition.
	 *
	 * @return the value of {@link RowKey} at the starting position
	 * of the range condition, or {@code null} if it is not set.
	 */
	public K getStart() {
		return start;
	}

	/**
	 * Returns the value of {@link RowKey} at the last position
	 * of the range condition.
	 *
	 * @return the value of {@link RowKey} at the last position
	 * of the range condition, or {@code null} if it is not set.
	 */
	public K getFinish() {
		return finish;
	}

	/**
	 * Returns a Collection containing all of the values of the row keys
	 * that make up the individual condition.
	 *
	 * <p>It is not defined whether an exception like {@link UnsupportedOperationException}
	 * will occur during execution, when a returned object is updated.
	 * Moreover, after an object is returned, it is not defined
	 * whether an update of this object will change the contents of the returned object.</p>
	 *
	 * @return {@link java.util.Collection} containing all of the values of the row keys
	 * that make up the individual condition.
	 */
	public java.util.Collection<K> getDistinctKeys() {
		if (distinctKeys == null) {
			return null;
		}

		return Collections.unmodifiableCollection(distinctKeys);
	}

}
