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
package com.toshiba.mwcloud.gs;

import java.io.Closeable;


/**
 * Manages a set of Rows obtained by executing a query.
 *
 * <p>It has a function of per-Row and per-Row-field manipulation and holds a cursor state similar to
 * {@link java.sql.ResultSet} to specify a target Row. The cursor is initially located just before the head of
 * a Row set.</p>
 */
public interface RowSet<R> extends Closeable {

	/**
	 * Returns TRUE if a Row set has at least one Row ahead of the current cursor position.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 */
	public boolean hasNext() throws GSException;

	/**
	 * Moves the cursor to the next Row in a Row set and returns the Row object at the moved position.
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException if a connection failure caused an error in creation of a Row object
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 */
	public R next() throws GSException;

	/**
	 * Deletes the Row at the current cursor position.
	 *
	 * <p>It can be used only for {@link RowSet} obtained with locking enabled. Like {@link Container#remove(Object)},
	 * further limitations are placed depending on the type and settings of a Container.
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException If called on {@link RowSet} obtained without locking enabled.
	 * @throws GSException if its operation is contrary to the restrictions specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or its related transaction, the relevant Container
	 * is deleted, its schema is changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 */
	void remove() throws GSException;

	/**
	 * Updates the values except a Row key of the Row at the cursor position, using the specified Row object.
	 *
	 * <p>{@code null} cannot be specified. The Row key contained in the specified Row object is ignored.</p>
	 *
	 * <p>It can be used only for{@link RowSet} obtained with locking enabled.
	 * Like {@link Container#put(Object, Object)}, further limitations are placed depending on the type and settings of
	 * a Container.
	 *
	 * @throws GSException if there is no Row at the cursor position.
	 * @throws GSException If called on {@link RowSet} obtained without locking enabled.
	 * @throws GSException if its operation is contrary to the restrictions specific to a particular Container.
	 * @throws GSException if a timeout occurs during this operation or its related transaction, the relevant Container
	 * is deleted, its schema is changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws ClassCastException if the specified Row object does not match the value types of the Row object used in
	 * mapping operation.
	 * @throws NullPointerException If a {@code null} parameter is specified;
	 * or if no object exists in the Row object which corresponds to the Row field.
	 */
	public void update(R rowObj) throws GSException;

	/**
	 * Returns the size, namely the number of Rows at the time of creating a Row set.
	 */
	public int size();

	/**
	 * Releases related resources as necessary.
	 *
	 * @throws GSException It will not be thrown in the current version.
	 *
	 * @see Closeable#close()
	 */
	public void close() throws GSException;

}
