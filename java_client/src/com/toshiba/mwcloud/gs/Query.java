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
 * Provides the functions of holding the information about a query related
 * to a specific {@link Container},
 * specifying the options for fetching and retrieving the result.
 *
 * @param <R> the type of {@link RowSet} elements to be obtained
 */
public interface Query<R> extends Closeable {

	/**
	 * Sets an option for a result set.
	 *
	 * <p>See {@link FetchOption} for the definitions of supported option items and values. </p>
	 *
	 * @param option an option item
	 * @param value an option value
	 *
	 * @throws GSException if the specified option value is invalid for the specified option item.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws NullPointerException if a {@code null} parameter(s) is specified.
	 *
	 * @see FetchOption
	 */
	public void setFetchOption(
			FetchOption option, Object value) throws GSException;

	/**
	 * Executes a query and returns a set of Rows as an execution result.
	 *
	 * <p>It behaves in the same way as {@link #fetch(boolean)} is called
	 * without requesting a lock for update. </p>
	 *
	 * @throws GSException if the target query contains any wrong parameter, syntax,
	 * or directive. For example, in the case of TQL, if the type of a specified
	 * Column does not match the parameter of a function. For detailed restrictions,
	 * see the descriptions of the functions to create a query.
	 * @throws GSException if the requested type of {@link RowSet} elements does not match
	 * the type of the result set in executing a TQL query.
	 * @throws GSException if a timeout occurs during this operation or
	 * its related transaction, the relevant Container is deleted, its schema is
	 * changed or a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container} is closed.
	 * @throws NullPointerException if an unsupported {@code null} parameter is
	 * specified when creating this query. It will not be thrown as a result
	 * of evaluating a TQL statement in GridDB.
	 *
	 * @see #fetch(boolean)
	 */
	public RowSet<R> fetch() throws GSException;

	/**
	 * Executes a query with the specified option and returns a set of Rows as an execution result.
	 *
	 * <p>It locks all target Rows if {@code true} is specified
	 * as {@code forUpdate}. If the target Rows are locked, update operations
	 * on the Rows by any other transactions are blocked while a relevant
	 * transaction is active. TRUE can be specified only if the autocommit mode
	 * is disabled on a relevant Container. </p>
	 *
	 * <p>If it is called to obtain new Rows, {@link RowSet} of the last result
	 * of this query will be closed.</p>
	 *
	 * <p>It will be failed if too much Rows are obtained, because of the data buffer
	 * size upper limit of GridDB node. See "System limiting values" in the GridDB API Reference about
	 * the buffer size upper limit.</p>
	 *
	 * @throws GSException if {@code true} is specified as {@code forUpdate} although the autocommit
	 * mode is enabled on a relevant Container.
	 * @throws GSException if {@code true} is specified as {@code forUpdate} for a query which
	 * cannot acquire a lock. For the availability of a lock, see the descriptions
	 * of the functions to create a query.
	 * @throws GSException if the target query contains any wrong parameter, syntax,
	 * or directive. For example, in the case of TQL, if the type of a specified
	 * Column does not match the parameter of a function. For detailed restrictions,
	 * see the descriptions of the functions to create a query.
	 * @throws GSException if the requested type of {@link RowSet} elements
	 * does not match the type of the result set in executing a TQL query.
	 * @throws GSException if a timeout occurs during this operation or its related
	 * transaction, the relevant Container is deleted, its schema is changed or
	 * a connection failure occurs.
	 * @throws GSException if called after this object or the relevant {@link Container}
	 * is closed.
	 * @throws NullPointerException if an unsupported {@code null} parameter
	 * is specified when creating this query.
	 */
	public RowSet<R> fetch(boolean forUpdate) throws GSException;

	/**
	 * Releases related resources properly.
	 *
	 * @throws GSException not be thrown in the current version.
	 *
	 * @see Closeable#close()
	 */
	public void close() throws GSException;

	/**
	 * Returns {@link RowSet} of the latest result.
	 *
	 * <p>After {@link RowSet} returned once, it returns {@code null} until the
	 * new query is executed.</p>
	 *
	 * @return {@link RowSet} of the latest result at the first time,
	 * or {@code null} from the second time or no query executed before.
	 *
	 * @throws GSException if called after this object or the relevant {@link Container}
	 * is closed.
	 */
	public RowSet<R> getRowSet() throws GSException;

}
