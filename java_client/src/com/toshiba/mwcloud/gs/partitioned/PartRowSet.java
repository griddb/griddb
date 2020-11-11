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
package com.toshiba.mwcloud.gs.partitioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TreeSet;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.QueryAnalysisEntry;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.Extensibles;
import com.toshiba.mwcloud.gs.common.Extensibles.AsContainer;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.Extensibles.QueryInfo;
import com.toshiba.mwcloud.gs.experimental.Experimentals;
import com.toshiba.mwcloud.gs.subnet.SubnetRowSet;
import com.toshiba.mwcloud.gs.subnet.SubnetRowSet.DistributedQueryTarget;

class PartRowSet<R>
implements RowSet<R>, Extensibles.AsRowSet<R>, Experimentals.AsRowSet<R> {

	private final PartContainer<?, ?> container;

	private final List<Extensibles.AsQuery<R>> subQueryList =
			new ArrayList<Extensibles.AsQuery<R>>();

	private final List<Extensibles.AsRowSet<R>> subList =
			new ArrayList<Extensibles.AsRowSet<R>>();

	private final List<Long> subIdList = new ArrayList<Long>();

	private final QueryInfo queryInfo;

	private Set<Long> problemSubIdSet;

	private Extensibles.AsRowSet<R> lastSub;

	private int nextSubIndex;

	private int rowCount = -1;

	private boolean subContainerLost;

	private Throwable lastProblem;

	private Filter filter;

	private DistributedQueryTarget target;

	PartRowSet(PartContainer<?, ?> container, QueryInfo queryInfo) {
		this.container = container;
		this.queryInfo = queryInfo;
	}

	@Override
	public boolean hasNext() throws GSException {
		return filter.hasNext(this);
	}

	@Override
	public R next() throws GSException {
		return filter.next(this);
	}

	@Override
	public void remove() throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public void update(R rowObj) throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public int size() {
		if (rowCount < 0) {
			try {
				try {
					prepareSubList(subQueryList.size());
				}
				catch (Throwable t) {
					throw closeByProblem(t);
				}
				rowCount = calculateRowCount(subList);
			}
			catch (GSException e) {
				throw new IllegalStateException(e);
			}
		}

		return rowCount;
	}

	@Override
	public void close() throws GSException {
		nextSubIndex = -1;
		closeAllSub(true);
	}

	@Override
	public Class<?> getMappingRowType() throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public Set<Integer> getExtOptionKeys() throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public byte[] getExtOption(int key) throws GSException {
		throw PartContainer.errorNotSupported();
	}

	@Override
	public QueryInfo getQueryInfo() {
		return queryInfo;
	}

	@Override
	public AsContainer<?, ?> getContainer() {
		return container;
	}

	@Override
	public Experimentals.RowId getRowIdForUpdate() throws GSException {
		throw PartContainer.errorNotSupported();
	}

	void addSub(long subId, Extensibles.AsQuery<R> sub) throws GSException {
		checkOpened();
		rowCount = -1;
		subQueryList.add(sub);
		subIdList.add(subId);
	}

	void removeUncheckedSub() throws GSException {
		final int subCount = subList.size();
		for (Extensibles.AsQuery<R> sub :
				subQueryList.subList(subCount, subQueryList.size())) {
			sub.close();
		}
		while (subQueryList.size() > subCount) {
			subQueryList.remove(subCount);
		}
		while (subIdList.size() > subCount) {
			subIdList.remove(subCount);
		}
	}

	List<Long> getSubIdList() {
		return subIdList;
	}

	boolean isProblemSubId(Long subId) {
		return problemSubIdSet != null && problemSubIdSet.contains(subId);
	}

	void addProblemSubId(Long subId) {
		if (problemSubIdSet == null) {
			problemSubIdSet = new HashSet<Long>();
		}
		problemSubIdSet.add(subId);
	}

	DistributedQueryTarget getTarget(boolean resolving) throws GSException {
		if (resolving) {
			prepareSubList(subQueryList.size());
		}
		return target;
	}

	void clearTarget() throws GSException {
		target = null;
	}

	boolean checkSubContainerLost() throws GSException {
		prepareSubList(subQueryList.size());
		return subContainerLost;
	}

	void clearSubContainerLost() {
		subContainerLost = false;
	}

	void prepareFollowing() throws GSException {
		checkOpened();

		try {
			prepareFilterAndRowType();
			filter.prepareFollowing(this);
		}
		catch (Throwable t) {
			throw closeByProblem(t);
		}
	}

	void prepareFollowingUnfiltered() throws GSException {
		Extensibles.AsRowSet<R> sub = lastSub;
		for (;;) {
			if (sub != null) {
				if (sub.hasNext()) {
					break;
				}
				sub.close();
			}

			if (nextSubIndex >= subQueryList.size()) {
				sub = null;
				break;
			}

			prepareSubList(nextSubIndex + 1);
			sub = subList.get(nextSubIndex);
			nextSubIndex++;
		}
		lastSub = sub;
	}

	boolean hasNextUnfiltered() throws GSException {
		return lastSub != null;
	}

	R nextUnfiltered() throws GSException {
		final R rowObj;
		try {
			rowObj = lastSub.next();
		}
		catch (NullPointerException e) {
			checkOpened();
			throw new GSException(
					GSErrorCode.NO_SUCH_ELEMENT, "No more rows were found", e);
		}
		if (!lastSub.hasNext()) {
			prepareFollowingUnfiltered();
		}
		return rowObj;
	}

	int findLastSubIndex() {
		if (lastSub != null && nextSubIndex >= 0) {
			final ListIterator<Extensibles.AsRowSet<R>> it =
					subList.listIterator(nextSubIndex);
			while (it.hasPrevious()) {
				if (it.previous() == lastSub) {
					return it.nextIndex();
				}
			}
		}
		return -1;
	}

	static long[] getDistLimits(
			Extensibles.AsRowSet<?> rowSet) throws GSException {
		final byte[] bytes =
				rowSet.getExtOption(ExtResultOptionType.DIST_LIMIT.number());
		if (bytes == null) {
			return null;
		}
		final BasicBuffer buf = BasicBuffer.wrap(bytes);

		final long limit = buf.base().getLong();
		final long offset = buf.base().getLong();
		return new long[] { limit, offset };
	}

	private void checkOpened() throws GSException {
		if (nextSubIndex < 0) {
			final Throwable lastProblem = this.lastProblem;
			if (lastProblem != null) {
				throw new GSException(
						"Row set already closed by other problem (" +
						"reason=" + lastProblem.getMessage() + ")",
						lastProblem);
			}

			throw new GSException(
					GSErrorCode.RESOURCE_CLOSED, "Already closed");
		}
	}

	private GSException closeByProblem(Throwable cause) {
		try {
			close();
		}
		catch (Throwable t) {
		}
		finally {
			lastProblem = cause;
		}

		if (cause instanceof GSException) {
			return (GSException) cause;
		}
		return new GSException(cause);
	}

	private void closeAllSub(boolean silent) throws GSException {
		boolean succeeded = false;
		try {
			final int subCount = subQueryList.size();
			for (int i = 0; i < subCount; i++) {
				final Query<?> sub = subQueryList.get(i);
				if (sub == null) {
					continue;
				}
				if (silent) {
					try {
						sub.close();
					}
					catch (Throwable t) {
					}
				}
				else {
					sub.close();
				}
			}
			succeeded = true;
		}
		finally {
			lastSub = null;
			if (!succeeded && !silent) {
				closeAllSub(true);
			}
		}
	}

	private void prepareFilterAndRowType() throws GSException {
		if (filter != null) {
			return;
		}

		final Extensibles.AsRowSet<R> sub = resolveSubAny(true);

		final Class<?> containerRowType = container.getRowType();
		final Class<?> rowType = sub.getMappingRowType();

		Filter filter = Filter.DEFAULT_INSTANCE;

		final long[] limits = getDistLimits(sub);
		if (limits != null) {
			filter = new LimitFilter(filter, limits[0], limits[1]);
		}

		if (rowType == QueryAnalysisEntry.class) {
			filter = new AnalysisFilter(filter);
		}
		else if (rowType != containerRowType) {
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unexpected row set type (" +
					"container=" + container.getLargeKey() +
					", expected=" + containerRowType +
					", actual=" + rowType +")");
		}

		this.filter = filter;
	}

	private void prepareSubList(int endIndex) throws GSException {
		for (int i = subList.size(); i < endIndex; i++) {
			subList.add(null);
			subList.remove(i);

			final Extensibles.AsQuery<R> subQuery = subQueryList.get(i);
			final Extensibles.AsRowSet<R> sub = subQuery.getRowSet();
			if (sub == null) {
				subContainerLost = true;
			}
			subList.add(sub);

			if (sub != null && target == null) {
				target = SubnetRowSet.getDistTarget(sub);
			}
		}
	}

	private Extensibles.AsRowSet<R> resolveSubAny(boolean force)
			throws GSException {
		Extensibles.AsRowSet<R> sub = null;
		final int subCount = subQueryList.size();
		for (int i = nextSubIndex; i < subCount; i++) {
			prepareSubList(i + 1);
			sub = subList.get(i);

			if (sub != null) {
				break;
			}
		}

		if (sub == null && force) {
			throw new GSException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by no available partitioned containers (" +
					"container=" + container.getLargeKey() + ")");
		}

		return sub;
	}

	private static int calculateRowCount(
			List<? extends RowSet<?>> subList) throws GSException {
		long value = 0;
		for (RowSet<?> sub : subList) {
			if (sub == null) {
				continue;
			}
			value += sub.size();
			if (value > Integer.MAX_VALUE) {
				throw new GSException(
						GSErrorCode.UNSUPPORTED_OPERATION,
						"Row count exceeds max value of Integer");
			}
		}
		return (int) value;
	}

	enum ExtResultOptionType {

		DIST_TARGET(SubnetRowSet.ExtResultOptionType.DIST_TARGET),
		DIST_LIMIT(33);

		static final Set<Integer> DEFAULT_OPTIONS;
		static {
			final Set<Integer> set = new TreeSet<Integer>();
			set.add(DIST_LIMIT.number());
			DEFAULT_OPTIONS = Collections.unmodifiableSet(set);
		}

		static final Set<Integer> TARGETED_OPTIONS;
		static {
			final Set<Integer> set = new TreeSet<Integer>(DEFAULT_OPTIONS);
			set.add(DIST_TARGET.number());
			TARGETED_OPTIONS = Collections.unmodifiableSet(set);
		}

		private final int number;

		ExtResultOptionType(int number) {
			this.number = number;
		}

		public int number() {
			return number;
		}

	}

	static class Filter {

		private static final Filter DEFAULT_INSTANCE = new Filter();

		<R> void prepareFollowing(
				PartRowSet<R> rowSet) throws GSException {
			rowSet.prepareFollowingUnfiltered();
		}

		<R> boolean hasNext(PartRowSet<R> rowSet) throws GSException {
			return rowSet.hasNextUnfiltered();
		}

		<R> R next(PartRowSet<R> rowSet) throws GSException {
			return rowSet.nextUnfiltered();
		}

		<R> void step(
				PartRowSet<R> rowSet, long count) throws GSException {
			for (long i = count; i != 0;) {
				if (!hasNext(rowSet)) {
					break;
				}
				next(rowSet);
				if (i > 0) {
					--i;
				}
			}
		}

	}

	static abstract class ChainFilter extends Filter {

		private final Filter base;

		ChainFilter(Filter base) {
			this.base = base;
		}

		@Override
		<R> void prepareFollowing(
				PartRowSet<R> rowSet) throws GSException {
			base.prepareFollowing(rowSet);
		}

		<R> boolean hasNext(PartRowSet<R> rowSet) throws GSException {
			return base.hasNext(rowSet);
		}

		@Override
		<R> R next(PartRowSet<R> rowSet) throws GSException {
			return base.next(rowSet);
		}

		@Override
		<R> void step(
				PartRowSet<R> rowSet, long count) throws GSException {
			base.step(rowSet, count);
		}

	}

	static class LimitFilter extends ChainFilter {

		private long restLimit;

		private long restOffset;

		private LimitFilter(Filter base, long limit, long offset) {
			super(base);
			restLimit = limit;
			restOffset = offset;
		}

		@Override
		<R> void prepareFollowing(
				PartRowSet<R> rowSet) throws GSException {
			super.prepareFollowing(rowSet);

			if (restOffset > 0) {
				super.step(rowSet, restOffset);
				restOffset = 0;
			}

			if (restLimit <= 0) {
				super.step(rowSet, -1);
			}
		}

		@Override
		<R> R next(PartRowSet<R> rowSet) throws GSException {
			final R rowObj = super.next(rowSet);
			if (--restLimit <= 0) {
				super.step(rowSet, -1);
			}

			return rowObj;
		}

	}

	static class AnalysisFilter extends ChainFilter {

		private AnalysisFilter(Filter base) {
			super(base);
		}

		@Override
		<R> R next(PartRowSet<R> rowSet) throws GSException {
			super.prepareFollowing(rowSet);
			final int subIndex = rowSet.findLastSubIndex();

			final R rowObj = super.next(rowSet);
			addSubName(rowSet, (QueryAnalysisEntry) rowObj, subIndex);
			return rowObj;
		}

		private void addSubName(
				PartRowSet<?> rowSet, QueryAnalysisEntry entry,
				int subIndex) {

			final StringBuilder builder = new StringBuilder();
			formatSubName(builder, rowSet, subIndex);
			final String stmt = entry.getStatement();
			if (!stmt.isEmpty()) {
				builder.append(':');
				builder.append(stmt);
			}
			entry.setStatement(builder.toString());
		}

		private static void formatSubName(
				StringBuilder builder, PartRowSet<?> rowSet,
				int subIndex) {
			if (subIndex < 0) {
				throw new IllegalStateException();
			}
			builder.append('#');
			builder.append(rowSet.subIdList.get(subIndex));
		}

	}

}
