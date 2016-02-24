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
package com.toshiba.mwcloud.gs.subnet;

import java.net.URL;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSRecoverableException;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.QueryAnalysisEntry;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TriggerInfo;
import com.toshiba.mwcloud.gs.TriggerInfo.EventType;
import com.toshiba.mwcloud.gs.common.BasicBuffer;
import com.toshiba.mwcloud.gs.common.BlobImpl;
import com.toshiba.mwcloud.gs.common.GSConnectionException;
import com.toshiba.mwcloud.gs.common.GSErrorCode;
import com.toshiba.mwcloud.gs.common.GSStatementException;
import com.toshiba.mwcloud.gs.common.LoggingUtils;
import com.toshiba.mwcloud.gs.common.PropertyUtils;
import com.toshiba.mwcloud.gs.common.RowMapper;
import com.toshiba.mwcloud.gs.common.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.common.RowMapper.BlobFactory;
import com.toshiba.mwcloud.gs.common.RowMapper.Cursor;
import com.toshiba.mwcloud.gs.common.RowMapper.MappingMode;
import com.toshiba.mwcloud.gs.common.Statement;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContainerCache;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.Context;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.ContextMonitor;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.RemoteReference;
import com.toshiba.mwcloud.gs.subnet.GridStoreChannel.SessionInfo;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.subnet.NodeConnection.OptionalRequestType;

@SuppressWarnings("deprecation")
public abstract class SubnetContainer<K, R> implements Container<K, R>,
		BlobFactory {

	private static final boolean BLOB_CLEAR_ON_OPERATION_ENABLED = false;

	static final int MAX_SESSION_REPAIR_COUNT = 2;

	public static final int SESSION_NOT_FOUND_ERROR_CODE = 110003;

	public static final int UUID_UNMATCHED_ERROR_CODE = 110016;

	public static final int ROW_SET_NOT_FOUND_ERROR_CODE = 60132;

	private static boolean timeSeriesUpdateEnabled = true;

	
	
	private static boolean queryStatementIdPreserved = true;

	private static final Map<Statement, Statement> TIME_SERIES_STATEMENT_MAP =
			new EnumMap<Statement, Statement>(Statement.class);

	static {
		final Map<Statement, Statement> map = TIME_SERIES_STATEMENT_MAP;

		map.put(
				Statement.GET_ROW,
				Statement.GET_TIME_SERIES_ROW);

		map.put(
				Statement.QUERY_TQL,
				Statement.QUERY_TIME_SERIES_TQL);

		map.put(
				Statement.PUT_ROW,
				Statement.PUT_TIME_SERIES_ROW);

		map.put(
				Statement.PUT_MULTIPLE_ROWS,
				Statement.PUT_TIME_SERIES_MULTIPLE_ROWS);

		map.put(
				Statement.DELETE_ROW,
				Statement.DELETE_TIME_SERIES_ROW);

		map.put(
				Statement.GET_MULTIPLE_ROWS,
				Statement.GET_TIME_SERIES_MULTIPLE_ROWS);
	}

	private static final Set<Statement> FIXED_SESSION_MODE_STATEMENTS =
			EnumSet.noneOf(Statement.class);

	static {
		final Set<Statement> set = FIXED_SESSION_MODE_STATEMENTS;
		set.add(Statement.CREATE_SESSION);
		set.add(Statement.CLOSE_SESSION);
		set.add(Statement.COMMIT_TRANSACTION);
		set.add(Statement.ABORT_TRANSACTION);
	}

	private final ContextMonitor contextMonitor =
			GridStoreChannel.createContextMonitorIfAvailable();

	private static final BaseGridStoreLogger LOGGER =
			LoggingUtils.getLogger("Transaction");

	private SubnetGridStore store;

	protected final GridStoreChannel channel;

	protected final Context context;

	protected final Class<R> rowType;

	protected final RowMapper mapper;

	private final int schemaVerId;

	private final int partitionId;

	private final long containerId;

	private final String normalizedContainerName;

	protected long sessionId = 0;

	private long transactionId = 1;

	private long statementId;

	private boolean sessionPrepared;

	private boolean containerLocked;

	private boolean transactionStarted;

	private boolean autoCommit = true;

	private boolean cacheDisabled;

	private BlobImpl lastBlob;

	private SessionReference sessionRef;

	protected SubnetContainer(SubnetGridStore store, GridStoreChannel channel,
			Context context, Class<R> rowType, RowMapper mapper,
			int schemaVerId, int partitionId, long containerId,
			String normalizedContainerName,
			String remoteContainerName) throws GSException {
		this.store = store;
		this.channel = channel;
		this.context = context;
		this.rowType = rowType;
		this.mapper = mapper;
		this.schemaVerId = schemaVerId;
		this.partitionId = partitionId;
		this.containerId = containerId;
		this.normalizedContainerName = normalizedContainerName;
		if (contextMonitor != null) {
			contextMonitor.setContainerName(remoteContainerName);
		}
		store.createReference(this);
	}

	SubnetGridStore getStore() {
		return store;
	}

	Class<R> getRowType() {
		return rowType;
	}

	public int getSchemaVersionId() {
		return schemaVerId;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public long getContainerId() {
		return containerId;
	}

	public String getNormalizedContainerName() {
		return normalizedContainerName;
	}

	protected void clearBlob(boolean force) {
		if ((force || autoCommit) && lastBlob != null) {
			lastBlob.close();
			lastBlob = null;
		}
	}

	void disableCache() {
		if (cacheDisabled) {
			return;
		}

		cacheDisabled = true;
		if (normalizedContainerName != null) {
			final ContainerCache cache = context.getContainerCache();
			if (cache != null) {
				cache.removeSchema(normalizedContainerName);
			}
		}
	}

	protected void executeStatement(
			Statement statement, BasicBuffer req, BasicBuffer resp,
			StatementFamily familyForSession) throws GSException {

		
		
		
		
		
		
		if (store == null || context.isClosedAsync()) {
			throw new GSException(
					GSErrorCode.CONTAINER_CLOSED, "Already closed");
		}

		final boolean sessionRequired = (familyForSession != null);
		for (int trialCount = 0;; trialCount++) {
			final long statementId;
			final long curSessionId = this.sessionId;
			if (sessionRequired) {
				if (!queryStatementIdPreserved ||
						familyForSession != StatementFamily.QUERY) {
					while (++this.statementId == 0) {
					}
				}
				statementId = this.statementId;
				if (curSessionId == 0 || statementId == 0) {
					throw new Error(
							"Internal error by empty session or statement ID");
				}
				final SessionReference sessionRef = this.sessionRef;
				if (sessionRef != null) {
					sessionRef.lastStatementId = statementId;
				}
			}
			else {
				statementId = 0;
			}

			final int requestSize = req.base().position();
			try {
				if (contextMonitor != null) {
					contextMonitor.startStatement(statement, statementId,
							getPartitionId(), getContainerId());
				}

				final Statement actualStatement;
				if (!SubnetGridStore.isContainerStatementUnified() &&
						mapper.isForTimeSeries() &&
						TIME_SERIES_STATEMENT_MAP.containsKey(statement)) {
					actualStatement = TIME_SERIES_STATEMENT_MAP.get(statement);
				}
				else {
					actualStatement = statement;
				}

				channel.executeStatement(
						context, actualStatement, getPartitionId(),
						statementId, req, resp, contextMonitor);

				if (sessionRequired) {
					if (!sessionPrepared) {
						setSessionIdDirect(curSessionId, true);
					}

					if (!autoCommit) {
						setTransactionStarted(true);
					}
				}

				if (trialCount > 0 && LOGGER.isInfoEnabled()) {
					LOGGER.info(
							"transaction.sessionRepaired",
							ContextMonitor.getObjectId(context),
							statement,
							partitionId,
							statementId,
							containerId,
							curSessionId,
							trialCount);
				}

				if (contextMonitor != null) {
					contextMonitor.endStatement();
				}
				break;
			}
			catch (GSStatementException e) {
				final boolean started = transactionStarted || containerLocked;
				if (statement != Statement.CLOSE_SESSION) {
					try {
						synchronized (context) {
							closeSession(true);
						}
					}
					catch (Exception e2) {
					}
				}

				if (!sessionRequired || !isInitialSessionLost(
						statement, statementId, started, e)) {
					try {
						disableCache();
					}
					catch (Exception e2) {
					}

					if (contextMonitor == null) {
						throw e;
					}
					else {
						throw contextMonitor.analyzeStatementException(
								e, context, this);
					}
				}
				else if (trialCount >= MAX_SESSION_REPAIR_COUNT) {
					throw new GSStatementException(
							e.getErrorCode(),
							"Failed to repair session (trialCount=" + trialCount +
							", reason=" + e.getMessage() + ")",
							e);
				}

				if (LOGGER.isInfoEnabled()) {
					LOGGER.info(
							"transaction.repairingSession",
							ContextMonitor.getObjectId(context),
							statement,
							partitionId,
							statementId,
							containerId,
							curSessionId,
							trialCount,
							started,
							e);
				}
			}

			final byte[] requestData = new byte[requestSize];
			req.base().position(0);
			req.base().get(requestData);
			channel.setupRequestBuffer(req);
			final int containerIdSize = Long.SIZE / Byte.SIZE;
			final int sessionIdPos = req.base().position() + containerIdSize;

			createSession();

			req.base().position(0);
			req.base().put(requestData);
			req.base().position(sessionIdPos);
			req.putLong(sessionId);

			if (isSessionIdGeneratorEnabled() &&
					!FIXED_SESSION_MODE_STATEMENTS.contains(statement)) {
				final int uuidSize = Long.SIZE / Byte.SIZE * 2;
				final int schemaVerIdSize = Integer.SIZE / Byte.SIZE;
				req.base().position(
						req.base().position() + uuidSize + schemaVerIdSize);
				req.putByteEnum(SessionMode.GET);
			}

			req.base().position(requestSize);
		}
	}

	protected void executeMultiStepStatement(
			Statement statement, BasicBuffer req, BasicBuffer resp,
			StatementFamily familyForSession, int statementStep)
			throws GSException {
		if (statementStep <= 0) {
			throw new Error("Internal error by illegal step");
		}

		try {
			executeStatement(statement, req, resp, familyForSession);
		}
		finally {
			if (sessionPrepared && isMultiStepStatementIdEnabled()) {
				statementId += statementStep - 1;
			}
		}
	}

	protected enum StatementFamily {

		QUERY,

		LOCK,

		UPDATE,

		POST

	}

	private static class SessionReference
	extends RemoteReference<SubnetContainer<?, ?>> {

		private final long sessionId;

		private long lastStatementId;

		public SessionReference(SubnetContainer<?, ?> target, long sessionId) {
			super(target, SubnetContainer.class,
					target.context, target.partitionId, target.containerId);
			this.sessionId = sessionId;
		}

		@Override
		public void close(GridStoreChannel channel, Context context)
				throws GSException {
			final SessionInfo sessionInfo = new SessionInfo(
					partitionId, containerId, sessionId, lastStatementId);
			closeSession(channel, context,
					context.getSynchronizedRequestBuffer(),
					context.getSynchronizedResponseBuffer(),
					sessionInfo);
		}

	}

	protected StatementFamily prepareSession(
			StatementFamily family) throws GSException {

		final boolean sessionRequired;
		if (isSessionIdGeneratorEnabled()) {
			boolean requiredImmediately;
			switch (family) {
			case QUERY:
				sessionRequired = (!autoCommit && transactionStarted);
				requiredImmediately = false;
				break;
			case LOCK:
				if (autoCommit) {
					throw new GSException(GSErrorCode.ILLEGAL_COMMIT_MODE,
							"Auto commit mode must be turned off " +
							"for a lock operation");
				}
				sessionRequired = true;
				requiredImmediately = false;
				break;
			case UPDATE:
				sessionRequired = (containerLocked ||
						!autoCommit || !mapper.hasKey());
				requiredImmediately = (sessionRequired && !mapper.hasKey());
				break;
			case POST:
				sessionRequired = true;
				requiredImmediately = true;
				break;
			default:
				throw new Error(
						"Internal error by unknown statement family");
			}

			if (!sessionPrepared) {
				if (requiredImmediately) {
					createSession();
				}
				else if (sessionRequired) {
					setSessionIdDirect(context.generateSessionId(), false);
				}
			}
		}
		else {
			sessionRequired =
					(family != StatementFamily.QUERY ||
					(!autoCommit && sessionId != 0));

			if (sessionRequired && sessionId == 0) {
				createSession();
			}
		}

		return (sessionRequired ? family : null);
	}

	private void createSession() throws GSException {
		final ContainerCache cache = context.getContainerCache();
		if (cache != null) {
			final SessionInfo sessionInfo = cache.takeSession(
					context, partitionId, containerId);
			if (sessionInfo != null) {
				setSessionIdDirect(sessionInfo.getSessionId(), true);
				statementId = sessionInfo.getLastStatementId();
				return;
			}
		}

		channel.cleanRemoteResources(
				context, Collections.singleton(SubnetContainer.class));

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		for (int trialCount = 0;; trialCount++) {
			channel.setupRequestBuffer(req);
			req.putLong(getContainerId());

			final long generatedSessionId = (isSessionIdGeneratorEnabled() ?
					context.generateSessionId() : 0);
			if (generatedSessionId != 0) {
				req.putLong(generatedSessionId);
			}
			if (isInitialSessionRetrialEnabled()) {
				req.putUUID(context.getSessionUUID());
			}

			NodeConnection.tryPutEmptyOptionalRequest(req);

			if (generatedSessionId == 0) {
				putNewSessionProperties(req, channel, context);
			}

			try {
				executeStatement(Statement.CREATE_SESSION, req, resp, null);
			}
			catch (GSStatementException e) {
				if (!isNewSessionConflicted(e)) {
					throw e;
				}
				else if (trialCount >= MAX_SESSION_REPAIR_COUNT) {
					throw new GSStatementException(
							e.getErrorCode(),
							"Failed to create session (" +
							"trialCount=" + trialCount +
							", reason=" + e.getMessage() + ")",
							e);
				}
				continue;
			}

			final long newSessionId = (generatedSessionId == 0 ?
					resp.base().getLong() : generatedSessionId);

			if (newSessionId == 0) {
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by empty session ID");
			}
			setSessionIdDirect(newSessionId, true);
			break;
		}
	}

	static void putNewSessionProperties(
			BasicBuffer req, GridStoreChannel channel,
			Context context) throws GSException {
		req.putInt(PropertyUtils.timeoutPropertyToIntSeconds(channel
				.getFailoverTimeoutMillis(context)));
		req.putInt(PropertyUtils.timeoutPropertyToIntSeconds(context
				.getTransactionTimeoutMillis()));
	}

	private void closeSession(boolean invalidating) throws GSException {
		
		

		if (sessionId == 0) {
			return;
		}

		try {
			final BasicBuffer req = context.getSynchronizedRequestBuffer();
			final BasicBuffer resp = context.getSynchronizedResponseBuffer();

			final ContainerCache cache = context.getContainerCache();
			if (cache == null || transactionStarted || invalidating) {
				channel.setupRequestBuffer(req);
				req.putLong(getContainerId());
				putSessionInfo(req, sessionId);
				NodeConnection.tryPutEmptyOptionalRequest(req);

				executeStatement(Statement.CLOSE_SESSION, req, resp,
						StatementFamily.POST);
			}
			else {
				final SessionInfo oldSessionInfo = cache.cacheSession(
						context, new SessionInfo(
								partitionId, containerId, sessionId, statementId));

				if (oldSessionInfo != null) {
					closeSession(channel, context, req, resp, oldSessionInfo);
				}
			}
		}
		finally {
			setSessionIdDirect(0, true);
		}
	}

	static void closeSession(
			GridStoreChannel channel, Context context,
			BasicBuffer req, BasicBuffer resp, SessionInfo sessionInfo)
			throws GSException {
		channel.setupRequestBuffer(req);
		req.putLong(sessionInfo.getContainerId());

		req.putLong(sessionInfo.getSessionId());
		if (isInitialSessionRetrialEnabled()) {
			req.putUUID(context.getSessionUUID());
		}
		NodeConnection.tryPutEmptyOptionalRequest(req);

		channel.executeStatement(
				context, Statement.CLOSE_SESSION,
				sessionInfo.getPartitionId(),
				sessionInfo.getLastStatementId() + 1, req, resp, null);
	}

	static void closeAllSessions(
			GridStoreChannel channel, Context context,
			BasicBuffer req, BasicBuffer resp,
			List<SessionInfo> allSessionInfos)
			throws GSException {
		final List<SessionInfo> sortedSessionInfos =
				new ArrayList<SessionInfo>(allSessionInfos);
		Collections.sort(sortedSessionInfos, new Comparator<SessionInfo>() {
			@Override
			public int compare(SessionInfo o1, SessionInfo o2) {
				return o1.getPartitionId() - o2.getPartitionId();
			}
		});

		final boolean summarized = SubnetGridStore.isSessionUUIDSummarized();

		for (int start = 0; start < sortedSessionInfos.size();) {
			final int partitionId = sortedSessionInfos.get(start).getPartitionId();
			int end = start;
			while (++end < sortedSessionInfos.size()) {
				final SessionInfo info = sortedSessionInfos.get(end);
				if (info.getPartitionId() != partitionId) {
					break;
				}
			}

			try {
				channel.setupRequestBuffer(req);
				if (summarized) {
					req.putUUID(context.getSessionUUID());
				}
				NodeConnection.tryPutEmptyOptionalRequest(req);

				req.putInt(end - start);
				for (int i = start; i < end; i++) {
					final SessionInfo info = sortedSessionInfos.get(i);

					NodeConnection.putStatementId(
							req, info.getLastStatementId() + 1);
					req.putLong(info.getContainerId());
					req.putLong(info.getSessionId());
					if (!summarized) {
						req.putUUID(context.getSessionUUID());
					}
				}

				channel.executeStatement(
						context, Statement.CLOSE_MULTIPLE_SESSIONS,
						partitionId, 0, req, resp, null);
			}
			catch (Exception e) {
				
			}
			start = end;
		}
	}

	enum TransactionMode {

		AUTO,

		BEGIN,

		CONTINUE

	}

	enum SessionMode {

		AUTO,

		CREATE,

		GET

	}

	enum TransactionInfoType {

		NO_UUID,

		SKIP_COMMIT_MODE,

	}

	protected void putTransactionInfo(
			BasicBuffer req, StatementFamily familyForSession,
			TransactionInfoType type, Boolean forUpdate) {
		final boolean sessionRequired = (familyForSession != null);
		if (sessionRequired && sessionId == 0) {
			throw new Error("Internal error by invalid session parameters");
		}

		req.putLong(sessionRequired ? sessionId : 0);

		if (type != TransactionInfoType.NO_UUID &&
				isInitialSessionRetrialEnabled()) {
			req.putUUID(context.getSessionUUID());
		}

		final boolean generatorEnabled = isSessionIdGeneratorEnabled();
		if (!generatorEnabled) {
			if (forUpdate != null) {
				req.putBoolean(forUpdate);
			}

			if (type != TransactionInfoType.SKIP_COMMIT_MODE) {
				req.putBoolean(autoCommit);
			}
		}

		req.putInt(getSchemaVersionId());

		if (generatorEnabled) {
			if (sessionRequired) {
				req.putByteEnum(sessionPrepared ?
						SessionMode.GET : SessionMode.CREATE);
			}
			else {
				req.putByteEnum(SessionMode.AUTO);
			}

			if (autoCommit || !sessionRequired) {
				req.putByteEnum(TransactionMode.AUTO);
			}
			else {
				req.putByteEnum(transactionStarted ?
						TransactionMode.CONTINUE : TransactionMode.BEGIN);
			}
		}

		tryPutOptionalRequest(req, (forUpdate != null && forUpdate), true);
	}

	protected void tryPutOptionalRequest(BasicBuffer req,
			boolean forUpdate, boolean containerLockAwared) {
		if (!NodeConnection.isOptionalRequestEnabled()) {
			return;
		}

		final boolean containerLockRequired =
				(containerLockAwared && containerLocked);

		if (forUpdate || containerLockRequired) {
			final OptionalRequest optionalRequest =
					context.getOptionalRequest();

			if (forUpdate) {
				optionalRequest.put(OptionalRequestType.FOR_UPDATE, true);
			}

			if (containerLockRequired) {
				optionalRequest.put(
						OptionalRequestType.CONTAINER_LOCK_REQUIRED, true);
			}

			optionalRequest.format(req);
		}
		else {
			NodeConnection.tryPutEmptyOptionalRequest(req);
		}
	}

	long getSessionIdDirect() {
		return sessionId;
	}

	void setSessionIdDirect(long sessionId, boolean statusUpdatable) {
		final boolean orgTransactionStarted = transactionStarted;
		final boolean orgSessionPrepared = sessionPrepared;
		final long orgSessionId = this.sessionId;

		if (statusUpdatable) {
			if (sessionId == 0) {
				statementId = 0;

				if (++transactionId == 0) {
					transactionId = 1;
				}

				sessionPrepared = false;
				containerLocked = false;
				transactionStarted = false;
			}
			else {
				sessionPrepared = true;
			}
		}

		try {
			synchronized (context) {
				if (this.sessionId != sessionId) {
					if (sessionRef != null) {
						context.removeRemoteReference(sessionRef);
						sessionRef = null;
					}

					if (sessionId != 0) {
						sessionRef = new SessionReference(this, sessionId);
						context.addRemoteReference(sessionRef);
					}
				}
			}
		}
		finally {
			this.sessionId = sessionId;

			if (LOGGER.isDebugEnabled()) {
				if (orgSessionPrepared ^ sessionPrepared) {
					LOGGER.debug(
							sessionPrepared ?
									"transaction.sessionStarted" :
									"transaction.sessionClosed",
							"transaction.sessionStarted",
							ContextMonitor.getObjectId(context),
							partitionId,
							containerId,
							(sessionPrepared ? sessionId : orgSessionId));
				}
				else if (sessionId != 0 && orgSessionId != sessionId) {
					LOGGER.debug(
							"transaction.sessionIdGenerated",
							ContextMonitor.getObjectId(context),
							partitionId,
							containerId,
							sessionId);
				}

				if (orgTransactionStarted ^ transactionStarted) {
					LOGGER.debug(
							transactionStarted ?
									"transaction.transactionStarted" :
									"transaction.transactionEnded",
							ContextMonitor.getObjectId(context),
							partitionId,
							containerId,
							sessionId);
				}
			}

			if (contextMonitor != null) {
				if (orgSessionPrepared ^ sessionPrepared) {
					if (sessionPrepared) {
						contextMonitor.startSession(sessionId);
					}
					else {
						contextMonitor.endSession();
					}
				}

				if (orgTransactionStarted ^ transactionStarted) {
					if (transactionStarted) {
						contextMonitor.startTransaction();
					}
					else {
						contextMonitor.endTransaction();
					}
				}
			}
		}
	}

	long getStatementIdDirect() {
		return statementId;
	}

	boolean isTransactionStarted() {
		return transactionStarted;
	}

	private void setTransactionStarted(boolean started) {
		final boolean orgStarted = transactionStarted;
		transactionStarted = started;

		if (orgStarted ^ started) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						started ?
								"transaction.transactionStarted" :
								"transaction.transactionEnded",
						ContextMonitor.getObjectId(context),
						partitionId,
						containerId,
						sessionId);
			}

			if (contextMonitor != null) {
				if (started) {
					contextMonitor.startTransaction();
				}
				else {
					contextMonitor.endTransaction();
				}
			}
		}
	}

	long updateStatementIdDirect() {
		if (sessionId == 0) {
			throw new Error("Internal error by empty session");
		}
		while (++this.statementId == 0) {
		}
		return this.statementId;
	}

	public static boolean isInitialSessionRetrialEnabled(int protocolVersion) {
		return (protocolVersion >= 2);
	}

	private static boolean isInitialSessionRetrialEnabled() {
		return isInitialSessionRetrialEnabled(NodeConnection.getProtocolVersion());
	}

	static boolean isSessionIdGeneratorEnabled() {
		return (NodeConnection.getProtocolVersion() >= 3);
	}

	static boolean isDDLSessionEnabled() {
		return (NodeConnection.getProtocolVersion() >= 3 &&
				!GridStoreChannel.v15DDLCompatible);
	}

	static boolean isRowSetIdHintDisabled() {
		return (NodeConnection.getProtocolVersion() >= 3);
	}

	static boolean isPartialRowSetLostAcceptable() {
		return (NodeConnection.getProtocolVersion() >= 3);
	}

	protected static MappingMode getRowMappingMode() {
		if (NodeConnection.getProtocolVersion() >= 3) {
			return MappingMode.ROWWISE_SEPARATED_V2; 
		}
		else {
			return MappingMode.ROWWISE_SEPARATED;
		}
	}

	static boolean isMultiStepStatementIdEnabled() {
		return (NodeConnection.getProtocolVersion() >= 5 &&
				!GridStoreChannel.v21StatementIdCompatible);
	}

	static boolean isNewSessionConflicted(GSStatementException cause) {
		if (!isSessionIdGeneratorEnabled()) {
			return false;
		}

		return (cause.getErrorCode() == UUID_UNMATCHED_ERROR_CODE);
	}

	static boolean isInitialSessionLost(
			Statement statement, long statementId, boolean transactionStarted,
			GSStatementException cause) {
		if (statement == Statement.CREATE_SESSION ||
				statement == Statement.CLOSE_SESSION) {
			return false;
		}

		final int errorCode = cause.getErrorCode();
		if (errorCode != SESSION_NOT_FOUND_ERROR_CODE &&
				errorCode != UUID_UNMATCHED_ERROR_CODE) {
			return false;
		}

		if (!isInitialSessionRetrialEnabled()) {
			return false;
		}

		if (isSessionIdGeneratorEnabled()) {
			if (transactionStarted) {
				return false;
			}
		}
		else {
			if (statementId != 1) {
				return false;
			}
		}

		return true;
	}

	protected void putSessionInfo(BasicBuffer req, long sessionId) {
		req.putLong(sessionId);
		if (isInitialSessionRetrialEnabled()) {
			req.putUUID(context.getSessionUUID());
		}
	}

	protected boolean isAutoCommit() {
		return autoCommit;
	}

	public long getSessionId() {
		return sessionId;
	}

	public RowMapper getRowMapper() {
		return mapper;
	}

	@Override
	public void commit() throws GSException {
		if (autoCommit) {
			throw new GSException(GSErrorCode.ILLEGAL_COMMIT_MODE,
					"Auto commit mode must be turned off " +
					"for a transactional operation");
		}

		if (!transactionStarted) {
			return;
		}

		if (++transactionId == 0) {
			transactionId = 1;
		}

		clearBlob(true);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putSessionInfo(req, sessionId);
		NodeConnection.tryPutEmptyOptionalRequest(req);

		try {
			executeStatement(Statement.COMMIT_TRANSACTION, req, resp,
					StatementFamily.POST);
		}
		finally {
			setTransactionStarted(false);
		}
	}

	@Override
	public void abort() throws GSException {
		if (autoCommit) {
			throw new GSException(GSErrorCode.ILLEGAL_COMMIT_MODE,
					"Auto commit mode must be turned off " +
					"for a transactional operation");
		}

		if (!transactionStarted) {
			return;
		}

		if (++transactionId == 0) {
			transactionId = 1;
		}

		clearBlob(true);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		boolean succeeded = false;
		try {
			channel.setupRequestBuffer(req);
			req.putLong(getContainerId());
			putSessionInfo(req, sessionId);
			NodeConnection.tryPutEmptyOptionalRequest(req);

			executeStatement(Statement.ABORT_TRANSACTION, req, resp,
					StatementFamily.POST);
			succeeded = true;
		}
		finally {
			try {
				setTransactionStarted(false);
			}
			finally {
				closeSubResources(!succeeded, true);
			}
		}
	}

	@Override
	public void setAutoCommit(boolean enabled) throws GSException {
		checkOpened();
		if (autoCommit && !enabled) {
			autoCommit = false;
			setTransactionStarted(false);
		}
		else if (!autoCommit && enabled) {
			commit();
			autoCommit = true;
		}
	}

	@Override
	public R get(K key) throws GSException {
		return get(key, false);
	}

	@Override
	public R get(K key, boolean forUpdate) throws GSException {
		final StatementFamily family = prepareSession(forUpdate ?
				StatementFamily.LOCK : StatementFamily.QUERY);

		clearBlob(false);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, forUpdate);

		mapper.encodeKey(req, key, getRowMappingMode());

		executeStatement(Statement.GET_ROW, req, resp, family);

		if (!resp.getBoolean()) {
			return null;
		}

		final boolean rowIdIncluded = !mapper.isForTimeSeries();
		final Cursor cursor = mapper.createCursor(
				resp, getRowMappingMode(), 1, rowIdIncluded, this);
		return rowType.cast(mapper.decode(cursor));
	}

	@Override
	public boolean put(R value) throws GSException {
		return put(null, value);
	}

	@Override
	public boolean put(K key, R value) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.UPDATE);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, null);

		mapper.encode(req, getRowMappingMode(), key, value);

		executeStatement(Statement.PUT_ROW, req, resp, family);

		final boolean found = resp.getBoolean();
		clearBlob(false);
		return found;
	}

	@Override
	public boolean put(java.util.Collection<R> rowCollection)
			throws GSException {
		try {
			if (rowCollection.isEmpty()) {
				return false;
			}
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(
					rowCollection, "rowCollection", e);
		}

		final StatementFamily family = prepareSession(StatementFamily.UPDATE);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, null);

		final int rowCount = rowCollection.size();
		req.putLong(rowCount);

		final Cursor cursor = mapper.createCursor(
				req, getRowMappingMode(), rowCount, false, this);
		for (R row : rowCollection) {
			mapper.encode(cursor, null, row);
		}

		executeMultiStepStatement(
				Statement.PUT_MULTIPLE_ROWS, req, resp, family, rowCount);

		clearBlob(false);
		return resp.getBoolean();
	}

	@Override
	public SubnetQuery<R> query(String tql) throws GSException {
		return query(tql, rowType);
	}

	@Override
	public <S> SubnetQuery<S> query(final String tql, Class<S> rowType)
			throws GSException {
		checkOpened();
		final RowMapper resultMapper = mapper.applyResultType(rowType);

		return new SubnetQuery<S>(this, rowType, resultMapper,
				Statement.QUERY_TQL, new QueryFormatter() {
					@Override
					public void format(BasicBuffer inBuf) {
						try {
							inBuf.putString(tql);
						}
						catch (NullPointerException e) {
							throw GSErrorCode.checkNullParameter(
									tql, "tql", e);
						}
					}

					@Override
					public String getQueryString() {
						return tql;
					}
				});
	}

	static interface ContainerSubResource {
	}

	static interface TransactionalResource extends ContainerSubResource {
	}

	private void closeSubResources(
			boolean silent, boolean transactionalOnly) throws GSException {
		synchronized (context) {
			final Set<Class<?>> targetClasses = new HashSet<Class<?>>();

			for (Class<?> targetClass :
					context.getReferenceTargetClasses()) {

				if (!ContainerSubResource.class.isAssignableFrom(targetClass)) {
					continue;
				}

				if (transactionalOnly &&
						!TransactionalResource.class.isAssignableFrom(
								targetClass)) {
					continue;
				}

				targetClasses.add(targetClass);
			}

			channel.closeAllRemoteResources(
					context, targetClasses, partitionId, containerId, silent);
		}
	}

	<S> SubnetRowSet<S> queryAndFetch(Statement statement,
			Class<S> resultType, RowMapper resultMapper,
			QueryFormatter formatter, boolean forUpdate,
			long fetchSize) throws GSException {
		final StatementFamily family = prepareSession(forUpdate ?
				StatementFamily.LOCK : StatementFamily.QUERY);

		clearBlob(false);
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, forUpdate);
		formatter.format(req);

		Object targetConnection;
		synchronized (context) {
			if (contextMonitor != null) {
				contextMonitor.setQuery(formatter.getQueryString());
			}

			try {
				executeStatement(statement, req, resp, family);
			}
			finally {
				if (contextMonitor != null) {
					contextMonitor.setQuery(null);
				}
			}

			targetConnection = channel.getLastConnection(context);
		}

		return acceptQueryResponse(statement, resultType, resultMapper,
				forUpdate, fetchSize, resp, targetConnection, true);
	}

	void makeQueryRequest(Statement statement,
			QueryFormatter formatter, boolean forUpdate,
			long fetchSize, BasicBuffer req,
			boolean noUUID) throws GSException {
		if (forUpdate && (!sessionPrepared || sessionId == 0)) {
			throw new Error("Internal error by invalid session status");
		}

		final StatementFamily family = prepareSession(forUpdate ?
				StatementFamily.LOCK : StatementFamily.QUERY);

		final TransactionInfoType type =
				(noUUID ? TransactionInfoType.NO_UUID : null);

		req.putLong(getContainerId());
		putTransactionInfo(req, family, type, forUpdate);
		formatter.format(req);
	}

	<S> SubnetRowSet<S> acceptQueryResponse(Statement statement,
			Class<S> resultType, RowMapper resultMapper,
			boolean forUpdate, long fetchSize,
			BasicBuffer resp, Object targetConnection,
			boolean bufSwapAllowed) throws GSException {
		final Class<?> mapperRowType;
		final MappingMode mode;
		final boolean rowIdIncluded;
		final boolean partial;
		if (isAnyQueryResultTypeEnabled(
				NodeConnection.getProtocolVersion()) ||
				statement == Statement.QUERY_TQL ||
				!bufSwapAllowed) {
			switch (resp.getByteEnum(QueryResultType.class)) {
			case ROW_SET:
				mapperRowType = mapper.getRowType();
				mode = getRowMappingMode();
				rowIdIncluded = !mapper.isForTimeSeries();
				partial = false;
				break;
			case AGGREGATION_RESULT:
				mapperRowType = AggregationResult.class;
				mode = MappingMode.AGGREGATED;
				rowIdIncluded = false;
				partial = false;
				break;
			case QUERY_ANALYSIS:
				mapperRowType = QueryAnalysisEntry.class;
				mode = getRowMappingMode();
				rowIdIncluded = false;
				partial = false;
				break;
			case PARTIAL_ROW_SET:
				mode = getRowMappingMode();
				rowIdIncluded = !mapper.isForTimeSeries();
				mapperRowType = mapper.getRowType();
				partial = true;
				break;
			default:
				throw new GSException(GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by unknown result type");
			}
		}
		else {
			mode = getRowMappingMode();
			rowIdIncluded = !mapper.isForTimeSeries();
			mapperRowType = mapper.getRowType();
			partial = false;
		}

		if (resultType != null && resultType != mapperRowType) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Inconsistent result type (" +
					"requiredType=" + resultType + ", " +
					"actualType=" + mapperRowType + ")");
		}

		if (resultMapper == null) {
			resultMapper = mapper.applyResultType(mapperRowType);
		}
		else if (resultMapper.getRowType() != mapperRowType) {
			throw new GSException(GSErrorCode.ILLEGAL_PARAMETER,
					"Inconsistent result type (" +
					"requiredType=" + resultMapper.getRowType() + ", " +
					"actualType=" + mapperRowType + ")");
		}

		if (forUpdate) {
			setTransactionStarted(true);
		}

		final long rowSetTransactionId = (forUpdate ? transactionId : 0);
		if (partial) {
			final long totalRowCount = resp.base().getLong();
			final long rowSetId = resp.base().getLong();
			final long rowSetIdHint =
					(isRowSetIdHintDisabled() ? 0 : resp.base().getLong());

			final RowMapper.Cursor cursor;
			{
				final int rowCount = getResultRowSetCount(resp);
				final BasicBuffer resultBuffer =
						getResultBuffer(resp, bufSwapAllowed);
				cursor = resultMapper.createCursor(
						resultBuffer, mode, rowCount, rowIdIncluded, this);
			}

			return new SubnetRowSet<S>(this, resultType, resultMapper,
					cursor, rowSetTransactionId, totalRowCount,
					rowSetId, rowSetIdHint, fetchSize, targetConnection);
		}
		else {
			final int rowCount = getResultRowSetCount(resp);
			final BasicBuffer resultBuffer =
					getResultBuffer(resp, bufSwapAllowed);
			final RowMapper.Cursor cursor = resultMapper.createCursor(resultBuffer,
					mode, rowCount, rowIdIncluded, this);

			return new SubnetRowSet<S>(
					this, resultType, resultMapper, cursor,
					forUpdate ? transactionId : 0);
		}
	}

	public enum QueryResultType {
		ROW_SET,
		AGGREGATION_RESULT,
		QUERY_ANALYSIS,
		PARTIAL_ROW_SET
	}

	private BasicBuffer getResultBuffer(
			BasicBuffer resp, boolean bufSwapAllowed) {
		final int resultDataSize = resp.base().remaining();
		final BasicBuffer resultBuffer;
		if (!bufSwapAllowed ||
				resultDataSize < GridStoreChannel.INITIAL_BUFFER_SIZE) {
			resultBuffer = new BasicBuffer(resultDataSize);
			resultBuffer.base().put(resp.base());
			resultBuffer.base().flip();
		}
		else {
			
			
			resultBuffer = resp;
			context.replaceResponseBuffer(channel.createResponseBuffer());
		}
		return resultBuffer;
	}

	private int getResultRowSetCount(BasicBuffer resp) throws GSException {
		final long longRowCount = resp.base().getLong();
		if (longRowCount < 0 || longRowCount > Integer.MAX_VALUE) {
			throw new GSConnectionException(GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by result row count out of range");
		}
		return (int) longRowCount;
	}

	public static boolean isAnyQueryResultTypeEnabled(int protocolVersion) {
		return (protocolVersion >= 2 &&
				(GridStoreChannel.v1ProtocolCompatible == null ||
				!GridStoreChannel.v1ProtocolCompatible));
	}

	static void closeRowSet(
			GridStoreChannel channel, Context context,
			int partitionId, long containerId,
			long rowSetId, long rowSetIdHint,
			Object targetConnection) throws GSException {

		synchronized (context) {
			final BasicBuffer req = context.getSynchronizedRequestBuffer();
			final BasicBuffer resp = context.getSynchronizedResponseBuffer();

			channel.setupRequestBuffer(req);
			req.putLong(containerId);
			NodeConnection.tryPutEmptyOptionalRequest(req);

			req.putLong(rowSetId);
			if (!isRowSetIdHintDisabled()) {
				req.putLong(rowSetIdHint);
			}

			channel.checkActiveConnection(
					context, partitionId, targetConnection);
			channel.executeStatement(
					context, Statement.CLOSE_ROW_SET,
					partitionId, 0, req, resp, null);
		}
	}

	public RowMapper.Cursor fetchRowSet(
			long rowSetId, long rowSetIdHint,
			long totalCount, long remainingCount, long fetchSize,
			Object[] targetConnection,
			RowMapper resultMapper) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		req.putInt(getSchemaVersionId());
		tryPutOptionalRequest(req, false, true);

		req.putLong(rowSetId);
		if (!isRowSetIdHintDisabled()) {
			req.putLong(rowSetIdHint);
		}
		req.putLong(totalCount - remainingCount);
		req.putLong(fetchSize);

		synchronized (context) {
			channel.checkActiveConnection(
					context, partitionId, targetConnection[0]);
			try {
				executeStatement(Statement.FETCH_ROW_SET, req, resp, null);
			}
			catch (GSRecoverableException e) {
				targetConnection[0] = null;
				throw e;
			}
			catch (GSStatementException e) {
				if (e.getErrorCode() == ROW_SET_NOT_FOUND_ERROR_CODE) {
					targetConnection[0] = null;
					throw new GSRecoverableException(
							GSErrorCode.RECOVERABLE_ROW_SET_LOST,
							"Row set temporarily lost by connection problem (" +
							"reason=" + e.getMessage() + ")", e);
				}
				throw e;
			}
		}

		final boolean resultClosed = resp.getBoolean();
		if (resultClosed) {
			targetConnection[0] = null;
		}

		final long varDataBaseOffset = resp.base().getLong();
		final int resultRowCount = getResultRowSetCount(resp);

		final long newRemainingCount = remainingCount - resultRowCount;
		if (newRemainingCount < 0 ||
				(resultClosed && newRemainingCount > 0 &&
						!isPartialRowSetLostAcceptable()) ||
				(!resultClosed && newRemainingCount == 0)) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by unexpected result (" +
					"resultClosed=" + resultClosed +
					", resultRowCount=" + resultRowCount +
					", remainingCount=" + remainingCount + ")");
		}

		final BasicBuffer resultBuffer = getResultBuffer(resp, true);
		boolean rowIdIncluded = !mapper.isForTimeSeries();

		final RowMapper.Cursor cursor = resultMapper.createCursor(
				resultBuffer, getRowMappingMode(),
				resultRowCount, rowIdIncluded, this);
		cursor.setVarDataBaseOffset(varDataBaseOffset);

		return cursor;
	}

	@Override
	public boolean remove(K key) throws GSException {
		final StatementFamily family = prepareSession(StatementFamily.UPDATE);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, null);

		mapper.encodeKey(req, key, getRowMappingMode());
		executeStatement(Statement.DELETE_ROW, req, resp, family);

		final boolean found = resp.getBoolean();
		clearBlob(false);
		return found;
	}

	public void remove(RowMapper resolvedMapper, long transactionId,
			long rowId, Object key) throws GSException {
		if (transactionId != this.transactionId || autoCommit) {
			if (transactionId == 0) {
				throw new GSException(GSErrorCode.NOT_LOCKED,
						"Update option must be turned on");
			}
			throw new GSException(GSErrorCode.TRANSACTION_CLOSED,
					"Transaction expired");
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(this.getContainerId());

		final StatementFamily family = StatementFamily.UPDATE;
		if (this.mapper.isForTimeSeries()) {
			putTransactionInfo(req, family, null, null);

			resolvedMapper.encodeKey(req, key, getRowMappingMode());
			executeStatement(Statement.DELETE_ROW, req, resp, family);
		}
		else {
			putTransactionInfo(
					req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);

			req.putLong(rowId);
			executeStatement(Statement.DELETE_ROW_BY_ID, req, resp, family);
		}
	}

	public void update(RowMapper resolvedMapper, long transactionId,
			long rowId, Object key, Object newRowObj) throws GSException {
		if (this.mapper.isForTimeSeries() && !timeSeriesUpdateEnabled) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"TimeSeries row can not be updated");
		}

		if (transactionId != this.transactionId || autoCommit) {
			if (transactionId == 0) {
				throw new GSException(GSErrorCode.NOT_LOCKED,
						"Update option must be turned on");
			}
			throw new GSException(GSErrorCode.TRANSACTION_CLOSED,
					"Transaction expired");
		}

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(this.getContainerId());

		final Statement statement;
		final StatementFamily family = StatementFamily.UPDATE;
		if (this.mapper.isForTimeSeries()) {
			putTransactionInfo(req, family, null, null);

			statement = Statement.PUT_ROW;
		}
		else {
			putTransactionInfo(
					req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);
			req.putLong(rowId);

			statement = Statement.UPDATE_ROW_BY_ID;
		}

		resolvedMapper.encode(
				req, getRowMappingMode(), key, newRowObj);
		executeStatement(statement, req, resp, family);
	}

	@Override
	public Blob createBlob() throws GSException {
		checkOpened();
		if (BLOB_CLEAR_ON_OPERATION_ENABLED) {
			return (lastBlob = new BlobImpl(lastBlob));
		}
		else {
			return new BlobImpl(null);
		}
	}

	@Override
	public Blob createBlob(byte[] data) throws GSException {
		final BlobImpl blob;
		if (BLOB_CLEAR_ON_OPERATION_ENABLED) {
			blob = new BlobImpl(lastBlob);
			blob.setDataDirect(data);
			lastBlob = blob;
		}
		else {
			blob = new BlobImpl(null);
			blob.setDataDirect(data);
		}
		return blob;
	}

	@Override
	public void createIndex(String columnName) throws GSException {
		final IndexType indexType;
		try {
			indexType = getDefaultIndexType(columnName);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(columnName, "columnName", e);
		}

		if (indexType == null) {
			throw new GSException(GSErrorCode.UNSUPPORTED_DEFAULT_INDEX,
					"Default index can not be assigned (" +
					"columnName=" + columnName + ")");
		}
		createIndex(columnName, indexType);
	}

	@Override
	public void createIndex(
			String columnName, IndexType type) throws GSException {

		final StatementFamily family = (isDDLSessionEnabled() ?
				prepareSession(StatementFamily.POST) : null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		if (family != null) {
			putSessionInfo(req, sessionId);
		}
		req.putInt(getSchemaVersionId());
		tryPutOptionalRequest(req, false, true);

		try {
			req.putInt(mapper.resolveColumnId(columnName));
			req.putByteEnum(type);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(columnName, "columnName", e);
			GSErrorCode.checkNullParameter(type, "type", e);
			throw e;
		}

		executeStatement(Statement.CREATE_INDEX, req, resp, family);
	}

	@Override
	public void dropIndex(String columnName) throws GSException {
		final IndexType indexType;
		try {
			indexType = getDefaultIndexType(columnName);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(columnName, "columnName", e);
		}

		if (indexType == null) {
			return;
		}
		dropIndex(columnName, indexType);
	}

	@Override
	public void dropIndex(
			String columnName, IndexType type) throws GSException {

		final StatementFamily family = (isDDLSessionEnabled() ?
				prepareSession(StatementFamily.POST) : null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		if (family != null) {
			putSessionInfo(req, sessionId);
		}
		req.putInt(getSchemaVersionId());
		tryPutOptionalRequest(req, false, true);

		try {
			req.putInt(mapper.resolveColumnId(columnName));
			req.putByteEnum(type);
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(columnName, "columnName", e);
			GSErrorCode.checkNullParameter(type, "type", e);
			throw e;
		}

		executeStatement(Statement.DROP_INDEX, req, resp, family);
	}

	private IndexType getDefaultIndexType(String columnName) throws GSException {
		final int columnId = mapper.resolveColumnId(columnName);
		if (mapper.isArray(columnId)) {
			return null;
		}

		switch (mapper.getFieldElementType(columnId)) {
		case TIMESTAMP:
			if (columnId == 0 && mapper.isForTimeSeries()) {
				return null;
			}
			return IndexType.TREE;
		case GEOMETRY:
			if (mapper.isForTimeSeries()) {
				return null;
			}
			return IndexType.SPATIAL;
		case BLOB:
			return null;
		default:
			return IndexType.TREE;
		}
	}

	@Override
	public void createEventNotification(URL url) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		NodeConnection.tryPutEmptyOptionalRequest(req);

		try {
			req.putString(url.toString());
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(url, "url", e);
			throw e;
		}

		executeStatement(
				Statement.CREATE_EVENT_NOTIFICATION, req, resp, null);
	}

	@Override
	public void dropEventNotification(URL url) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		NodeConnection.tryPutEmptyOptionalRequest(req);

		try {
			req.putString(url.toString());
		}
		catch (NullPointerException e) {
			GSErrorCode.checkNullParameter(url, "url", e);
			throw e;
		}

		executeStatement(Statement.DROP_EVENT_NOTIFICATION, req, resp, null);
	}

	@Override
	public void flush() throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		NodeConnection.tryPutEmptyOptionalRequest(req);

		executeStatement(Statement.FLUSH_LOG, req, resp, null);
	}

	@Override
	public void close() throws GSException {

		
		
		
		synchronized (context) {
			if (isClosed()) {
				return;
			}

			try {
				closeSubResources(false, false);
			}
			finally {
				try {
					closeSession(false);
				}
				finally {
					final SubnetGridStore store = this.store;
					if (store != null) {
						this.store = null;
						store.removeReference(this);
					}
				}
			}
		}

	}

	public boolean isClosed() {
		return (store == null || context.isClosedAsync());
	}

	protected void checkOpened() throws GSException {
		if (isClosed()) {
			throw new GSException(GSErrorCode.RESOURCE_CLOSED,
					"Already closed");
		}
	}

	public interface QueryFormatter {
		public void format(BasicBuffer inBuf) throws GSException;
		public String getQueryString();
	}

	@Override
	public ContainerType getType() throws GSException {
		checkOpened();
		return mapper.getContainerType();
	}

	@Override
	public R createRow() throws GSException {
		checkOpened();
		return rowType.cast(mapper.createRow(false));
	}

	

	public RowSet<Row> getRowSet(
			long[] position, long fetchLimit) throws GSException {
		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());

		final StatementFamily family = null;
		putTransactionInfo(
				req, family, TransactionInfoType.SKIP_COMMIT_MODE, null);

		req.putLong(fetchLimit);
		req.putLong(position[0]);

		executeStatement(
				Statement.GET_MULTIPLE_ROWS, req, resp, family);

		position[0] = resp.base().getLong();
		final long rowCount = resp.base().getLong();

		
		
		final BasicBuffer resultData = resp;
		context.replaceResponseBuffer(channel.createResponseBuffer());

		if (mapper.getRowType() != Row.class) {
			throw new GSException(GSErrorCode.UNSUPPORTED_OPERATION,
					"Not supported for this row type (" +
					"class=" + mapper.getRowType() + ")");
		}

		final RowMapper.Cursor cursor = mapper.createCursor(
				resultData, getRowMappingMode(), (int) rowCount, false, this);

		return new SubnetRowSet<Row>(this, Row.class, mapper, cursor, 0);
	}

	public boolean putRowSet(SubnetRowSet<?> rowSet) throws GSException {
		final int rowCount = rowSet.size();
		if (rowCount == 0) {
			return false;
		}
		final StatementFamily family = prepareSession(StatementFamily.UPDATE);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		putTransactionInfo(req, family, null, null);

		req.putLong(rowCount);

		final Cursor cursor = mapper.createCursor(
				req, getRowMappingMode(), rowCount, false, this);
		for (int i = 0; i < rowCount; i++) {
			mapper.encode(cursor, null, rowSet.nextGeneralRow(), true);
		}

		executeMultiStepStatement(
				Statement.PUT_MULTIPLE_ROWS, req, resp, family, rowCount);

		clearBlob(false);
		return resp.getBoolean();
	}

	@Override
	public void createTrigger(TriggerInfo info) throws GSException {
		final StatementFamily family = (isDDLSessionEnabled() ?
				prepareSession(StatementFamily.POST) : null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		if (family != null) {
			putSessionInfo(req, sessionId);
		}
		req.putInt(getSchemaVersionId());
		tryPutOptionalRequest(req, false, true);

		if (info.getName() == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Trigger name not assigned");
		}
		req.putString(info.getName());
		if (info.getType() == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Trigger type not assigned");
		}
		req.putByteEnum(info.getType());
		if (info.getURI() == null) {
			throw new GSException(GSErrorCode.EMPTY_PARAMETER,
					"Trigger URI not assigned");
		}
		req.putString(info.getURI().toString());
		int eventType = 0;
		for (EventType e : info.getTargetEvents()) {
			eventType |= 1 << e.ordinal();
		}
		req.putInt(eventType);
		final int columnCount = info.getTargetColumns().size();
		req.putInt(columnCount);
		for (String columnName : info.getTargetColumns()) {
			req.putInt(mapper.resolveColumnId(columnName));
		}

		final String emptyString = "";
		switch (info.getType()) {
		case REST:
			req.putString(emptyString);
			req.putString(emptyString);
			req.putString(emptyString);
			req.putString(emptyString);
			req.putString(emptyString);
			break;
		case JMS:
			req.putString("activemq");

			if (info.getJMSDestinationType() == null) {
				throw new GSException(GSErrorCode.EMPTY_PARAMETER,
						"Destination type not assigned");
			}
			req.putString(info.getJMSDestinationType());

			if (info.getJMSDestinationName() == null) {
				throw new GSException(GSErrorCode.EMPTY_PARAMETER,
						"Destination name not assigned");
			}
			req.putString(info.getJMSDestinationName());

			if (info.getUser() == null) {
				req.putString(emptyString);
			} else {
				req.putString(info.getUser());
			}

			if (info.getPassword() == null) {
				req.putString(emptyString);
			} else {
				req.putString(info.getPassword());
			}
			break;

		default:
			throw new Error("Internal error by unknown trigger type");
		}

		executeStatement(Statement.CREATE_TRIGGER, req, resp, family);
	}

	@Override
	public void dropTrigger(String name) throws GSException {
		final StatementFamily family = (isDDLSessionEnabled() ?
				prepareSession(StatementFamily.POST) : null);

		final BasicBuffer req = context.getRequestBuffer();
		final BasicBuffer resp = context.getResponseBuffer();

		channel.setupRequestBuffer(req);
		req.putLong(getContainerId());
		if (family != null) {
			putSessionInfo(req, sessionId);
		}
		req.putInt(getSchemaVersionId());
		tryPutOptionalRequest(req, false, true);
		req.putString(name);

		executeStatement(Statement.DROP_TRIGGER, req, resp, family);
	}

}
