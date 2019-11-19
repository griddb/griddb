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

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import com.toshiba.mwcloud.gs.common.GSErrorCode;

/**
 * <div lang="ja">
 * コンテナの更新を監視するためのトリガ情報を表します。
 *
 * <p>トリガ名の表記などの内容の妥当性について、必ずしも検査するとは
 * 限りません。</p>
 *
 * @since 1.5
 * </div><div lang="en">
 * Represent the trigger information for monitoring the Container update.
 *
 * <p>Regarding the validity of the contents such as the notation of the trigger name,
 * this will not be necessarily inspected.</p>
 * </div>
 */
public class TriggerInfo {

	/**
	 * <div lang="ja">
	 * トリガの種別を表します。
	 * </div><div lang="en">
	 * Represent the trigger type.
	 * </div>
	 */
	public enum Type {

		/**
		 * <div lang="ja">
		 * コンテナの更新時にRESTで通知するトリガ種別を示します。
		 * </div><div lang="en">
		 * Indicate the trigger type to notify in a REST notification when a Container is updated.
		 * </div>
		 */
		REST,

		/**
		 * <div lang="ja">
		 * コンテナの更新時にJava Message Service(JMS)で通知するトリガ種別を示します。
		 * </div><div lang="en">
		 * Indicate the trigger type to notify in a Java Message Service (JMS) notification when a Container is updated.
		 * </div>
		 */
		JMS
	}

	/**
	 * <div lang="ja">
	 * トリガで監視対象とする更新操作種別を表します。
	 * </div><div lang="en">
	 * Represent the update operation type subject to monitoring by the trigger.
	 * </div>
	 */
	public enum EventType {

		/**
		 * <div lang="ja">
		 * コンテナに対するロウ新規作成または更新を示します。
		 * </div><div lang="en">
		 * Indicate the creation of a new row or update of an existing row for a Container.
		 * </div>
		 */
		PUT,

		/**
		 * <div lang="ja">
		 * コンテナに対するロウ削除を示します。
		 * </div><div lang="en">
		 * Indicate the deletion of a row for a Container.
		 * </div>
		 */
		DELETE
	}

	private String name;

	private Type type;

	private URI uri;

	private Set<EventType> eventSet;

	private Set<String> columnNameSet;

	private String jmsDestinationType;

	private String jmsDestinationName;

	private String user;

	private String password;

	/**
	 * <div lang="ja">
	 * トリガ情報を生成します。
	 * </div><div lang="en">
	 * Generate the trigger information.
	 * </div>
	 */
	public TriggerInfo() {
		this.name = null;
		this.type = null;
		this.uri = null;
		this.eventSet = Collections.emptySet();
		this.columnNameSet = Collections.emptySet();
		this.jmsDestinationType = null;
		this.jmsDestinationName = null;
		this.user = null;
		this.password = null;
	}

	TriggerInfo(TriggerInfo info) {
		final boolean sameClass;
		try {
			sameClass = (info.getClass() == TriggerInfo.class);
		}
		catch (NullPointerException e) {
			throw GSErrorCode.checkNullParameter(info, "info", e);
		}

		if (sameClass) {
			this.name = info.name;
			this.type = info.type;
			this.uri = info.uri;
			this.eventSet = info.eventSet;
			this.columnNameSet = info.columnNameSet;
			this.jmsDestinationType = info.jmsDestinationType;
			this.jmsDestinationName = info.jmsDestinationName;
			this.user = info.user;
			this.password = info.password;
		}
		else {
			setName(getName());
			setType(getType());
			setURI(getURI());
			setTargetEvents(getTargetEvents());
			setTargetColumns(getTargetColumns());
			setJMSDestinationType(getJMSDestinationType());
			setJMSDestinationName(getJMSDestinationName());
			setUser(getUser());
			setPassword(getPassword());
		}
	}

	/**
	 * <div lang="ja">
	 * トリガ名を取得します。
	 * </div><div lang="en">
	 * Get the trigger name.
	 * </div>
	 */
	public String getName() {
		return name;
	}

	/**
	 * <div lang="ja">
	 * トリガ名を設定します。
	 *
	 * <p>空文字列・{@code null}が設定された場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 * </div><div lang="en">
	 * Set the trigger name.
	 *
	 * <p>If a blank character string or {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 * </div>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <div lang="ja">
	 * トリガ種別を取得します。
	 * </div><div lang="en">
	 * Get the trigger type.
	 * </div>
	 */
	public Type getType() {
		return type;
	}

	/**
	 * <div lang="ja">
	 * トリガ種別を設定します。
	 *
	 * <p>{@code null}が設定された場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 * </div><div lang="en">
	 * Set the trigger type.
	 *
	 * <p>If {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 * </div>
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * <div lang="ja">
	 * トリガ発火時の通知先URIを取得します。
	 * </div><div lang="en">
	 * Get the notification destination URI when the trigger is discharged.
	 * </div>
	 */
	public URI getURI() {
		return uri;
	}

	/**
	 * <div lang="ja">
	 * トリガ発火時の通知先URIを設定します。
	 *
	 * <p>{@code null}が設定された場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 * </div><div lang="en">
	 * Set the notification destination URI when the trigger is discharged.
	 *
	 * <p>If {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 * </div>
	 */
	public void setURI(URI uri) {
		this.uri = uri;
	}

	/**
	 * <div lang="ja">
	 * トリガ発火対象とする更新操作種別を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 * </div><div lang="en">
	 * Get the update operation type subject to trigger discharge.
	 *
	 * <p>When a returned value is updated,
	 * {@link UnsupportedOperationException} may occur.
	 * In addition, the contents of the returned object will not be changed
	 * by the operation on this object.</p>
	 * </div>
	 */
	public Set<EventType> getTargetEvents() {
		return eventSet;
	}

	/**
	 * <div lang="ja">
	 * トリガ発火対象とする更新操作種別を設定します。
	 *
	 * <p>複数の更新操作を設定した場合は、そのいずれかが行われた場合にトリガが
	 * 発火します。</p>
	 *
	 * <p>更新操作の設定がない場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Set the update operation type subject to trigger discharge.
	 *
	 * <p>If multiple update operations are set, a trigger will be discharges
	 * if any one of these operations is carried out.</p>
	 *
	 * <p>If there is no update operation setting, an error occurs
	 * when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 *
	 * <p>The contents of the specified object will not change
	 * even if they are changed after they have been invoked.</p>
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * </div>
	 */
	public void setTargetEvents(Set<EventType> eventSet) {
		final EnumSet<EventType> tmp = EnumSet.noneOf(EventType.class);
		tmp.addAll(eventSet);
		this.eventSet = Collections.unmodifiableSet(tmp);
	}

	/**
	 * <div lang="ja">
	 * トリガ発火時に通知対象とするカラム名を取得します。
	 *
	 * <p>返却された値に対して変更操作を行った場合、
	 * {@link UnsupportedOperationException}が発生することがあります。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 * </div><div lang="en">
	 * Get the column name subject to notification when a trigger is discharged.
	 *
	 * <p>When a returned value is updated, {@link UnsupportedOperationException} may occur.
	 * In addition, the contents of the returned object will not be changed
	 * by the operation on this object.</p>
	 * </div>
	 */
	public Set<String> getTargetColumns() {
		return columnNameSet;
	}

	/**
	 * <div lang="ja">
	 * トリガ発火時に通知対象とするカラム名を設定します。
	 *
	 * <p>通知対象のカラムを特定する際、ASCIIの大文字・小文字表記の
	 * 違いは区別されません。同一のカラムを指す複数のカラム名を含めても、
	 * そのカラムの値は通知には一度しか設定されません。</p>
	 *
	 * <p>カラム名の指定がない場合、通知にはいずれのカラムの値も設定されません。</p>
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @throws NullPointerException 引数に{@code null}が指定された場合
	 * </div><div lang="en">
	 * Set the column name subject to notification when a trigger is discharged.
	 *
	 * <p>When identifying a column as the target of the
	 * notification, the difference in uppercase and lowercase
	 * letters is not distinguished. Even if the same column name is
	 * set several times, the value of the column will only be set
	 * once in the notification.</p>
	 *
	 * <p>If the column name is not specified, none of the column values will be set
	 * in the notification.</p>
	 *
	 * <p>The contents of the specified object will not change
	 * even if they are changed after they have been invoked.</p>
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 * </div>
	 */
	public void setTargetColumns(Set<String> columnSet) {
		this.columnNameSet = Collections.unmodifiableSet(new HashSet<String>(columnSet));
	}

	/**
	 * <div lang="ja">
	 * JMS通知で使用するデスティネーション種別を取得します。
	 * </div><div lang="en">
	 * Get the destination type used in a JMS notification.
	 * </div>
	 */
	public String getJMSDestinationType() {
		return jmsDestinationType;
	}

	/**
	 * <div lang="ja">
	 * JMS通知で使用するデスティネーション種別を設定します。
	 *
	 * <p>{@code "queue"}または{@code "topic"}が指定できます。
	 * ASCIIの大文字・小文字表記の違いは区別されます。</p>
	 *
	 * <p>"queue"または"topic"以外が指定された場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 * </div><div lang="en">
	 * Set the destination type used in a JMS notification.
	 *
	 * <p>{@code "queue"} or {@code "topic"} can be specified. Case sensitive.</p>
	 *
	 * <p>If a character string other than "queue" or "topic" is specified,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 * </div>
	 */
	public void setJMSDestinationType(String destinationType) {
		this.jmsDestinationType = destinationType;
	}

	/**
	 * <div lang="ja">
	 * JMS通知で使用するデスティネーション名を取得します。
	 * </div><div lang="en">
	 * Get the destination name used in a JMS notification.
	 * </div>
	 */
	public String getJMSDestinationName() {
		return jmsDestinationName;
	}

	/**
	 * <div lang="ja">
	 * JMS通知で使用するデスティネーション名を設定します。
	 *
	 * <p>{@code null}が指定された場合、
	 * {@link Container#createTrigger(TriggerInfo)}の実行時にエラーとなります。</p>
	 * </div><div lang="en">
	 * Set the destination name used in a JMS notification.
	 *
	 * <p>If {@code null} is specified, an error occurs
	 * when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 * </div>
	 */
	public void setJMSDestinationName(String destinationName) {
		this.jmsDestinationName = destinationName;
	}

	/**
	 * <div lang="ja">
	 * 通知先サーバに接続する際のユーザ名を取得します。
	 *
	 * <p>現バージョンでは、JMS通知でJMSサーバへ接続する場合にのみ使用されます。</p>
	 * </div><div lang="en">
	 * Get user name when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 * </div>
	 */
	public String getUser() {
		return user;
	}

	/**
	 * <div lang="ja">
	 * 通知先サーバに接続する際のユーザ名を設定します。
	 *
	 * <p>現バージョンでは、JMS通知でJMSサーバへ接続する場合にのみ使用されます。</p>
	 *
	 * <p>設定がない、または空文字列・{@code null}が設定された場合、
	 * 空文字列をユーザ名として使用し接続します。</p>
	 *
	 * <p>ユーザ名・パスワードとも設定がない、または空文字列・{@code null}
	 * が設定された場合、ユーザ名・パスワードを使用せずに接続します。</p>
	 * </div><div lang="en">
	 * Set user name when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 *
	 * <p>If there is no setting, or if a blank character string/{@code null} is set,
	 * the blank character string will be used as the user name in the connection.</p>
	 *
	 * <p>If both the user name and password have not been set, or if a blank character string/{@code null} is set,
	 * the user will be connected without using the user name and password.</p>
	 * </div>
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * <div lang="ja">
	 * 通知先サーバに接続する際のパスワードを取得します。
	 *
	 * <p>現バージョンでは、JMS通知でJMSサーバへ接続する場合にのみ使用されます。</p>
	 * </div><div lang="en">
	 * Get password when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 * </div>
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * <div lang="ja">
	 * 通知先サーバに接続する際のパスワードを設定します。
	 *
	 * <p>現バージョンでは、JMS通知でJMSサーバへ接続する場合にのみ使用されます。</p>
	 *
	 * <p>設定がない、または空文字列・{@code null}が設定された場合、
	 * 空文字列をパスワードとして使用し接続します。</p>
	 *
	 * <p>ユーザ名・パスワードとも設定がない、または空文字列・{@code null}が
	 * 設定された場合、ユーザ名・パスワードを使用せずに接続します。</p>
	 * </div><div lang="en">
	 * Set the password when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 *
	 * <p>If there is no setting, or if a blank character string/{@code null} is set,
	 * the blank character string will be used as the password in the connection.</p>
	 *
	 * <p>If both the user name and password have not been set,
	 * or if a blank character string/{@code null} is set, the user will be connected
	 * without using the user name and password.</p>
	 * </div>
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public String getJMSUser() {
		return getUser();
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void setJMSUser(String user) {
		setUser(user);
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public String getJMSPassword() {
		return getPassword();
	}

	/**
	 * <div lang="ja">
	 * @deprecated
	 * </div><div lang="en">
	 * @deprecated
	 * </div>
	 */
	@Deprecated
	public void setJMSPassword(String password) {
		setPassword(password);
	}

	static TriggerInfo toImmutable(TriggerInfo base) {
		if (base instanceof Immutable) {
			return (Immutable) base;
		}
		return new Immutable(base);
	}

	private static class Immutable extends TriggerInfo {

		Immutable(TriggerInfo info) {
			super(info);
		}

		@Override
		public void setName(String name) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setType(Type type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setURI(URI uri) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setTargetEvents(Set<EventType> eventSet) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setTargetColumns(Set<String> columnSet) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setJMSDestinationType(String destinationType) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setJMSDestinationName(String destinationName) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setUser(String user) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setPassword(String password) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setJMSUser(String user) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setJMSPassword(String password) {
			throw new UnsupportedOperationException();
		}

	}

}
