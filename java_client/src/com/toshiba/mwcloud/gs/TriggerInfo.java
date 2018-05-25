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

/**
 * Represent the trigger information for monitoring the Container update.
 *
 * <p>Regarding the validity of the contents such as the notation of the trigger name,
 * this will not be necessarily inspected.</p>
 */
public class TriggerInfo {

	/**
	 * Represent the trigger type.
	 */
	public enum Type {

		/**
		 * Indicate the trigger type to notify in a REST notification when a Container is updated.
		 */
		REST,

		/**
		 * Indicate the trigger type to notify in a Java Message Service (JMS) notification when a Container is updated.
		 */
		JMS
	}

	/**
	 * Represent the update operation type subject to monitoring by the trigger.
	 */
	public enum EventType {

		/**
		 * Indicate the creation of a new row or update of an existing row for a Container.
		 */
		PUT,

		/**
		 * Indicate the deletion of a row for a Container.
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
	 * Generate the trigger information.
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

	/**
	 * Get the trigger name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set the trigger name.
	 *
	 * <p>If a blank character string or {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the trigger type.
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Set the trigger type.
	 *
	 * <p>If {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Get the notification destination URI when the trigger is discharged.
	 */
	public URI getURI() {
		return uri;
	}

	/**
	 * Set the notification destination URI when the trigger is discharged.
	 *
	 * <p>If {@code null} is set,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 */
	public void setURI(URI uri) {
		this.uri = uri;
	}

	/**
	 * Get the update operation type subject to trigger discharge.
	 *
	 * <p>When a returned value is updated,
	 * {@link UnsupportedOperationException} may occur.
	 * In addition, the contents of the returned object will not be changed
	 * by the operation on this object.</p>
	 */
	public Set<EventType> getTargetEvents() {
		return eventSet;
	}

	/**
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
	 */
	public void setTargetEvents(Set<EventType> eventSet) {
		final EnumSet<EventType> tmp = EnumSet.noneOf(EventType.class);
		tmp.addAll(eventSet);
		this.eventSet = Collections.unmodifiableSet(tmp);
	}

	/**
	 * Get the column name subject to notification when a trigger is discharged.
	 *
	 * <p>When a returned value is updated, {@link UnsupportedOperationException} may occur.
	 * In addition, the contents of the returned object will not be changed
	 * by the operation on this object.</p>
	 */
	public Set<String> getTargetColumns() {
		return columnNameSet;
	}

	/**
	 * Set the column name subject to notification when a trigger is discharged.
	 *
	 * <p>The column name is not case-sensitive. Even if the same column name is set several times,
	 * the value of the column will only be set once in the notification.</p>
	 *
	 * <p>If the column name is not specified, none of the column values will be set
	 * in the notification.</p>
	 *
	 * <p>The contents of the specified object will not change
	 * even if they are changed after they have been invoked.</p>
	 *
	 * @throws NullPointerException If {@code null} is specified in the argument
	 */
	public void setTargetColumns(Set<String> columnSet) {
		this.columnNameSet = Collections.unmodifiableSet(new HashSet<String>(columnSet));
	}

	/**
	 * Get the destination type used in a JMS notification.
	 */
	public String getJMSDestinationType() {
		return jmsDestinationType;
	}

	/**
	 * Set the destination type used in a JMS notification.
	 *
	 * <p>"queue" or "topic" can be specified. Case sensitive.</p>
	 *
	 * <p>If a character string other than "queue" or "topic" is specified,
	 * an error occurs when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 */
	public void setJMSDestinationType(String destinationType) {
		this.jmsDestinationType = destinationType;
	}

	/**
	 * Get the destination name used in a JMS notification.
	 */
	public String getJMSDestinationName() {
		return jmsDestinationName;
	}

	/**
	 * Set the destination name used in a JMS notification.
	 *
	 * <p>If {@code null} is specified, an error occurs
	 * when {@link Container#createTrigger(TriggerInfo)} is executed.</p>
	 */
	public void setJMSDestinationName(String destinationName) {
		this.jmsDestinationName = destinationName;
	}

	/**
	 * Get user name when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 */
	public String getUser() {
		return user;
	}

	/**
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
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Get password when connecting to a notification destination server.
	 *
	 * <p>In the current version, the user name is used only when connecting to the JMS server
	 * with a JMS notification.</p>
	 */
	public String getPassword() {
		return password;
	}

	/**
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
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public String getJMSUser() {
		return getUser();
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public void setJMSUser(String user) {
		setUser(user);
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public String getJMSPassword() {
		return getPassword();
	}

	/**
	 * @deprecated
	 */
	@Deprecated
	public void setJMSPassword(String password) {
		setPassword(password);
	}

}
