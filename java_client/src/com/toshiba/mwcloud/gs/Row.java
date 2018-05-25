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

import java.sql.Blob;
import java.util.Date;

/**
 * A general-purpose Row for managing fields in any schema.
 */
public interface Row {

	/**
	 * Returns the schema corresponding to the specified Row.
	 *
	 * <p>It returns {@link ContainerInfo} in which only the Column layout
	 * information including the existance of any {@link RowKey} is set, and
	 * the Container name, the Container type, index settings, and the
	 * TimeSeries configuration options are not included.</p>
	 *
	 * @return {@link ContainerInfo} having only container information
	 * related to the schema.
	 *
	 * @throws GSException This will not be thrown in the current version.
	 */
	public ContainerInfo getSchema() throws GSException;

	/**
	 * Sets the value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value or its element.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setValue(int column, Object fieldValue) throws GSException;

	/**
	 * Returns the value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 */
	public Object getValue(int column) throws GSException;

	/**
	 * Sets the String value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setString(int column, String fieldValue) throws GSException;

	/**
	 * Returns the String value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public String getString(int column) throws GSException;

	/**
	 * Sets the boolean value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setBool(int column, boolean fieldValue) throws GSException;

	/**
	 * Returns the boolean value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public boolean getBool(int column) throws GSException;

	/**
	 * Sets the byte value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setByte(int column, byte fieldValue) throws GSException;

	/**
	 * Returns the byte value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public byte getByte(int column) throws GSException;

	/**
	 * Sets the short value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setShort(int column, short fieldValue) throws GSException;

	/**
	 * Returns the short value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public short getShort(int column) throws GSException;

	/**
	 * Sets the int value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setInteger(int column, int fieldValue) throws GSException;

	/**
	 * Returns the int value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public int getInteger(int column) throws GSException;

	/**
	 * Sets the long value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setLong(int column, long fieldValue) throws GSException;

	/**
	 * Returns the long value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public long getLong(int column) throws GSException;

	/**
	 * Sets the float value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setFloat(int column, float fieldValue) throws GSException;

	/**
	 * Returns the float value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public float getFloat(int column) throws GSException;

	/**
	 * Sets the double value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setDouble(int column, double fieldValue) throws GSException;

	/**
	 * Returns the double value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public double getDouble(int column) throws GSException;

	/**
	 * Sets the TIMESTAMP value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this object will be changed or not.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setTimestamp(int column, Date fieldValue) throws GSException;

	/**
	 * Returns the TIMESTAMP value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this object will be changed or not.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public Date getTimestamp(int column) throws GSException;

	/**
	 * Sets the Geometry value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setGeometry(
			int column, Geometry fieldValue) throws GSException;

	/**
	 * Returns the Geometry value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public Geometry getGeometry(int column) throws GSException;

	/**
	 * Sets the Blob value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setBlob(int column, Blob fieldValue) throws GSException;

	/**
	 * Returns the Blob value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public Blob getBlob(int column) throws GSException;

	/**
	 * Sets the String array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value or its element.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setStringArray(
			int column, String[] fieldValue) throws GSException;

	/**
	 * Returns the String array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public String[] getStringArray(
			int column) throws GSException;

	/**
	 * Sets the boolean array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setBoolArray(
			int column, boolean[] fieldValue) throws GSException;

	/**
	 * Returns the boolean array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public boolean[] getBoolArray(int column) throws GSException;

	/**
	 * Sets the byte array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setByteArray(
			int column, byte[] fieldValue) throws GSException;

	/**
	 * Returns the byte array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public byte[] getByteArray(int column) throws GSException;

	/**
	 * Sets the short array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setShortArray(
			int column, short[] fieldValue) throws GSException;

	/**
	 * Returns the short array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public short[] getShortArray(int column) throws GSException;

	/**
	 * Sets the int array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setIntegerArray(
			int column, int[] fieldValue) throws GSException;

	/**
	 * Returns the int array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public int[] getIntegerArray(int column) throws GSException;

	/**
	 * Sets the long array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setLongArray(
			int column, long[] fieldValue) throws GSException;

	/**
	 * Returns the long array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public long[] getLongArray(int column) throws GSException;

	/**
	 * Sets the float array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setFloatArray(
			int column, float[] fieldValue) throws GSException;

	/**
	 * Returns the byte float value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public float[] getFloatArray(int column) throws GSException;

	/**
	 * Sets the double array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setDoubleArray(
			int column, double[] fieldValue) throws GSException;

	/**
	 * Returns the double array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public double[] getDoubleArray(int column) throws GSException;

	/**
	 * Sets the TIMESTAMP array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this content will be changed or not.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value or its element.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public void setTimestampArray(
			int column, Date[] fieldValue) throws GSException;

	/**
	 * Returns the TIMESTAMP array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this content will be changed or not
	 * On the otherhand, the contents of the returned object will not be changed
	 * by operating this object.</p>
	 *
	 * <p>An effect of updates of the returned object to this object is uncertain.
	 * Moreover, after an object is returned,
	 * updating this object will not change the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 */
	public Date[] getTimestampArray(int column) throws GSException;

}
