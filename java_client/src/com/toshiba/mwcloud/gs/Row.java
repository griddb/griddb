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
 * <div lang="ja">
 * 任意のスキーマについて汎用的にフィールド操作できるロウです。
 *
 * @since 1.5
 * </div><div lang="en">
 * A general-purpose Row for managing fields in any schema.
 * </div>
 */
public interface Row {

	/**
	 * <div lang="ja">
	 * このロウに対応するスキーマを取得します。
	 *
	 * <p>ロウキーの有無を含むカラムレイアウトにする情報のみが設定された
	 * {@link ContainerInfo}が求まります。
	 * コンテナ名、コンテナ種別、索引設定、時系列構成オプションなどその他の
	 * コンテナ情報は含まれません。</p>
	 *
	 * @return スキーマに関するコンテナ情報のみを持つ{@link ContainerInfo}
	 *
	 * @throws GSException 現バージョンでは送出されない
	 * </div><div lang="en">
	 * Returns the schema corresponding to the specified Row.
	 *
	 * <p>It returns {@link ContainerInfo} in which only the Column layout
	 * information including the existence of any {@link RowKey} is set, and
	 * the Container name, the Container type, index settings, and the
	 * TimeSeries configuration options are not included.</p>
	 *
	 * @return {@link ContainerInfo} having only container information
	 * related to the schema.
	 *
	 * @throws GSException This will not be thrown in the current version.
	 * </div>
	 */
	public ContainerInfo getSchema() throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のフィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 配列型のフィールド値の配列要素に{@code null}が
	 * 含まれる場合
	 * @throws GSException フィールドの値がカラムの型と一致しない場合
	 * </div><div lang="en">
	 * Sets the value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException when {@code null} is specified as a field value for a column with a NOT NULL constraint
	 * @throws GSException if the array element of a field value of array type contains {@code null}
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setValue(int column, Object fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のフィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * </div><div lang="en">
	 * Returns the value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * </div>
	 */
	public Object getValue(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSTRING型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the String value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as field value for a column with NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setString(int column, String fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSTRING型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the String value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public String getString(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBOOL型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the boolean value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setBool(int column, boolean fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBOOL型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the boolean value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public boolean getBool(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBYTE型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the byte value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setByte(int column, byte fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBYTE型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the byte value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public byte getByte(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSHORT型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the short value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setShort(int column, short fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSHORT型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the short value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public short getShort(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のINTEGER型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the int value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setInteger(int column, int fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のINTEGER型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the int value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public int getInteger(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のLONG型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the long value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setLong(int column, long fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のLONG型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the long value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public long getLong(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のFLOAT型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the float value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setFloat(int column, float fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のFLOAT型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the float value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public float getFloat(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のDOUBLE型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the double value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setDouble(int column, double fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のDOUBLE型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は空の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the double value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as empty value.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public double getDouble(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTIMESTAMP型フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更した場合に、
	 * このオブジェクトの内容が変化するかどうかは未定義です。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the TIMESTAMP value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this object will be changed or not.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setTimestamp(int column, Date fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTIMESTAMP型フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更した場合に、
	 * このオブジェクトの内容が変化するかどうかは未定義です。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the TIMESTAMP value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this object will be changed or not.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public Date getTimestamp(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のGEOMETRY型フィールドに値を設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the Geometry value to the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setGeometry(
			int column, Geometry fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のGEOMETRY型フィールドの値を取得します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the Geometry value of the specified field.
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public Geometry getGeometry(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBLOB型フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the Blob value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setBlob(int column, Blob fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBLOB型フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the Blob value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public Blob getBlob(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSTRING型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException フィールド値の配列要素に{@code null}が含まれる場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the String array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the array element of a field value of array type contains {@code null}
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setStringArray(
			int column, String[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSTRING型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the String array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public String[] getStringArray(
			int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBOOL型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the boolean array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setBoolArray(
			int column, boolean[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBOOL型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the boolean array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public boolean[] getBoolArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBYTE型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the byte array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setByteArray(
			int column, byte[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のBYTE型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the byte array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public byte[] getByteArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSHORT型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the short array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setShortArray(
			int column, short[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のSHORT型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the short array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public short[] getShortArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のINTEGER型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the int array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setIntegerArray(
			int column, int[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のINTEGER型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the int array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public int[] getIntegerArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のLONG型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the long array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setLongArray(
			int column, long[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のLONG型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the long array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public long[] getLongArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のFLOAT型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the float array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setFloatArray(
			int column, float[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のFLOAT型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the byte float value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public float[] getFloatArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のDOUBLE型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the double array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setDoubleArray(
			int column, double[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のDOUBLE型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更したとしても、
	 * このオブジェクトの内容は変化しません。
	 * また、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Returns the double array value of the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * the contents of this object will not be changed.
	 * Moreover, after an object is returned, updating this object will not change
	 * the contents of the returned object.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 *
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public double[] getDoubleArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTIMESTAMP型配列フィールドに値を設定します。
	 *
	 * <p>指定したオブジェクトの内容を呼び出し後に変更した場合に、
	 * このオブジェクトの内容が変化するかどうかは未定義です。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 * @param fieldValue 対象フィールドの値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムに対して、フィールド値
	 * として{@code null}が指定された場合
	 * @throws GSException フィールド値の配列要素に{@code null}が含まれる場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
	 * Sets the TIMESTAMP array value to the specified field.
	 *
	 * <p>If the contents of a specified object is changed after it has been invoked,
	 * it is not defined whether the contents of this content will be changed or not.</p>
	 *
	 * @param column the Column number of the target field, from {@code 0} to number of Columns minus one.
	 * @param fieldValue value of the target field
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if {@code null} is specified as the field value for a column with a NOT NULL constraint.
	 * @throws GSException if the array element of a field value of array type contains {@code null}
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public void setTimestampArray(
			int column, Date[] fieldValue) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のTIMESTAMP型配列フィールドの値を取得します。
	 *
	 * <p>返却されたオブジェクトの内容を呼び出し後に変更した場合に、
	 * このオブジェクトの内容が変化するかどうかは未定義です。
	 * 一方、このオブジェクトに対する操作により、返却されたオブジェクトの内容が
	 * 変化することはありません。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 対象フィールドの値。NULLが設定されている場合は{@code null}
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException 指定のカラム番号の型と一致しない場合
	 * </div><div lang="en">
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
	 * @return the value of the target field. If NULL is set as {@code null}.
	 *
	 * @throws GSException if the specified Column number is out of range.
	 * @throws GSException if the type of the specified field does not match the type of the Column.
	 * </div>
	 */
	public Date[] getTimestampArray(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のフィールドにNULLを設定します。
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 * @throws GSException NOT NULL制約の設定されたカラムが指定された場合
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Set the field to NULL.
	 *
	 * @param column Column number of the targeted field. Values greater than {@code 0} and less than the number of columns
	 *
	 * @throws GSException when a column number outside the range is specified
	 * @throws GSException when a column with NOT NULL constraint is specified
	 *
	 * @since 3.5
	 * </div>
	 */
	public void setNull(int column) throws GSException;

	/**
	 * <div lang="ja">
	 * 指定のフィールドにNULLが設定されているかどうかを返します。
	 *
	 * <p>NOT NULL制約の設定されたカラムが指定された場合、常に{@code false}を
	 * 返します。</p>
	 *
	 * @param column 対象フィールドのカラム番号。{@code 0}以上かつカラム数未満の値
	 *
	 * @return 指定のフィールドにNULLが設定されているかどうか
	 *
	 * @throws GSException 範囲外のカラム番号が指定された場合
	 *
	 * @since 3.5
	 * </div><div lang="en">
	 * Returns to the specified field regardless it is set to NULL or not.
	 *
	 * <p>Whenever a column with the NOT NULL constraint is specified, returns {@code false}.</p>
	 *
	 * @param column Column number of target field. Values greater than {@code 0} and less than the number of columns
	 *
	 * @return regardless the specified field is set to NULL or not
	 *
	 * @throws GSException when a column number outside the range is specified
	 *
	 * @since 3.5
	 * </div>
	 */
	public boolean isNull(int column) throws GSException;

}
