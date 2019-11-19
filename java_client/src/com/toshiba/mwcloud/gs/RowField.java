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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <div lang="ja">
 * {@link Container}の処理におけるマッピング対象のロウフィールドについて、
 * オプションを設定します。
 *
 * <p>複合ロウキーを構成する各ロウフィールドに対しても、使用できます。
 * 複合ロウキーの場合、複合ロウキー全体を一つのオブジェクトとして
 * 設定・取得するためのフィールド・メソッドに対しては、使用できません。</p>
 *
 * </div><div lang="en">
 * TODO Sets options for mapping Row fields of a {@link Container}.
 * </div>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface RowField {

	/**
	 * <div lang="ja">
	 * 指定のカラム名を使用します。
	 *
	 * <p>空の文字を指定した場合、対応するフィールド名またはメソッド名に
	 * 基づきカラム名を決定します。</p>
	 * </div><div lang="en">
	 * Sets the name to be used as a Column name.
	 *
	 * <p>If an empty string is specified, the Column name is determined based on the corresponding field name or
	 * method name.</p>
	 * </div>
	 */
	String name() default "";

	/**
	 * <div lang="ja">
	 * カラム番号を設定します。
	 *
	 * <p>カラム順序を明示的に指定する場合、{@code 0}以上かつカラム数未満の値を
	 * 指定します。同一コンテナ上で重複するカラム番号を指定することはできません。
	 * また、現バージョンでは、ロウキーは常に先頭カラムになるように設定
	 * しなければなりません。デフォルト値の{@code -1}を指定した場合、対応する
	 * カラムの番号は自動的に決定されます。</p>
	 *
	 * <p>複合ロウキーを含むロウオブジェクトのうち、ロウキー以外の
	 * ロウフィールドに対して番号を設定する場合、複合ロウキーを構成する個々の
	 * カラムに割り当てられる番号と重複しないようにする必要があります。</p>
	 * </div><div lang="en">
	 * TODO Sets a Column number.
	 *
	 * <p>To specify the location of a Column explicitly, specify {@code 0} or more and less than the number of Columns.
	 * Duplicate Column numbers cannot be specified in a single Container. A Row key must be always assigned to
	 * the first Column. If the default value {@code -1} is specified, the corresponding Column number is automatically
	 * determined.</p>
	 * </div>
	 */
	int columnNumber() default -1;

}
