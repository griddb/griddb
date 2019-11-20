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
 * {@link Container}のキーと対応することを示します。
 *
 * <p>複合ロウキーの場合、複合ロウキー全体を一つのオブジェクトとして
 * 設定・取得するためのフィールド・メソッドに対して使用します。
 * 複合ロウキーを構成する各ロウフィールドに対しては、使用できません。</p>
 * </div><div lang="en">
 * TODO Indicates the correspondence with a key of {@link Container}.
 * </div>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface RowKey {

}
