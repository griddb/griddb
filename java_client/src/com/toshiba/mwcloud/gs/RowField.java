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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets options for mapping Row fields of a {@link Container}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface RowField {

	/**
	 * Sets the name to be used as a Column name.
	 *
	 * <p>If an empty string is specified, the Column name is determined based on the corresponding field name or
	 * method name.</p>
	 */
	String name() default "";

	/**
	 * Sets a Column number.
	 *
	 * <p>To specify the location of a Column explicitly, specify {@code 0} or more and less than the number of Columns.
	 * Duplicate Column numbers cannot be specified in a single Container. A Row key must be always assigned to
	 * the first Column. If the default value {@code -1} is specified, the corresponding Column number is automatically
	 * determined.</p>
	 */
	int columnNumber() default -1;

}
