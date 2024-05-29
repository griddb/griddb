package com.toshiba.mwcloud.gs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <div lang="ja">
 * {@link Container}の処理におけるマッピング対象のロウフィールドについて、
 * 日時精度を設定します。
 *
 * <p>このアノテーションが付与されていないロウフィールドは、日時精度の
 * 指定がなかったものとみなされます。</p>
 *
 * <p>日時精度とカラム型との対応は、{@link ColumnInfo#getTimePrecision()}の
 * 記載の通りです。</p>
 *
 * @since 5.3
 * </div><div lang="en">
 * Sets date/time precision in row fields to be mapped during 
 * {@link Container} operations.
 *
 * <p>A row field that does not have this annotation is considered 
 * not to have the specification of date/time precision.</p>
 *
 * <p>The mapping between time and date precision and column types 
 * is as specified in {@link ColumnInfo#getTimePrecision()}.</p>
 *
 * @since 5.3
 * </div>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface TimePrecision {

	/**
	 * <div lang="ja">
	 * 日時精度を返却します。
	 * @return 日時精度
	 * @since 5.3
	 * </div><div lang="en">
	 * Returns date/time precision
	 * @return date/time precision
	 * @since 5.3
	 * </div>
	 */
	TimeUnit value();

}
