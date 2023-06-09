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
	 * @since 5.3
	 * </div>
	 */
	TimeUnit value();

}
