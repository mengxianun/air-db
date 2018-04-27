/**
 * 
 */
package com.mxy.air.db.annotation;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 标记类或方法在执行时打印SQL语句信息, 方法将在执行之前打印SQL语句信息
 * 
 * @author mengxiangyun
 *
 */
@Retention(RUNTIME)
@Target({ TYPE, METHOD })
@Inherited
public @interface SQLLog {

}
