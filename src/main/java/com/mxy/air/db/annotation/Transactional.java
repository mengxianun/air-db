package com.mxy.air.db.annotation;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 将标记这个注解的类或方法将加入事务
 * 
 * @author mengxiangyun
 *
 */
@Retention(RUNTIME)
@Target({ TYPE, METHOD })
@Inherited
public @interface Transactional {

}
