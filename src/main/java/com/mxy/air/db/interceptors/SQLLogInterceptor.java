package com.mxy.air.db.interceptors;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxy.air.db.DbException;

public class SQLLogInterceptor implements MethodInterceptor {

	private static final Logger logger = LoggerFactory.getLogger(SQLLogInterceptor.class);

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object[] args = invocation.getArguments();
		// 拦截方法参数(String sql, Object[] params), 第一个参数为SQL语句, 第二个参数为SQL参数
		print(args[0].toString(), (Object[]) args[1]);
		return invocation.proceed();
	}

	/**
	 * 替换SQL中的占位符为实际值并打印SQL
	 * 
	 * @param sql
	 * @param params
	 */
	private void print(String sql, Object[] params) {
		if (params == null || params.length == 0) {
			logger.info("SQL: {}", sql);
		}
		if (!match(sql, params)) {
			throw new DbException(String.format("SQL语句占位符数量与参数数量不匹配. SQL: %s.", sql));
		}
		int cols = params.length;
		Object[] values = new Object[cols];
		System.arraycopy(params, 0, values, 0, cols);
		for (int i = 0; i < cols; i++) {
			Object value = values[i];
			if (value instanceof Date) {
				values[i] = "'" + value + "'";
			} else if (value instanceof String) {
				values[i] = "'" + value + "'";
			} else if (value instanceof Boolean) {
				values[i] = (Boolean) value ? 1 : 0;
			}
		}
		String statement = String.format(sql.replaceAll("\\?", "%s"), values);
		logger.info("SQL: {}", statement);
	}

	/**
	 * SQL占位符和参数个数是否匹配
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	private static boolean match(String sql, Object[] params) {
		if (params == null || params.length == 0)
			return true;
		Matcher m = Pattern.compile("(\\?)").matcher(sql);
		int count = 0;
		while (m.find()) {
			count++;
		}
		return count == params.length;
	}

}
