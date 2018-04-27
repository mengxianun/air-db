package com.mxy.air.db.interceptors;

import java.sql.SQLException;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.mxy.air.db.SQLUtil;

public class TransactionInterceptor implements MethodInterceptor {

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = null;
		SQLUtil.startTransaction();
		try {
			result = invocation.proceed();
			SQLUtil.commit();
		} catch (SQLException e) {
			SQLUtil.rollback();
			throw e;
		} finally {
			SQLUtil.close();
		}
		return result;
	}

}
