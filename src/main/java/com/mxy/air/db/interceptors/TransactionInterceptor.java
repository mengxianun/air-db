package com.mxy.air.db.interceptors;

import java.sql.SQLException;

import com.google.inject.Inject;
import com.mxy.air.db.SQLSession;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class TransactionInterceptor implements MethodInterceptor {

	@Inject
	private SQLSession sqlSession;

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result;
		sqlSession.startTransaction();
		try {
			result = invocation.proceed();
			sqlSession.commit();
		} catch (SQLException e) {
			sqlSession.rollback();
			throw e;
		} finally {
			sqlSession.close();
		}
		return result;
	}

}
