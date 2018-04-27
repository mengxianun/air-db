package com.mxy.air.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/*
 * SQL事务操作工具类
 */
public class SQLUtil {

	/*
	 * 当前线程数据库连接
	 */
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<>();

	/*
	 * 数据源
	 */
	private static DataSource dataSource;

	public static void setDataSource(DataSource dataSource) {
		SQLUtil.dataSource = dataSource;
	}

	public static void startTransaction() {
		Connection conn = threadLocal.get();
		if (conn == null) {
			try {
				conn = dataSource.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			threadLocal.set(conn);
		}
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void commit() {
		Connection conn = threadLocal.get();
		if (conn != null) {
			try {
				conn.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void rollback() {
		Connection conn = threadLocal.get();
		if (conn != null) {
			try {
				conn.rollback();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void close() {
		Connection conn = threadLocal.get();
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} finally {
				threadLocal.remove();
			}
		}
	}

	public static Connection getConnection() {
		Connection conn = threadLocal.get();
		if (conn == null) {
			try {
				return dataSource.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		} else {
			return conn;
		}
	}

}
