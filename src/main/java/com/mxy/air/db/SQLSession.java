package com.mxy.air.db;

import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapHandler;
import com.mxy.air.db.jdbc.handlers.MapListHandler;
import com.mxy.air.db.jdbc.handlers.ObjectHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author mengxiangyun
 */
@SQLLog
public class SQLSession {

	private DataSource dataSource;
	// 当前线程的数据库连接, 事务操作时, 事务内的数据库操作共用同一个连接
	private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
	// 连接是否关闭, 事务操作时由外部程序控制连接的关闭, 非事务操作时由JdbcRunner关闭连接
	private boolean closeConnnection;
	private JdbcRunner runner;

	public SQLSession(DataSource dataSource) {
		this.dataSource = dataSource;
		this.runner = new JdbcRunner(dataSource);
		this.closeConnnection = true; // 默认非事务运行
	}

	public void startTransaction() {
		Connection conn = connectionThreadLocal.get();
		if (conn == null) {
			try {
				conn = dataSource.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			connectionThreadLocal.set(conn);
		}
		try {
			conn.setAutoCommit(false);
			closeConnnection = false;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取数据库连接, 先从当前线程中获取, 如果没有的话从DataSource获取一个新的数据库连接
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection conn = connectionThreadLocal.get();
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

	public void commit() throws SQLException {
		Connection conn = connectionThreadLocal.get();
		if (conn != null) {
			try {
				conn.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void rollback() throws SQLException {
		Connection conn = connectionThreadLocal.get();
		if (conn != null) {
			try {
				conn.rollback();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void close() {
		Connection conn = connectionThreadLocal.get();
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} finally {
				connectionThreadLocal.remove();
				closeConnnection = true;
			}
		}
	}

	public Map<String, Object> detail(String sql, Object[] params) throws SQLException {
		return runner.query(getConnection(), closeConnnection, sql, new MapHandler(), params);
	}

	public List<Map<String, Object>> list(String sql, Object[] params) throws SQLException {
		return runner.query(getConnection(), closeConnnection, sql, new MapListHandler(), params);
	}

	public long count(String sql, Object[] params) throws SQLException {
		Object result = runner.query(getConnection(), closeConnnection, sql, new ObjectHandler(), params);
		long count = Long.parseLong(result.toString());
		return count;
	}

	public Object insert(String sql, Object[] params) throws SQLException {
		return runner.insert(getConnection(), closeConnnection, sql, new ObjectHandler(), params);
	}

	public int update(String sql, Object[] params) throws SQLException {
		return runner.update(getConnection(), closeConnnection, sql, params);
	}

	public int delete(String sql, Object[] params) throws SQLException {
		return runner.update(getConnection(), closeConnnection, sql, params);
	}

}
