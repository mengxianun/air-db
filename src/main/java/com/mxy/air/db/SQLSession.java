package com.mxy.air.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapHandler;
import com.mxy.air.db.jdbc.handlers.MapListHandler;
import com.mxy.air.db.jdbc.handlers.ObjectHandler;
import com.mxy.air.db.jdbc.trans.Atom;

/**
 * @author mengxiangyun
 */
public class SQLSession {

	private static final Logger logger = LoggerFactory.getLogger(SQLSession.class);

	private DataSource dataSource;
	// 当前线程的数据库连接, 事务操作时, 事务内的数据库操作共用同一个连接
	private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
	// 连接是否关闭, 事务操作时由外部程序控制连接的关闭, 非事务操作时由JdbcRunner关闭连接
	private static ThreadLocal<Boolean> closeConnection = new ThreadLocal<>();
	private JdbcRunner runner;

	public SQLSession(DataSource dataSource) {
		this.dataSource = dataSource;
		this.runner = new JdbcRunner(dataSource);
		closeConnection.set(true); // 默认非事务运行
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
			closeConnection.set(false);
			if (logger.isDebugEnabled()) {
				logger.debug("Start new transaction.");
			}
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
				if (logger.isDebugEnabled()) {
					logger.debug("Transaction commit.");
				}
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
				if (logger.isDebugEnabled()) {
					logger.debug("Transaction rollback.");
				}
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
				if (logger.isDebugEnabled()) {
					logger.debug("Transaction close.");
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} finally {
				connectionThreadLocal.remove();
				closeConnection.set(true);
			}
		}
	}

	/**
	 * 指定一组事务操作
	 * 
	 * @param atoms
	 * @throws SQLException
	 */
	public void trans(Atom... atoms) throws SQLException {
		if (null == atoms) {
			return;
		}
		try {
			// 开启事务
			startTransaction();
			// 执行任务
			for (Atom atom : atoms) {
				atom.run();
			}
			// 提交事务
			commit();
		} catch (Exception e) {
			// 回滚事务
			rollback();
			throw e;
		} finally {
			close();
		}
	}

	public boolean isCloseConnection() {
		return closeConnection.get() == null ? true : closeConnection.get();
	}

	@SQLLog
	public Map<String, Object> detail(String sql, Object[] params) throws SQLException {
		return runner.query(getConnection(), isCloseConnection(), sql, new MapHandler(), params);
	}

	@SQLLog
	public List<Map<String, Object>> list(String sql, Object[] params) throws SQLException {
		return runner.query(getConnection(), isCloseConnection(), sql, new MapListHandler(), params);
	}

	@SQLLog
	public long count(String sql, Object[] params) throws SQLException {
		Object result = runner.query(getConnection(), isCloseConnection(), sql, new ObjectHandler(), params);
		long count = Long.parseLong(result.toString());
		return count;
	}

	@SQLLog
	public Object insert(String sql, Object[] params) throws SQLException {
		return runner.insert(getConnection(), isCloseConnection(), sql, new ObjectHandler(), params);
	}

	@SQLLog
	public int update(String sql, Object[] params) throws SQLException {
		return runner.update(getConnection(), isCloseConnection(), sql, params);
	}

	@SQLLog
	public int delete(String sql, Object[] params) throws SQLException {
		return runner.update(getConnection(), isCloseConnection(), sql, params);
	}

}
