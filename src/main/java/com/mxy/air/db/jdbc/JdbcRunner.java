package com.mxy.air.db.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

/**
 * 执行SQL语句并返回处理结果
 * 
 * @author mengxiangyun
 *
 */
public class JdbcRunner extends AbstractJdbcRunner {

	public JdbcRunner(DataSource dataSource) {
		super(dataSource);
	}

	/**
	 * 执行查询SQL, 返回第一行结果, 没有替换参数. 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(String sql, ResultSetHandler<T> handler) throws SQLException {
		return query(getConnection(), true, sql, handler);
	}

	/**
	 * 执行查询SQL, 返回第一行结果, 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(String sql, ResultSetHandler<T> handler, Object... params) throws SQLException {
		return query(getConnection(), true, sql, handler, params);
	}

	/**
	 * 执行查询SQL, 返回第一行结果, 没有替换参数. 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> handler) throws SQLException {
		return query(conn, false, sql, handler);
	}

	/**
	 * 执行查询SQL, 返回第一行结果, 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> handler, Object... params) throws SQLException {
		return query(conn, false, sql, handler, params);
	}

	/**
	 * 执行查询SQL, 返回第一行结果
	 * 
	 * @param conn
	 *            数据库连接
	 * @param closeConn
	 *            是否关闭连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T query(Connection conn, boolean closeConn, String sql, ResultSetHandler<T> handler, Object... params)
			throws SQLException {
		T result = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sql);
			fillStatement(stmt, params);
			rs = stmt.executeQuery();
			result = handler.handle(rs);
		} catch (SQLException e) {
			throw e;
//			throw new SQLException(e.getCause());
			//			this.rethrow(e, sql, params);
		} finally {
			close(stmt, rs);
			if (closeConn)
				close(conn);
		}
		return result;
	}

	/**
	 * 执行更新SQL, 没有替换参数. 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @return
	 * @throws SQLException
	 */
	public int update(String sql) throws SQLException {
		return update(getConnection(), true, sql);
	}

	/**
	 * 执行更新SQL, 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public int update(String sql, Object... params) throws SQLException {
		return update(getConnection(), true, sql, params);
	}

	/**
	 * 执行更新sql, 没有替换参数. 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @return
	 * @throws SQLException
	 */
	public int update(Connection conn, String sql) throws SQLException {
		return update(conn, false, sql);
	}

	/**
	 * 执行更新sql, 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param closeConn
	 *            是否关闭连接
	 * @param sql
	 *            执行的sql
	 * @return
	 * @throws SQLException
	 */
	public int update(Connection conn, String sql, Object... params) throws SQLException {
		return update(conn, false, sql, params);
	}

	/**
	 * 执行更新SQL
	 * 
	 * @param conn
	 *            数据库连接
	 * @param closeConn
	 *            是否关闭连接
	 * @param sql
	 *            执行的sql
	 * @param params
	 *            sql参数
	 * @return 更新的行数
	 * @throws SQLException
	 */
	public int update(Connection conn, boolean closeConn, String sql, Object... params) throws SQLException {
		int count = 0;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql);
			fillStatement(stmt, params);
			count = stmt.executeUpdate();
		} catch (SQLException e) {
			this.rethrow(e, sql, params);
		} finally {
			close(stmt);
			if (closeConn)
				close(conn);
		}
		return count;
	}

	/**
	 * 执行插入SQL, 没有替换参数. 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @return
	 * @throws SQLException
	 */
	public <T> T insert(String sql, ResultSetHandler<T> handler) throws SQLException {
		return insert(getConnection(), true, sql, handler);
	}

	/**
	 * 执行插入SQL, 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T insert(String sql, ResultSetHandler<T> handler, Object... params) throws SQLException {
		return insert(getConnection(), true, sql, handler, params);
	}

	/**
	 * 执行插入SQL, 没有替换参数. 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @return
	 * @throws SQLException
	 */
	public <T> T insert(Connection conn, String sql, ResultSetHandler<T> handler) throws SQLException {
		return insert(conn, false, sql, handler);
	}

	/**
	 * 执行插入SQL, 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T insert(Connection conn, String sql, ResultSetHandler<T> handler, Object... params)
			throws SQLException {
		return insert(conn, false, sql, handler, params);
	}

	/**
	 * 执行插入SQL
	 * 
	 * @param conn
	 *            数据库连接
	 * @param closeConn
	 *            是否关闭连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return 插入数据的主键
	 * @throws SQLException
	 */
	public <T> T insert(Connection conn, boolean closeConn, String sql, ResultSetHandler<T> handler,
			Object... params) throws SQLException {
		T result = null;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			fillStatement(stmt, params);
			stmt.executeUpdate();
			ResultSet rs = stmt.getGeneratedKeys();
			result = handler.handle(rs);
		} catch (SQLException e) {
			this.rethrow(e, sql, params);
		} finally {
			close(stmt);
			if (closeConn)
				close(conn);
		}
		return result;
	}

	public int[] batch(String sql, Object[][] params) throws SQLException {
		return this.batch(getConnection(), true, sql, params);
	}

	public int[] batch(Connection conn, String sql, Object[][] params) throws SQLException {
		return this.batch(conn, false, sql, params);
	}

	public int[] batch(Connection conn, boolean closeConn, String sql, Object[][] params) throws SQLException {
		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			if (closeConn) {
				close(conn);
			}
			throw new SQLException("Null SQL statement");
		}

		if (params == null) {
			if (closeConn) {
				close(conn);
			}
			throw new SQLException("Null parameters. If parameters aren't need, pass an empty array.");
		}

		PreparedStatement stmt = null;
		int[] rows = null;
		try {
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			for (int i = 0; i < params.length; i++) {
				this.fillStatement(stmt, params[i]);
				stmt.addBatch();
			}
			rows = stmt.executeBatch();

		} catch (SQLException e) {
			this.rethrow(e, sql, (Object[]) params);
		} finally {
			close(stmt);
			if (closeConn) {
				close(conn);
			}
		}

		return rows;
	}

	/**
	 * 执行批量插入SQL, 从数据源中获取连接, 操作完成关闭数据库连接
	 * 
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T insertBatch(String sql, ResultSetHandler<T> handler, Object[][] params) throws SQLException {
		return insertBatch(getConnection(), true, sql, handler, params);
	}

	/**
	 * 执行批量插入SQL, 数据库连接由调用者关闭
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	public <T> T insertBatch(Connection conn, String sql, ResultSetHandler<T> handler, Object[][] params)
			throws SQLException {
		return insertBatch(conn, false, sql, handler, params);
	}

	/**
	 * 执行批量插入SQL
	 * 
	 * @param conn
	 *            数据库连接
	 * @param closeConn
	 *            是否关闭连接
	 * @param sql
	 *            执行的sql
	 * @param handler
	 *            结果集处理器
	 * @param params
	 *            sql参数
	 * @return
	 * @throws SQLException
	 */
	private <T> T insertBatch(Connection conn, boolean closeConn, String sql, ResultSetHandler<T> handler,
			Object[][] params) throws SQLException {
		T result = null;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			for (int i = 0; i < params.length; i++) {
				fillStatement(stmt, params[i]);
				stmt.addBatch();
			}
			stmt.executeBatch();
			ResultSet rs = stmt.getGeneratedKeys();
			result = handler.handle(rs);
		} catch (SQLException e) {
			this.rethrow(e, sql, (Object[]) params);
		} finally {
			close(stmt);
			if (closeConn)
				close(conn);
		}
		return result;
	}

}
