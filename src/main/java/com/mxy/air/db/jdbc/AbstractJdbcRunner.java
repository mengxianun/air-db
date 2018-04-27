package com.mxy.air.db.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.sql.DataSource;

/**
 * Jdbc辅助操作
 * 
 * @author mengxiangyun
 *
 */
public class AbstractJdbcRunner {

	private final DataSource dataSource;

	protected AbstractJdbcRunner(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	protected DataSource getDataSource() {
		return dataSource;
	}

	protected Connection getConnection() throws SQLException {
		return this.dataSource.getConnection();
	}

	protected void fillStatement(PreparedStatement stmt, Object... params) throws SQLException {
		if (params == null) {
			return;
		}
		for (int i = 0; i < params.length; i++) {
			stmt.setObject(i + 1, params[i]);
		}
	}

	/**
	 * 重新抛出一个包含更多信息的异常
	 * 
	 * @param cause
	 * @param sql
	 * @param params
	 * @throws SQLException
	 */
	protected void rethrow(SQLException cause, String sql, Object... params) throws SQLException {

		String causeMessage = cause.getMessage();
		if (causeMessage == null) {
			causeMessage = "";
		}
		StringBuffer msg = new StringBuffer(causeMessage);

		msg.append(" SQL: ");
		msg.append(sql);
		msg.append(" Parameters: ");

		if (params == null) {
			msg.append("[]");
		} else {
			msg.append(Arrays.deepToString(params));
		}

		SQLException e = new SQLException(msg.toString(), cause.getSQLState(), cause.getErrorCode());
		e.setNextException(cause);

		throw e;
	}

	protected void close(Connection conn, Statement stmt, ResultSet rs) {
		close(rs);
		close(stmt);
		close(conn);
	}

	protected void close(Statement stmt, ResultSet rs) {
		close(rs);
		close(stmt);
	}

	protected void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	protected void close(Statement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	protected void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
