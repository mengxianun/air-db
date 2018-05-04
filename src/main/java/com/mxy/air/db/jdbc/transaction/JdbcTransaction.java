package com.mxy.air.db.jdbc.transaction;

import com.mxy.air.db.jdbc.Transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author mengxiangyun
 */
public class JdbcTransaction implements Transaction {

	private DataSource dataSource;
	private Connection connection;
	private boolean closeConnection;

	@Override
	public Connection getConnection() throws SQLException {
		if (connection == null) {
			connection = dataSource.getConnection();
		}
		return connection;
	}

	@Override
	public void commit() throws SQLException {
		connection.commit();
	}

	@Override
	public void rollback() throws SQLException {
		connection.rollback();
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}
}
