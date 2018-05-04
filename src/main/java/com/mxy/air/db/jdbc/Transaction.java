package com.mxy.air.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author mengxiangyun
 */
public interface Transaction {

	public Connection getConnection() throws SQLException;

	public void commit() throws SQLException;

	public void rollback() throws SQLException;

	public void close() throws SQLException;

}
