package com.mxy.air.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapHandler;
import com.mxy.air.db.jdbc.handlers.MapListHandler;
import com.mxy.air.db.jdbc.handlers.ObjectHandler;
import com.google.inject.Inject;

@SQLLog
public class SQLRunnerImpl implements SQLRunner {

	@Inject
	private JdbcRunner runner;

	public Map<String, Object> detail(String sql, Object[] params) throws SQLException {
		return runner.query(SQLUtil.getConnection(), sql, new MapHandler(), params);
	}

	public List<Map<String, Object>> list(String sql, Object[] params) throws SQLException {
		return runner.query(SQLUtil.getConnection(), sql, new MapListHandler(), params);
	}

	public long count(String sql, Object[] params) throws SQLException {
		Object result = runner.query(SQLUtil.getConnection(), sql, new ObjectHandler(), params);
		long count = Long.parseLong(result.toString());
		return count;
	}

	public Object insert(String sql, Object[] params) throws SQLException {
		return runner.insert(SQLUtil.getConnection(), sql, new ObjectHandler(), params);
	}

	public int update(String sql, Object[] params) throws SQLException {
		return runner.update(SQLUtil.getConnection(), sql, params);
	}

	public int delete(String sql, Object[] params) throws SQLException {
		return runner.update(SQLUtil.getConnection(), sql, params);
	}

}
