package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mxy.air.db.jdbc.ResultSetHandler;

/**
 * 将ResultSet转换为List
 * 
 * @author mengxiangyun
 *
 * @param <T>
 */
public abstract class AbstractListHandler<T> implements ResultSetHandler<List<T>> {

	/**
	 * 将ResultSet转换为List
	 */
	public List<T> handle(ResultSet rs) throws SQLException {
		List<T> rows = new ArrayList<>();
		while (rs.next()) {
			rows.add(handleRow(rs));
		}
		return rows;
	}

	/**
	 * 行处理器, 将一行数据转换为T对象
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	protected abstract T handleRow(ResultSet rs) throws SQLException;

}
