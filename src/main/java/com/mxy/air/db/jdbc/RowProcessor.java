package com.mxy.air.db.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * 将ResultSet行转换为其他对象
 * 
 * @author mengxiangyun
 *
 */
public interface RowProcessor {

	/**
	 * 将ResultSet行中的列值转换成一个对象数组, 在将ResultSet传递给该方法之前, 应该放置在一个有效的行上
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public Object[] toArray(ResultSet rs) throws SQLException;

	/**
	 * 将ResultSet行中的列值转换成一个Map对象, 在将ResultSet传递给该方法之前, 应该放置在一个有效的行上
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> toMap(ResultSet rs) throws SQLException;

}
