package com.mxy.air.db.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RowProcessor的简单实现
 * 
 * @author mengxiangyun
 *
 */
public class BasicRowProcessor implements RowProcessor {

	public Object[] toArray(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int count = meta.getColumnCount();
		Object[] result = new Object[count];
		for (int i = 0; i < count; i++) {
			result[i] = rs.getObject(i + 1);
		}
		return result;
	}

	public Map<String, Object> toMap(ResultSet rs) throws SQLException {
		Map<String, Object> result = new LinkedHashMap<>();
		ResultSetMetaData meta = rs.getMetaData();
		int count = meta.getColumnCount();
		for (int i = 1; i <= count; i++) {
			String columnName = meta.getColumnLabel(i);
			if (columnName == null) {
				columnName = meta.getColumnName(i);
			}
			result.put(columnName, rs.getObject(i));
		}
		return result;
	}

}
