package com.mxy.air.db.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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
			if (null == columnName || 0 == columnName.length()) {
				columnName = meta.getColumnName(i);
			}
			Object columnValue;
			int columnType = meta.getColumnType(i);
			switch (columnType) {
			// 对BIT, TINYINT特殊处理, 避免返回布尔类型true或false, 而是返回数字类型
			case Types.BIT:
			case Types.TINYINT:
				columnValue = rs.getByte(i);
				break;

			default:
				columnValue = rs.getObject(i);
				break;
			}
			result.put(columnName, columnValue);
		}
		return result;
	}

}
