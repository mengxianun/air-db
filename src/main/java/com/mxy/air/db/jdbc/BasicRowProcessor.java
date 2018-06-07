package com.mxy.air.db.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
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

	@SuppressWarnings("unchecked")
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
			if (columnName.indexOf(".") != -1) { // 列名包含'.'字符, 代表这是一个关联查询, 构建一个Map对象保存关联表属性, key为表名, value为列名:列值的键值对
				String[] associationInfo = columnName.split("\\.");
				String associationTableName = associationInfo[0];
				String associationColumnName = associationInfo[1];
				Map<String, Object> columns = result.containsKey(associationTableName)
						? (Map<String, Object>) result.get(associationTableName)
						: new HashMap<>();
				columns.put(associationColumnName, columnValue);
				result.put(associationTableName, columns);
			} else {
				result.put(columnName, columnValue);
			}
		}
		return result;
	}

}
