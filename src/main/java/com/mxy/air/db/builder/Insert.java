package com.mxy.air.db.builder;

import java.util.Map;

import com.mxy.air.db.DbException;
import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

public class Insert extends SQLBuilder {

	public Insert() {
		statementType = StatementType.INSERT;
	}

	public Insert(String table, Map<String, Object> values) {
		this();
		this.table = table;
		this.values = values;
	}
    
	public Insert build() {
		// 配置
		JSONObject tableConfig = tableConfigs.getObject(table);
		if (tableConfig == null) {
			throw new DbException(String.format("数据库表[%s]不存在", table));
		}
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		StringBuilder builder = new StringBuilder();
		builder.append("insert into ").append(table);
		StringBuilder columnBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		boolean comma = false;
		params.clear();
		// 循环表的列
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			// 如果字段不是数据库表中的字段, 就跳过
			if (!columnConfigs.containsKey(column)) {
				continue;
			}
			if (comma) {
				columnBuilder.append(", ");
				valueBuilder.append(", ");
			}
			columnBuilder.append(column);
			valueBuilder.append("?");
			if (value instanceof JSONObject || value instanceof JSONArray) {
				value = value.toString();
			}
			params.add(value);
			comma = true;
		}
		builder.append("(").append(columnBuilder).append(")").append(" values(").append(valueBuilder).append(")");
		sql = builder.toString();
		return this;
    }

}
