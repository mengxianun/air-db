package com.mxy.air.db.builder;

import java.util.List;
import java.util.Map;

import com.mxy.air.db.SQLBuilder;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

public class Insert extends SQLBuilder {

	public Insert() {
	}

	public Insert(String table, Map<String, Object> values, List<SQLBuilder> associations) {
		this.table = table;
		this.values = values;
		this.associations = associations;
		build();
	}
    
	public Insert build() {
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
