package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mxy.air.db.SQLBuilder;

public class Update extends SQLBuilder {

	public Update() {
	}

	public Update(String table, Map<String, Object> values, String where, List<Object> params) {
		this.table = table;
		this.values = values;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
		build();
	}
    
	public Update build() {
		StringBuilder builder = new StringBuilder();
		builder.append("update ").append(table).append(" set ");
		StringBuilder columnBuilder = new StringBuilder();
		List<Object> updateParams = new ArrayList<>();
		boolean comma = false;
		for (Entry<String, Object> entry : values.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			if (comma) {
				columnBuilder.append(", ");
			}
			columnBuilder.append(column).append(" = ").append("?");
			// 关键字处理
			updateParams.add(value);
			comma = true;
		}
		builder.append(columnBuilder);
		// 将列参数排在where参数前面
		params = new ArrayList<>(updateParams);
		params.addAll(whereParams);
		if (!isEmpty(where)) {
			builder.append(" where ").append(where);
		}
		sql = builder.toString();
		return this;
    }

}
