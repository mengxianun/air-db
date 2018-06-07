package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mxy.air.db.DbException;
import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONObject;

public class Update extends SQLBuilder {

	public Update() {
		statementType = StatementType.UPDATE;
	}

	public Update(String table, Map<String, Object> values, String where, List<Object> params) {
		this();
		this.table = table;
		this.values = values;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
	}
    
	public Update build() {
		// 配置
		JSONObject tableConfig = tableConfigs.getObject(table);
		if (tableConfig == null) {
			throw new DbException(String.format("数据库表[%s]不存在", table));
		}
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		StringBuilder builder = new StringBuilder();
		builder.append("update ").append(table).append(" set ");
		StringBuilder columnBuilder = new StringBuilder();
		List<Object> updateParams = new ArrayList<>();
		boolean comma = false;
		for (Entry<String, Object> entry : values.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			// 如果字段不是数据库表中的字段, 就跳过
			if (!columnConfigs.containsKey(column)) {
				continue;
			}
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
