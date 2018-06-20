package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.mxy.air.db.AirContext;
import com.mxy.air.db.DbException;
import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONObject;

public class Update extends SQLBuilder {

	public Update() {
		statementType = StatementType.UPDATE;
	}

	public Update(String table, String alias, Map<String, Object> values, List<Condition> conditions) {
		this();
		this.table = table;
		this.alias = alias;
		this.values = values;
		this.conditions = conditions;
	}
    
	public Update toBuild() {
		String aliasPrefix = Strings.isNullOrEmpty(alias) ? "" : alias + ".";
		if (db == null)
			db = AirContext.getDefaultDb();
		dialect = AirContext.getDialect(db);
		// 配置
		JSONObject tableConfig = AirContext.getTableConfig(db, table);
		if (tableConfig == null) {
			throw new DbException(String.format("数据库表[%s]不存在", table));
		}
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		StringBuilder builder = new StringBuilder();
		builder.append("update ").append(table).append(" ").append(alias).append(" set ");
		boolean comma = false;
		for (Entry<String, Object> entry : values.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			// 如果字段不是数据库表中的字段, 就跳过
			if (!columnConfigs.containsKey(column)) {
				continue;
			}
			if (comma) {
				builder.append(", ");
			}
			builder.append(aliasPrefix).append(column).append(" = ").append("?");
			// 关键字处理
			params.add(value);
			comma = true;
		}
		// Where
		builder.append(buildWhere());
		sql = builder.toString();
		return this;
    }

	public List<Object> whereParams() {
		List<Object> whereParams = new ArrayList<>();
		for (Object value : values.values()) {
			if (!params.contains(value)) {
				whereParams.add(value);
			}
		}
		return whereParams;
	}

}
