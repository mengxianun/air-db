package com.mxy.air.db.builder.es;

import java.util.Date;
import java.util.List;

import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Select;

public class EsSelect extends Select {

	public EsSelect(String table, String alias, List<Join> joins, String[] columns, List<Condition> conditions,
			String[] groups, String[] orders, long[] limit) {
		super(table, alias, joins, columns, conditions, groups, orders, limit);
	}

	@Override
	public SQLBuilder nativeSQL() {
		if (!isEmpty(limit)) {
			countSql = nativeSQL(countSql, whereParams.toArray());
		}
		sql = nativeSQL(sql, params.toArray());
		params.clear();
		whereParams.clear();
		conditions.clear();
		return this;
	}

	public String nativeSQL(String sql, Object[] params) {
		int cols = params.length;
		Object[] values = new Object[cols];
		System.arraycopy(params, 0, values, 0, cols);
		for (int i = 0; i < cols; i++) {
			Object value = values[i];
			if (value instanceof Date) {
				values[i] = "'" + value + "'";
			} else if (value instanceof String) {
				values[i] = "'" + value + "'";
			} else if (value instanceof Boolean) {
				values[i] = (Boolean) value ? 1 : 0;
			}
		}
		return String.format(sql.replaceAll("\\?", "%s"), values);
	}

	@Override
	public String count() {
		StringBuilder builder = new StringBuilder();
		builder.append("select").append(" count(*) ").append(tableString).append(" ").append(whereString);
		return builder.toString();
	}


}
