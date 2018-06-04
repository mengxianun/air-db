package com.mxy.air.db.builder;

import java.util.Arrays;
import java.util.List;

import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.jdbc.Page;

public class Select extends SQLBuilder {

	private String countSql;

	public Select() {
		statementType = StatementType.SELECT;
	}

	public Select(String table) {
		this();
		this.table = table;
	}

	public Select(String table, List<Join> joins, String[] columns, String where, List<Object> params,
			String[] groups, String[] orders,
			long[] limit) {
		this();
		this.table = table;
		this.joins = joins;
		this.columns = columns;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
		this.groups = groups;
		this.orders = orders;
		this.limit = limit;
		build();
	}

	public Select build() {
    	StringBuilder builder = new StringBuilder();
		String columnString = columns == null ? "*" : String.join(",", columns);
		builder.append("select ").append(columnString).append(" from ").append(table);
		if (!isEmpty(joins)) {
			for (Join join : joins) {
				builder.append(" ").append(join.getJoinType().text()).append(" ").append(join.getTargetTable())
						.append(" on ").append(table).append(".").append(join.getColumn()).append(" = ")
						.append(join.getTargetTable()).append(".").append(join.getTargetColumn());
			}
		}
		if (!isEmpty(where)) {
			builder.append(" where ").append(where);
		}
		if (!isEmpty(groups)) {
			builder.append(" group by ").append(String.join(",", groups));
		}
		if (!isEmpty(orders)) {
			builder.append(" order by ").append(String.join(",", orders));
		}
		sql = builder.toString();
		if (!isEmpty(limit)) {
			countSql = count(sql);
			sql = SQLBuilder.dialect.processLimit(sql);
			Object[] limitParams = SQLBuilder.dialect.processLimitParams(new Page(limit[0], limit[1]));
			params.addAll(Arrays.asList(limitParams));
		}
		return this;
    }

	public String count(String sql) {
		return "select count(1) from (" + sql + ") t";
	}

	public String getCountSql() {
		return countSql;
	}

}
