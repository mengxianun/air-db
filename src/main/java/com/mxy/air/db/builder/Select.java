package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.jdbc.Page;
import com.mxy.air.json.JSONObject;

public class Select extends SQLBuilder {

	private String countSql;

	public Select() {
		statementType = StatementType.SELECT;
	}

	public Select(String table) {
		this();
		this.table = table;
		this.alias = DEFAULT_ALIAS;
	}

	public Select(String table, String alias, List<Join> joins, String[] columns, String where, List<Object> params,
			String[] groups, String[] orders,
			long[] limit) {
		this();
		this.table = table;
		this.alias = alias;
		this.joins = joins;
		this.columns = columns;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
		this.groups = groups;
		this.orders = orders;
		this.limit = limit;
	}

	public Select build() {
		// 主表的配置
		JSONObject tableConfig = tableConfigs.getObject(table);
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		// SQL字符串
    	StringBuilder builder = new StringBuilder();
		// 表字符串
		StringBuilder tableString = new StringBuilder();
		tableString.append(" from ").append(table).append(" ").append(alias);
		// 列字符串
		StringBuilder columnString = new StringBuilder();
		// 拼接表字符串, 列字符串
		if (columns == null) { // 如果未指定列，则查询所有字段，所有字段信息从配置中获取
			columns = columnConfigs.keySet().toArray(new String[] {});
			// 主表的列
			for (String column : columns) {
				columnString.append(alias).append(".").append(column).append(",");
			}
			// 去掉最后的','分隔符
			if (columnString.toString().endsWith(",")) {
				columnString.deleteCharAt(columnString.length() - 1);
			}
			if (!isEmpty(joins)) { // 存在关联表查询
				for (Join join : joins) {
					// 拼接table字符串
					tableString.append(" ").append(join.getJoinType().text()).append(" ").append(join.getTargetTable())
							.append(" ").append(join.getTargetAlias()).append(" on ").append(join.getAlias())
							.append(".").append(join.getColumn()).append(" = ").append(join.getTargetAlias())
							.append(".").append(join.getTargetColumn());
					// join表的配置
					JSONObject joinTableConfig = tableConfigs.getObject(join.getTargetTable());
					JSONObject joinColumnConfigs = joinTableConfig.getObject(TableConfig.COLUMNS);
					for (String joinColumn : joinColumnConfigs.keySet()) {
						columnString.append(",").append(join.getTargetAlias()).append(".").append(joinColumn)
								.append(" ").append("'").append(join.getTargetTable()).append(".").append(joinColumn)
								.append("'");
					}
				}
			}
		} else { // 指定了操作的列
			/*
			 * 1. 先获取所有指定要查询的字段, 即查询列columns
			 * 2. 遍历columns, 拼接主表的字段字符串, 并过滤出关联表的字段
			 * 3. 遍历关联表, 拼接表字符串, 并确定关联表字段对应的表
			 * 4. 判断用户是否指定了关联表的字段. 如果某个关联表没有指定要查询的字段, 则查询该关联表的所有字段, 并构造成嵌套对象的形式.
			 *    如果用户指定了要查询的关联表的字段, 则只查询该关联表的指定字段
			 */
			// 所有要查询指定的关联表的字段
			List<String> joinColumns = Lists.newArrayList(columns);
			// 遍历所有要操作的字段，排除主表的字段，剩余的就是关联表的字段
			for (String column : columns) {
				if (column.contains(".")) { // 带表别名的字段
					String[] tableAlias = column.split("\\.");
					if (tableAlias[0].equals(alias)) { // 主表的字段
						columnString.append(column).append(","); // 拼接主表字段字符串
						joinColumns.remove(column); // 删除主表的字段
					}
				} else { // 未指定表别名字段
					String columnName = column;
					if (columnName.indexOf(" ") != -1) {
						columnName = column.split(" ")[0];
					}
					if (columnConfigs.containsKey(columnName)) { // 查询的列在主表的列配置中, 即表示该列是属于主表的列
						columnString.append(alias).append(".").append(column).append(","); // 拼接主表字段字符串
						joinColumns.remove(column); // 删除主表的字段
					}
				}
			}
			// 去掉最后的','分隔符
			if (columnString.toString().endsWith(",")) {
				columnString.deleteCharAt(columnString.length() - 1);
			}
			// 关联表
			if (!isEmpty(joins)) {
				for (Join join : joins) {
					// 拼接table字符串
					tableString.append(" ").append(join.getJoinType().text()).append(" ").append(join.getTargetTable())
							.append(" ").append(join.getTargetAlias()).append(" on ").append(join.getAlias())
							.append(".").append(join.getColumn()).append(" = ").append(join.getTargetAlias())
							.append(".").append(join.getTargetColumn());
					// join表的配置
					JSONObject joinTableConfig = tableConfigs.getObject(join.getTargetTable());
					JSONObject joinColumnConfigs = joinTableConfig.getObject(TableConfig.COLUMNS);
					// 查询字段是否已经指定了关联表的字段，如果已经指定，则什么都不做，如果没有指定，则查询所有关联表字段
					boolean specialColumn = false;
					List<String> removeJoinColumns = new ArrayList<>();
					for (String column : joinColumns) {
						if (column.contains(".")) { // 带表别名的字段
							String[] tableAlias = column.split("\\.");
							if (tableAlias[0].equals(join.getTargetAlias())) { // 存在关联表的别名，代表已经指定了关联表的字段
								specialColumn = true;
								columnString.append(",").append(column)
										.append(" ").append("'").append(join.getTargetTable()).append(".")
										.append(tableAlias[1]).append("'");
								removeJoinColumns.add(column);
								//							joinColumns.remove(tableAlias[1]);
							}
						} else { // 未指定表别名字段
							String columnName = column;
							if (columnName.indexOf(" ") != -1) {
								columnName = column.split(" ")[0];
							}
							// 如果主表不包含该字段，并且关联表包含该字段，则代表用户指定了关联表的字段
							if (joinColumnConfigs.containsKey(columnName)) {
								specialColumn = true;
								columnString.append(",").append(join.getTargetAlias()).append(".").append(column)
										.append(" ").append("'").append(join.getTargetTable()).append(".")
										.append(columnName).append("'");
								removeJoinColumns.add(column);
								//							joinColumns.remove(columnName);
							}
						}
					}
					// 删除已经匹配到关联表的关联表字段
					joinColumns.removeAll(removeJoinColumns);
					// 未指定关联表字段，则查询所有关联表字段
					if (!specialColumn) {
						for (String joinColumn : joinColumnConfigs.keySet()) {
							columnString.append(",").append(join.getTargetAlias()).append(".").append(joinColumn)
									.append(" ").append("'").append(join.getTargetTable()).append(".")
									.append(joinColumn).append("'");
						}
					}
				}
			}
		}

		builder.append("select ").append(columnString).append(tableString);
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
