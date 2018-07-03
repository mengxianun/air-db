package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.mxy.air.db.AirContext;
import com.mxy.air.db.SQLBuilder;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.jdbc.Page;
import com.mxy.air.json.JSONObject;

public class Select extends SQLBuilder {

	protected String countSql;

	protected List<Object> whereParams = new ArrayList<>();

	public Select() {
		statementType = StatementType.SELECT;
	}

	public Select(String table) {
		this();
		this.table = table;
		this.alias = "t";
	}

	public Select(String table, String alias, List<Join> joins, String[] columns, List<Condition> conditions,
			String[] groups, String[] orders, long[] limit) {
		this();
		this.table = table;
		this.alias = alias;
		this.joins = joins;
		this.columns = columns;
		this.conditions = conditions;
		this.groups = groups;
		this.orders = orders;
		this.limit = limit;
	}

	public Select toBuild() {
		if (alias == null)
			alias = "";
		String aliasPrefix = Strings.isNullOrEmpty(alias) ? "" : alias + ".";
		if (db == null)
			db = AirContext.getDefaultDb();
		dialect = AirContext.getDialect(db);
		JSONObject tableConfigs = AirContext.getAllTableConfig(db);
		// 主表的配置
		JSONObject tableConfig = tableConfigs.getObject(table);
		/*
		 * 设置空对象, 防止后面调用的时候报空指针错误
		 */
		if (tableConfig == null) {
			tableConfig = new JSONObject();
		}
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		if (columnConfigs == null) {
			columnConfigs = new JSONObject();
		}
		// SQL字符串
    	StringBuilder builder = new StringBuilder();
		// 表字符串
		StringBuilder tableBuilder = new StringBuilder();
		/*
		 * 在存在一对多或多对多并且分页查询的情况下, 将基础表数据库作为子查询, 以保证分页的准确性
		 */
		boolean manyLimit = false;
		if (!isEmpty(joins)) { // 存在关联表查询
			for (Join join : joins) {
				TableConfig.Association.Type associationType = AirContext.getAssociation(db, join.getTable(),
						join.getTargetTable());
				if (associationType == TableConfig.Association.Type.ONE_TO_MANY
						|| associationType == TableConfig.Association.Type.MANY_TO_MANY) {
					if (!isEmpty(limit)) {
						manyLimit = true;
					}
				}
			}
		}
		if (manyLimit) {
			// 主表的查询条件
			List<Condition> primaryTableConditions = conditions.stream().filter(c -> c.getTable().equals(table))
					.collect(Collectors.toList());
			conditions.removeAll(primaryTableConditions);
			Select primarySelect = new Select(table, null, Collections.emptyList(), null, primaryTableConditions, null,
					null, limit);
			primarySelect.build();
			tableBuilder.append(" from (").append(primarySelect.sql()).append(") ").append(alias);
			countSql = primarySelect.getCountSql();
			whereParams = primarySelect.getWhereParams();
			params.addAll(primarySelect.params());
		} else {
			tableBuilder.append(" from ").append(table).append(" ").append(alias);
		}
		// 列字符串
		StringBuilder columnBuilder = new StringBuilder();
		// 拼接表字符串, 列字符串
		if (columns == null) { // 如果未指定列，则查询所有字段，所有字段信息从配置中获取
			columns = columnConfigs.keySet().toArray(new String[] {});
			// 主表的列
			for (String column : columns) {
				columnBuilder.append(aliasPrefix).append(column).append(",");
			}
			// 去掉最后的','分隔符
			if (columnBuilder.toString().endsWith(",")) {
				columnBuilder.deleteCharAt(columnBuilder.length() - 1);
			}
			if (!isEmpty(joins)) { // 存在关联表查询
				for (Join join : joins) {
					// 拼接table字符串
					tableBuilder.append(" ").append(join.getJoinType().text()).append(" ").append(join.getTargetTable())
							.append(" ").append(join.getTargetAlias()).append(" on ").append(join.getAlias())
							.append(".").append(join.getColumn()).append(" = ").append(join.getTargetAlias())
							.append(".").append(join.getTargetColumn());

					// join表的配置
					JSONObject joinTableConfig = tableConfigs.getObject(join.getTargetTable());
					JSONObject joinColumnConfigs = joinTableConfig.getObject(TableConfig.COLUMNS);
					for (String joinColumn : joinColumnConfigs.keySet()) {
						if (join.getTable().equals(table)) {
							columnBuilder.append(",").append(join.getTargetAlias()).append(".").append(joinColumn)
									.append(" ").append("'").append(join.getTargetTable()).append(".")
									.append(joinColumn).append("'");
						} else {
							columnBuilder.append(",").append(join.getTargetAlias()).append(".").append(joinColumn)
									.append(" ").append("'").append(join.getTable()).append(".")
									.append(join.getTargetTable()).append(".").append(joinColumn).append("'");
						}
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
			List<String> remainColumns = Lists.newArrayList(columns);
			// 遍历所有要操作的字段，排除主表的字段，剩余的就是关联表的字段
			for (String column : columns) {
				if (column.contains(".")) { // 带表别名的字段
					String[] tableColumn = column.split("\\.");
					if (tableColumn[0].equals(table)) { // 主表的字段
						columnBuilder.append(aliasPrefix).append(tableColumn[1]).append(",");
						remainColumns.remove(column); // 删除主表的字段
					}
				} else { // 未指定表别名字段
					String columnName = column;
					if (columnName.indexOf(" ") != -1) {
						columnName = column.split(" ")[0];
					}
					if (columnConfigs.containsKey(columnName)) { // 查询的列在主表的列配置中, 即表示该列是属于主表的列
						columnBuilder.append(aliasPrefix).append(column).append(","); // 拼接主表字段字符串
					} else { // 其他字段字符串, 如方法
						/*
						 *   判断查询列是否为函数, 是函数的话直接拼接, 不是的话不拼接
						 */
						columnBuilder.append(column).append(",");
					}
					remainColumns.remove(column); // 删除字段
				}
			}
			// 去掉最后的','分隔符
			if (columnBuilder.toString().endsWith(",")) {
				columnBuilder.deleteCharAt(columnBuilder.length() - 1);
			}
			// 关联表
			if (!isEmpty(joins)) {
				for (Join join : joins) {
					// 拼接table字符串
					tableBuilder.append(" ").append(join.getJoinType().text()).append(" ").append(join.getTargetTable())
							.append(" ").append(join.getTargetAlias()).append(" on ").append(join.getAlias())
							.append(".").append(join.getColumn()).append(" = ").append(join.getTargetAlias())
							.append(".").append(join.getTargetColumn());
					// join表的配置
					JSONObject joinTableConfig = tableConfigs.getObject(join.getTargetTable());
					JSONObject joinColumnConfigs = joinTableConfig.getObject(TableConfig.COLUMNS);
					// 查询字段是否已经指定了关联表的字段，如果已经指定，则什么都不做，如果没有指定，则查询所有关联表字段
					boolean specialColumn = false;
					List<String> removeJoinColumns = new ArrayList<>();
					for (String column : remainColumns) {
						if (column.contains(".")) { // 带表别名的字段
							String[] tableColumn = column.split("\\.");
							if (tableColumn[0].equals(join.getTargetTable())) { // 存在关联表的别名，代表已经指定了关联表的字段
								specialColumn = true;
								columnBuilder.append(",").append(join.getTargetAlias()).append(".")
										.append(tableColumn[1])
										.append(" ").append("'").append(join.getTargetTable()).append(".")
										.append(tableColumn[1]).append("'");
								removeJoinColumns.add(column);
							}
						} else { // 未指定表别名字段
							String columnName = column;
							if (columnName.indexOf(" ") != -1) {
								columnName = column.split(" ")[0];
							}
							// 如果主表不包含该字段，并且关联表包含该字段，则代表用户指定了关联表的字段
							if (joinColumnConfigs.containsKey(columnName)) {
								specialColumn = true;
								columnBuilder.append(",").append(join.getTargetAlias()).append(".").append(column)
										.append(" ").append("'").append(join.getTargetTable()).append(".")
										.append(columnName).append("'");
								removeJoinColumns.add(column);
								//							joinColumns.remove(columnName);
							}
						}
					}
					// 删除已经匹配到关联表的关联表字段
					remainColumns.removeAll(removeJoinColumns);
					// 未指定关联表字段，则查询所有关联表字段
					if (!specialColumn) {
						for (String joinColumn : joinColumnConfigs.keySet()) {
							if (!Strings.isNullOrEmpty(columnBuilder.toString())) {
								columnBuilder.append(",");
							}
							columnBuilder.append(join.getTargetAlias()).append(".").append(joinColumn)
									.append(" ").append("'").append(join.getTargetTable()).append(".")
									.append(joinColumn).append("'");
						}
					}
				}
			}
			/*
			 * 处理剩余的既不是主表的字段, 也不是关联表的字段, 如数据库函数
			 */
			for (String remainColumn : remainColumns) {
				if (!Strings.isNullOrEmpty(columnBuilder.toString())) {
					columnBuilder.append(",");
				}
				columnBuilder.append(remainColumn);
			}
		}

		columnString = columnBuilder.length() > 0 ? columnBuilder.toString() : "*";
		tableString = tableBuilder.toString();
		whereString = buildWhere();
		groupString = buildGroup();
		orderString = buildOrder();
		builder.append("select ");
		builder.append(columnString);
		builder.append(tableString);
		builder.append(whereString);
		builder.append(groupString);
		builder.append(orderString);
		sql = builder.toString();
		if (!isEmpty(limit) && !manyLimit) {
			countSql = count();
			whereParams = new ArrayList<>(params);
			sql = dialect.processLimit(sql);
			Object[] limitParams = dialect.processLimitParams(new Page(limit[0], limit[1]));
			params.addAll(Arrays.asList(limitParams));
		}
		return this;
    }

	public String count() {
		StringBuilder builder = new StringBuilder();
		builder.append("select count(1) from (").append("select ").append(columnString)
				.append(tableString).append(" ").append(whereString)
				.append(") origin_table");
		return builder.toString();
		//		return "select count(1) from (" + sql + ") origin_table";
	}

	public String getCountSql() {
		return countSql;
	}

	public List<Object> getWhereParams() {
		return whereParams;
	}

}
