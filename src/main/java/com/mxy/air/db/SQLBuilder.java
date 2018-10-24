package com.mxy.air.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Strings;
import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Delete;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Select;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.config.TableConfig.Association;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.jdbc.Dialect;
import com.mxy.air.json.JSONObject;

/**
 * SQL构造器
 * 
 * @author mengxiangyun
 *
 */
public abstract class SQLBuilder {
    
	protected enum StatementType {
		SELECT, INSERT, UPDATE, DELETE
	}

	// SQL操作类型
	protected StatementType statementType;

	protected String db;

	// 数据库表
	protected String table;

	// 数据库表别名
	protected String alias;

	// 关联查询的表
	protected List<Join> joins;

	// 要操作的数据库表的列
	protected String[] columns;

	// where查询条件
    protected String where;

	// group分组字段
	protected String[] groups;

	// order排序字段
	protected String[] orders;
	// 分页参数: [start, end]
	protected long[] limit;
	// 插入或更新数据
	protected Map<String, Object> values;

	// SQL语句
    protected String sql;

	// SQL语句参数，该属性包含where条件参数
	protected List<Object> params = new ArrayList<>();

	// 条件
	protected List<Condition> conditions = new ArrayList<>();

	// 表别名, key为表名, value为表别名
	protected Map<String, String> aliases = new HashMap<>();

	// 方言
	protected Dialect dialect;

	/*
	 * 拼接后的字符串
	 */
	protected String columnString;

	protected String tableString;

	protected String whereString;

	protected String groupString;

	protected String orderString;

	public static Select select(String table) {
		return new Select(table);
	}

	public static Select select(String table, String alias, List<Join> joins, String[] columns,
			List<Condition> conditions, String[] groups,
			String[] orders,
			long[] limit) {
		return new Select(table, alias, joins, columns, conditions, groups, orders, limit);
    }

	public static Insert insert(String table, Map<String, Object> values) {
		return new Insert(table, values);
	}

	public static Update update(String table, String alias, Map<String, Object> values, List<Condition> conditions) {
		return new Update(table, alias, values, conditions);
    }

	public static Delete delete(String table, String alias, List<Condition> conditions) {
		return new Delete(table, alias, conditions);
    }
    
	protected SQLBuilder build() {
		if (Strings.isNullOrEmpty(table)) {
			return this;
		}
		/*
		 * 初始清空构建数据, 防止再次构建时有脏数据
		 */
		clear();
		return toBuild();
	}

	/**
	 * 清空数据
	 */
	protected void clear() {
		sql = null;
		params.clear();
		conditions.forEach(c -> c.getValues().clear());
		columnString = null;
		tableString = null;
		whereString = null;
		groupString = null;
		orderString = null;
	}

	protected abstract SQLBuilder toBuild();

	protected SQLBuilder nativeSQL() {
		return this;
	};

	public StatementType geStatementType() {
		return statementType;
	}

	protected String buildWhere() {
		StringBuilder builder = new StringBuilder();
		if (!conditions.isEmpty()) {
			builder.append(" where");
			boolean first = true;
			for (Condition condition : conditions) {
				String conditionSql = condition.sql();
				if (first) {
					if (conditionSql.startsWith(Operator.AND.sql())) {
						conditionSql = conditionSql.substring(4);
					} else if (conditionSql.startsWith(Operator.OR.sql())) {
						conditionSql = conditionSql.substring(3);
					}
				}
				builder.append(" ").append(conditionSql);
				condition.getValues().forEach(params::add);
				first = false;
			}
		}
		return builder.toString();
	}

	protected String buildGroup() {
		StringBuilder builder = new StringBuilder();
		if (!isEmpty(groups)) {
			builder.append(" group by ").append(String.join(",", groups));
		}
		return builder.toString();
	}

	protected String buildOrder() {
		StringBuilder builder = new StringBuilder();
		if (!isEmpty(orders)) {
			builder.append(" order by ").append(String.join(",", orders));
		}
		return builder.toString();
	}
    
	public boolean isEmpty(final CharSequence cs) {
		return cs == null || cs.length() == 0;
	}

	public boolean isEmpty(final String[] objects) {
		return objects == null || objects.length == 0;
	}

	public boolean isEmpty(final long[] objects) {
		return objects == null || objects.length == 0;
	}

	public boolean isEmpty(final Collection<?> c) {
		return c == null || c.isEmpty();
	}

    public boolean isNumeric(final CharSequence cs) {
        if (isEmpty(cs)) {
            return false;
        }
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isDigit(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

	public static String getRandomString(int length) {
		//		String base = "abcdefghijklmnopqrstuvwxyz0123456789";
		String base = "abcdefghijklmnopqrstuvwxyz";
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			int number = random.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();
	}

	public SQLBuilder equal(String column, Object value) {
		where = isEmpty(where) ? "" : where + " and ";
		where += column + "= ?";
		params.add(value);
		return this;
	}

	public SQLBuilder notEqual(String column, Object value) {
		where = isEmpty(where) ? "" : where + " and ";
		where += column + "<> ?";
		params.add(value);
		return this;
	}

	/**
	 * 所有条件反向
	 */
	public SQLBuilder reverse() {
		if (!isEmpty(where)) {
			where = "not " + where.replaceAll(" and ", " and not ");
		}
		return this;
	}

	public String db() {
		return db;
	}

	public SQLBuilder db(String db) {
		this.db = db;
		return this;
	}

	public String table() {
		return table;
	}

	public SQLBuilder table(String table) {
		this.table = table;
		return this;
	}

	public List<Join> joins() {
		return joins;
	}

	public SQLBuilder joins(List<Join> joins) {
		this.joins = joins;
		return this;
	}

	public String[] columns() {
		return columns;
	}

	public SQLBuilder columns(String[] columns) {
		this.columns = columns;
		return this;
	}

	public String where() {
		return where;
	}

	public SQLBuilder where(String where) {
		this.where = where;
		return this;
	}

	public String[] groups() {
		return groups;
	}

	public SQLBuilder groups(String[] groups) {
		this.groups = groups;
		return this;
	}

	public String[] orders() {
		return orders;
	}

	public SQLBuilder orders(String[] orders) {
		this.orders = orders;
		return this;
	}

	public long[] limit() {
		return limit;
	}

	public SQLBuilder limit(long[] limit) {
		this.limit = limit;
		return this;
	}

	public Map<String, Object> values() {
		return values;
	}

	public SQLBuilder value(String column, Object value) {
		this.values.put(column, value);
		// 插入或更新, SQL参数由values重新构成
		this.params.clear();
		return this;
	}

	public SQLBuilder values(Map<String, Object> values) {
		this.values = values;
		return this;
	}

	public String sql() {
		return sql;
	}

	public SQLBuilder sql(String sql) {
		this.sql = sql;
		return this;
	}

	public List<Object> params() {
		return params;
	}

	public SQLBuilder params(List<Object> params) {
		this.params = params;
		return this;
	}

	public List<Condition> conditions() {
		return this.conditions;
	}

	public SQLBuilder conditions(List<Condition> conditions) {
		this.conditions = conditions;
		return this;
	}

	public void dialect(Dialect dialect) {
		this.dialect = dialect;
	}

	public boolean addJoin(String joinDb, String joinTable, String joinTableAlias) {
		if (joins == null) {
			joins = new ArrayList<>();
		}
		for (Join join : joins) {
			if (join.getTargetTable().equals(joinTable)) {
				return true;
			}
		}
		boolean findAssociation = false;
		JSONObject columnsConfig = AirContext.getColumnsConfig(db, table);
		for (String columnName : columnsConfig.keySet()) {
			JSONObject columnConfig = columnsConfig.getObject(columnName);
			if (columnConfig.containsKey(Column.ASSOCIATION)) {
				JSONObject columnAssociation = columnConfig.getObject(Column.ASSOCIATION);
				String associationTable = columnAssociation.getString(Association.TARGET_TABLE);
				String associationColumn = columnAssociation.getString(Association.TARGET_COLUMN);
				String associationType = columnAssociation.getString(Association.TYPE);
				if (joinTable.equals(associationTable)) {
					this.joins
							.add(new Join(table, alias, columnName, associationTable, joinTableAlias, associationColumn,
							JoinType.LEFT, Association.Type.from(associationType)));
					findAssociation = true;
					break;
				}
			}
		}
		if (!findAssociation) {
			for (String columnName : columnsConfig.keySet()) {
				JSONObject columnConfig = columnsConfig.getObject(columnName);
				if (columnConfig.containsKey(Column.ASSOCIATION)) {
					JSONObject columnAssociation = columnConfig.getObject(Column.ASSOCIATION);
					String associationTable = columnAssociation.getString(Association.TARGET_TABLE);
					String associationColumn = columnAssociation.getString(Association.TARGET_COLUMN);
					String associationType = columnAssociation.getString(Association.TYPE);
					JSONObject innerColumnsConfig = AirContext.getColumnsConfig(db, associationTable);
					for (String innerColumnName : innerColumnsConfig.keySet()) {
						JSONObject innerColumnConfig = innerColumnsConfig.getObject(innerColumnName);
						if (innerColumnConfig.containsKey(Column.ASSOCIATION)) {
							JSONObject innerColumnAssociation = innerColumnConfig.getObject(Column.ASSOCIATION);
							String innerAssociationTable = innerColumnAssociation.getString(Association.TARGET_TABLE);
							String innerAssociationColumn = innerColumnAssociation.getString(Association.TARGET_COLUMN);
							String innerAssociationType = innerColumnAssociation.getString(Association.TYPE);
							if (joinTable.equals(innerAssociationTable)) {
								Join join = new Join(table, alias, columnName, associationTable, null,
										associationColumn, JoinType.LEFT, Association.Type.from(associationType));
								this.joins.add(join);
								this.joins.add(new Join(associationTable, join.getTargetAlias(), innerColumnName,
										innerAssociationTable, joinTableAlias, innerAssociationColumn, JoinType.LEFT,
										Association.Type.from(innerAssociationType)));
								return true;
							}
						}
					}
				}
			}
			return false;
		} else {
			return true;
		}
	}

	public void addCondition(Condition condition) {
		this.conditions.add(condition);
	}

}
