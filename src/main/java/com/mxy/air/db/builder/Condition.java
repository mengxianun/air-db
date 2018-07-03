package com.mxy.air.db.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Strings;
import com.mxy.air.db.AirContext;
import com.mxy.air.db.Structure.Operator;

/**
 * SQL条件
 * @author mengxiangyun
 *
 */
public class Condition {

	/*
	 * 数据库名称
	 */
	private String db;

	/*
	 * 数据库表
	 */
	private String table;

	/*
	 * 数据库表别名
	 */
	private String alias;

	/*
	 * 连接符
	 */
	private Operator connector;

	/*
	 * 运算符
	 */
	private Operator operator;

	/*
	 * 条件字段
	 */
	private String column;

	/*
	 * 条件值, 如果是嵌套条件, 则为 List<Condition> 类型
	 */
	private Object value;

	/**
	 * 所有条件的真实值集合
	 */
	private List<Object> values = new ArrayList<>();

	public Condition(String db, String table, String alias, Operator connector, Operator operator, String column,
			Object value) {
		this.db = db;
		this.table = table;
		this.alias = alias;
		this.connector = connector;
		this.operator = operator;
		this.column = column;
		this.value = value;
	}

	public String sql() {
		StringBuilder conditionBuilder = new StringBuilder();
		/*
		 * 条件连接符
		 */
		switch (connector) {
		case AND:
			conditionBuilder.append(" ").append(Operator.AND.sql()).append(" ");
			break;
		case OR:
			conditionBuilder.append(" ").append(Operator.OR.sql()).append(" ");
			break;

		default:
			break;
		}
		/*
		 * 1. 嵌套条件, 集合类型
		 */
		if (value instanceof Collection) {
			Collection<?> collection = (Collection<?>) value;
			conditionBuilder.append("(");
			boolean first = true;
			for (Object nested : collection) {
				Condition nestedCondition = (Condition) nested;
				String sql = ((Condition) nestedCondition).sql();
				if (first) {
					if (sql.startsWith(Operator.AND.sql())) {
						sql = sql.substring(4);
					} else if (sql.startsWith(Operator.OR.sql())) {
						sql = sql.substring(3);
					}
				} else {
					conditionBuilder.append(" ");
					first = false;
				}
				conditionBuilder.append(sql);
				nestedCondition.getValues().forEach(values::add);
			}
			conditionBuilder.append(")");
		/*
		 * 2. 单个条件, 其他类型
		 */
		} else {
			String aliasPrefix = Strings.isNullOrEmpty(alias) ? "" : alias + ".";
			String type = AirContext.getColumnType(db, table, column);
			switch (operator) {
			case EQUAL:
				conditionBuilder.append(aliasPrefix).append(column).append(" = ?");
				break;
			case NOT_EQUAL:
				conditionBuilder.append(aliasPrefix).append(column).append(" != ?");
				break;
			case GT:
				conditionBuilder.append(aliasPrefix).append(column).append(" > ?");
				break;
			case GTE:
				conditionBuilder.append(aliasPrefix).append(column).append(" >= ?");
				break;
			case LT:
				conditionBuilder.append(aliasPrefix).append(column).append(" < ?");
				break;
			case LTE:
				conditionBuilder.append(aliasPrefix).append(column).append(" <= ?");
				break;
			case IN:
				conditionBuilder.append(aliasPrefix).append(column).append(" in (");
				// 将所有元素替换为?占位符 a,b,c -> ?,?,?
				String inString = value.toString().replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				conditionBuilder.append(inString);
				conditionBuilder.append(")");

				value = value.toString().split(",");
				break;
			case NOT_IN:
				conditionBuilder.append(aliasPrefix).append(column).append(" not in (");
				// 将所有元素替换为?占位符 a,b,c -> ?,?,?
				String notInString = value.toString().replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				conditionBuilder.append(notInString);
				conditionBuilder.append(")");

				value = value.toString().split(",");
				break;
			case BETWEEN:
				conditionBuilder.append(aliasPrefix).append(column).append(" between ? and ?");
				value = value.toString().split("~");
				break;
			case LIKE:
				conditionBuilder.append(aliasPrefix).append(column).append(" like ?");
				break;

			default:
				break;
			}
			/*
			 * 添加值
			 */
			if (value.getClass().isArray()) {
				Arrays.stream((Object[]) value).forEach(v -> values.add(wrap(v, type)));
			} else {
				values.add(wrap(value, type));
			}
		}
		String sql = conditionBuilder.toString().trim();
		return sql;
	}

	/**
	 * 
	 * @param value 字段值
	 * @param type 数据库字段类型
	 * @return
	 */
	private Object wrap(Object value, String type) {
		if (Strings.isNullOrEmpty(type)) {
			return value;
		} else if (type.equals("varchar")) {
			return value.toString();
		} else if (type.equals("int")) {
			return Integer.parseInt(value.toString());
		} else {
			return value;
		}
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public Operator getConnector() {
		return connector;
	}

	public void setConnector(Operator connector) {
		this.connector = connector;
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public List<Object> getValues() {
		return values;
	}

	public void setValues(List<Object> values) {
		this.values = values;
	}

}
