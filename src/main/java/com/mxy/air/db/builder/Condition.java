package com.mxy.air.db.builder;

import static com.mxy.air.db.Structure.Operator.BETWEEN;
import static com.mxy.air.db.Structure.Operator.EQUAL;
import static com.mxy.air.db.Structure.Operator.GT;
import static com.mxy.air.db.Structure.Operator.GTE;
import static com.mxy.air.db.Structure.Operator.IN;
import static com.mxy.air.db.Structure.Operator.LIKE;
import static com.mxy.air.db.Structure.Operator.LT;
import static com.mxy.air.db.Structure.Operator.LTE;
import static com.mxy.air.db.Structure.Operator.NOT_EQUAL;
import static com.mxy.air.db.Structure.Operator.NOT_IN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.mxy.air.db.AirContext;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * SQL条件
 * @author mengxiangyun
 *
 */
public class Condition {

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

	private String conditionString;

	private JSONObject conditionObject;

	private JSONArray conditionArray;

	public Condition(String db, String table, String alias) {
		this.db = db;
		this.table = table;
		this.alias = alias;
	}

	public Condition(Operator connector, Operator operator, String column, Object value) {
		this.connector = connector;
		this.operator = operator;
		this.column = column;
		this.value = value;
	}

	public Condition(String conditionString, String db, String table, String alias) {
		this(db, table, alias);
		this.conditionString = conditionString;
		this.connector = Operator.AND;
		parseString(conditionString);
	}

	public Condition(JSONObject conditionObject, String db, String table, String alias) {
		this(db, table, alias);
		this.conditionObject = conditionObject;
		Entry<String, Object> entry = conditionObject.entrySet().iterator().next();
		connector = Operator.from(entry.getKey());
		Object conditionArray = entry.getValue();
		if (conditionArray instanceof JSONArray) {
			parseArray((JSONArray) conditionArray);
		} else {
			parseString(value.toString());
		}
	}

	public Condition(JSONArray conditionArray, String db, String table, String alias) {
		this(db, table, alias);
		this.conditionArray = conditionArray;
		this.connector = Operator.AND;
		parseArray(conditionArray);
	}

	private void parseString(String conditionString) {
		this.conditionString = conditionString;
		this.connector = Operator.AND;
		parseOperator(conditionString);
		String[] kv = conditionString.split(this.operator.op());
		this.column = kv[0];
		this.value = kv[1];
	}

	private void parseArray(JSONArray conditionArray) {
		List<Condition> conditions = new ArrayList<>();
		List<Object> conditionList = conditionArray.toList();
		for (Object condition : conditionList) {
			if (condition instanceof JSONArray) {
				conditions.add(new Condition((JSONArray) condition, this.db, this.table, this.alias));
			} else if (condition instanceof JSONObject) {
				conditions.add(new Condition((JSONObject) condition, this.db, this.table, this.alias));
			} else {
				conditions.add(new Condition(condition.toString(), this.db, this.table, this.alias));
			}
		}
		this.value = conditions;
	}

	/**
	 * 解析条件运算符
	 * 
	 * @param condition
	 * @return
	 */
	private void parseOperator(String conditionString) {
		if (conditionString.indexOf(LIKE.op()) > 0) { // like
			this.operator = LIKE;
		} else if (conditionString.indexOf(LTE.op()) > 0) {// 小于等于
			this.operator = LTE;
		} else if (conditionString.indexOf(GTE.op()) > 0) { // 大于等于
			this.operator = GTE;
		} else if (conditionString.indexOf(LT.op()) > 0) { // 小于
			this.operator = LT;
		} else if (conditionString.indexOf(GT.op()) > 0) { // 大于
			this.operator = GT;
		} else if (conditionString.indexOf(NOT_EQUAL.op()) > 0) {
			String[] kv = conditionString.split(this.operator.op());
			this.column = kv[0];
			this.value = kv[1];
			if (this.value.toString().indexOf(",") != -1) { // not in
				this.operator = NOT_IN;
			} else { // 不等于
				this.operator = NOT_EQUAL;
			}
		} else if (conditionString.indexOf(EQUAL.op()) > 0) {
			String[] kv = conditionString.split(this.operator.op());
			this.column = kv[0];
			this.value = kv[1];
			if (this.value.toString().indexOf(",") != -1) { // in
				this.operator = IN;
			} else if (this.value.toString().indexOf("~") != -1) { // between
				this.operator = BETWEEN;
			} else { // 等于
				this.operator = EQUAL;
			}
		}
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
			for (Object nestedCondition : collection) {
				if (nestedCondition instanceof Condition) {
					conditionBuilder.append(((Condition) nestedCondition).sql());
				}
			}
			conditionBuilder.append(")");
		/*
		 * 2. 单个条件, 其他类型
		 */
		} else {
			String type = AirContext.getTableColumnType(db, table, column);
			value = wrap(value, type);
			switch (operator) {
			case EQUAL:
				conditionBuilder.append(alias).append(column).append(" = ?");
				break;
			case NOT_EQUAL:
				conditionBuilder.append(alias).append(column).append(" != ?");
				break;
			case GT:
				conditionBuilder.append(alias).append(column).append(" > ?");
				break;
			case GTE:
				conditionBuilder.append(alias).append(column).append(" >= ?");
				break;
			case LT:
				conditionBuilder.append(alias).append(column).append(" < ?");
				break;
			case LTE:
				conditionBuilder.append(alias).append(column).append(" <= ?");
				break;
			case IN:
				conditionBuilder.append(alias).append(column).append(" in (");
				// 将所有元素替换为?占位符 a,b,c -> ?,?,?
				String inString = value.toString().replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				conditionBuilder.append(inString);
				conditionBuilder.append(")");

				List<String> inParams = Arrays.asList(value.toString().split(","));
				value = inParams.stream().map(p -> wrap(p, type)).collect(Collectors.toList());
				break;
			case NOT_IN:
				conditionBuilder.append(alias).append(column).append(" not in (");
				// 将所有元素替换为?占位符 a,b,c -> ?,?,?
				String notInString = value.toString().replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				conditionBuilder.append(notInString);
				conditionBuilder.append(")");

				List<String> notInParams = Arrays.asList(value.toString().split(","));
				value = notInParams.stream().map(p -> wrap(p, type)).collect(Collectors.toList());
				break;
			case BETWEEN:
				conditionBuilder.append(alias).append(column).append(" between ? and ?");
				Object[] betweenParams = (Object[]) value;
				betweenParams[0] = wrap(betweenParams[0], type);
				betweenParams[1] = wrap(betweenParams[1], type);
				value = betweenParams;
				break;
			case LIKE:
				conditionBuilder.append(alias).append(column).append(" like ?");
				break;

			default:
				break;
			}
		}
		String sql = conditionBuilder.toString().trim();
		if (sql.startsWith(Operator.AND.sql())) {
			sql = sql.substring(4);
		} else if (sql.startsWith(Operator.OR.sql())) {
			sql = sql.substring(3);
		}
		return sql;
	}

	/**
	 * 
	 * @param value 字段值
	 * @param type 数据库字段类型
	 * @return
	 */
	private Object wrap(Object value, String type) {
		if (type.equals("varchar")) {
			if (value.toString().startsWith("'") && value.toString().endsWith("'")) {
				return value;
			}
			return value = "'" + value + "'";
		}
		return value;
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

}
