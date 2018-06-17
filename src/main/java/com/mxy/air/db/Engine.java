package com.mxy.air.db;

import static com.mxy.air.db.Structure.FIELDS;
import static com.mxy.air.db.Structure.GROUP;
import static com.mxy.air.db.Structure.JOIN;
import static com.mxy.air.db.Structure.LIMIT;
import static com.mxy.air.db.Structure.NATIVE;
import static com.mxy.air.db.Structure.ORDER;
import static com.mxy.air.db.Structure.VALUES;
import static com.mxy.air.db.Structure.WHERE;
import static com.mxy.air.db.Structure.Operator.AND;
import static com.mxy.air.db.Structure.Operator.BETWEEN;
import static com.mxy.air.db.Structure.Operator.EQUAL;
import static com.mxy.air.db.Structure.Operator.GT;
import static com.mxy.air.db.Structure.Operator.GTE;
import static com.mxy.air.db.Structure.Operator.IN;
import static com.mxy.air.db.Structure.Operator.LIKE;
import static com.mxy.air.db.Structure.Operator.LT;
import static com.mxy.air.db.Structure.Operator.LTE;
import static com.mxy.air.db.Structure.Operator.NOT;
import static com.mxy.air.db.Structure.Operator.NOT_EQUAL;
import static com.mxy.air.db.Structure.Operator.NOT_IN;
import static com.mxy.air.db.Structure.Order.MINUS;
import static com.mxy.air.db.Structure.Order.PLUS;
import static com.mxy.air.db.Structure.Type.DELETE;
import static com.mxy.air.db.Structure.Type.DETAIL;
import static com.mxy.air.db.Structure.Type.INSERT;
import static com.mxy.air.db.Structure.Type.QUERY;
import static com.mxy.air.db.Structure.Type.SELECT;
import static com.mxy.air.db.Structure.Type.TRANSACTION;
import static com.mxy.air.db.Structure.Type.UPDATE;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Native;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * 引擎
 * 
 * @author mengxiangyun
 *
 */
public class Engine {

	public static final String DEFAULT_ALIAS = "t";

	public static final String DEFAULT_JOIN_TABLE_ALIAS_PREFIX = "j_";

	private JSONObject object;

	private SQLBuilder builder;

	private Type type;
	
	private String db;
	
	private String table;
	
	private String alias;

	/*
	 * 所有操作的表, 包括主表和join的表
	 */
	List<String> tables = new ArrayList<>();

	private List<Join> joins = new ArrayList<>();

	private List<Condition> conditions;

	// 表别名, key为表名, value为表别名
	private Map<String, String> aliases = new HashMap<>();

	public Engine() {}

	public Engine(JSONObject object) {
		this.object = object;
	}

	/**
	 * 解析JSON对象, 返回SQLBuilder
	 * 
	 * @param object
	 * @return
	 */
	public Engine parse() {
		if (object.containsKey(NATIVE)) {
			if (AirContext.getConfig().getBoolean(DatacolorConfig.NATIVE)) {
				builder = new Native(object.getString(NATIVE));
				return this;
			} else {
				throw new DbException("属性[" + NATIVE + "]被禁用");
			}
		}
		// 操作类型
		parseType();
		table = object.getString(type).trim();
		if (table.indexOf(".") != -1) { // 指定了数据源
			String[] dbTableString = table.split("\\.");
			db = dbTableString[0];
			table = dbTableString[1];
		}
		if (db == null) {
			db = AirContext.getDefaultDb();
		}
		/*
		 * Elasticsearch 不添加别名
		 */
		if (!AirContext.isElasticsearch(db)) {
			alias = DEFAULT_ALIAS;
		}
		if (!Strings.isNullOrEmpty(alias)) {
			aliases.put(table, alias);
		}
		switch (type) {
		case DETAIL:
		case QUERY:
		case SELECT:
			builder = select(object);
			break;
		case INSERT:
			builder = insert(object);
			break;
		case UPDATE:
			builder = update(object);
			break;
		case DELETE:
			builder = delete(object);
			break;

		default:
			break;
		}
		/*
		 * 临时添加Conditions
		 */
		//		builder.conditions(conditions);
		builder.db(db);
		//		builder.build();
		return this;
	}

	/**
	 * 获取操作类型
	 * 
	 * @param object
	 * @return
	 */
	public void parseType() {
		List<Type> types = new ArrayList<>();
		if (object.containsKey(DETAIL)) {
			types.add(DETAIL);
		}
		if (object.containsKey(QUERY)) {
			types.add(QUERY);
		}
		if (object.containsKey(SELECT)) {
			types.add(SELECT);
		}
		if (object.containsKey(INSERT)) {
			types.add(INSERT);
		}
		if (object.containsKey(UPDATE)) {
			types.add(UPDATE);
		}
		if (object.containsKey(DELETE)) {
			types.add(DELETE);
		}
		if (object.containsKey(TRANSACTION)) {
			types.add(TRANSACTION);
		}
		if (types.size() != 1) { // 操作类型只能是一个
			throw new DbException("未指定操作类型或指定了多个操作类型");
		}
		type = types.get(0);
	}

	/**
	 * 查询
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder select(JSONObject object) {
		// join
		joins = parseJoin(object.get(JOIN));
		// 字段, 默认查询所有
		String[] fields = object.containsKey(FIELDS) ? object.getArray(FIELDS).toStringArray() : null;
		// SQL参数
		List<Object> params = new ArrayList<>();
		// where
		//		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		//		String where = whereEntry.getKey();
		//		params.addAll(whereEntry.getValue());
		conditions = parseWhereNew(object.get(WHERE));
		// group
		String[] groups = parseGroup(object.get(GROUP));
		// order
		String[] orders = parseOrder(object.get(ORDER));
		// limit
		long[] limit = null;
		if (object.containsKey(LIMIT)) {
			Object[] limitArray = object.getArray(LIMIT).array();
			limit = Arrays.stream(limitArray).mapToLong(i -> Long.parseLong(i.toString())).toArray();
		}
		return SQLBuilder.select(table, alias, joins, fields, null, params, conditions, groups, orders, limit);
	}

	/**
	 * 插入
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder insert(JSONObject object) {
		// 插入的值
		Map<String, Object> values = object.containsKey(VALUES) ? object.getObject(VALUES).map() : null;
		return SQLBuilder.insert(table, values);

	}

	/**
	 * 更新
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder update(JSONObject object) {
		// SQL参数
		List<Object> params = new ArrayList<>();
		// 插入或更新的值
		Map<String, Object> values = object.containsKey(VALUES) ? object.getObject(VALUES).map() : null;
		// where
		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		String where = whereEntry.getKey();
		params.addAll(whereEntry.getValue());
		return SQLBuilder.update(table, alias, values, where, params);
	}

	/**
	 * 删除
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder delete(JSONObject object) {
		// SQL参数
		List<Object> params = new ArrayList<>();
		// where
		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		String where = whereEntry.getKey();
		params.addAll(whereEntry.getValue());
		return SQLBuilder.delete(table, alias, where, params);
	}

	/**
	 * 解析join 
	 *   1. 字符串： 
	 *     "join":"nation" // 默认left 
	 *   2. 对象：    
	 *     "join":{ "left":"nation" } 
	 *   3. 数组       
	 *     "join":[ 
	 *       { "left":"nation" } 
	 *     ]
	 * 
	 * @param join
	 *            包含join信息的JSON对象
	 * @return
	 */
	private List<Join> parseJoin(Object join) {
		if (join == null) {
			return null;
		}
		/*
		 * 获取所有操作的表
		 */
		tables.add(table);
		if (join instanceof JSONArray) { // join多个表
			JSONArray joinArray = (JSONArray) join;
			for (Object joinObject : joinArray.list()) {
				if (joinObject instanceof JSONObject) {
					tables.add(((JSONObject) joinObject).entrySet().iterator().next().getValue().toString());
				} else {
					tables.add(joinObject.toString());
				}
			}
		} else if (join instanceof JSONObject) {
			tables.add(((JSONObject) join).entrySet().iterator().next().getValue().toString());
		} else {
			tables.add(join.toString());
		}

		if (join instanceof JSONArray) { // join多个表
			JSONArray joinArray = (JSONArray) join;
			for (Object joinObject : joinArray.list()) {
				if (joinObject instanceof JSONObject) {
					Join parseJoin = parseJoin((JSONObject) joinObject);
					if (parseJoin != null) {
						joins.add(parseJoin);
					}
				} else { // 字符串, join单个表, 默认left
					Join parseJoin = parseJoin(joinObject.toString(), JoinType.LEFT);
					if (parseJoin != null) {
						joins.add(parseJoin);
					}
				}
			}
		} else if (join instanceof JSONObject) { // join单个表, key为JoinType, value为join的表
			Join parseJoin = parseJoin((JSONObject) join);
			if (parseJoin != null) {
				joins.add(parseJoin);
			}
		} else { // 字符串, join单个表, 默认left
			Join parseJoin = parseJoin(join.toString(), JoinType.LEFT);
			if (parseJoin != null) {
				joins.add(parseJoin);
			}
		}
		return joins;
	}

	/**
	 * 解析JSONObject类型join
	 * 
	 * @param table
	 * @param join
	 * @return
	 */
	private Join parseJoin(JSONObject join) {
		Entry<String, Object> entry = join.entrySet().iterator().next();
		String joinType = entry.getKey();
		String joinTable = entry.getValue().toString();
		Join parseJoin = parseJoin(joinTable, JoinType.from(joinType));
		return parseJoin;
	}

	/**
	 * 解析String类型join, 从主表的配置中找到关联关系
	 * 
	 * @param table 主表
	 * @param joinTable join的表
	 * @param joinType join类型
	 * @return
	 */
	private Join parseJoin(String joinTable, JoinType joinType) {
		JSONObject tableConfig = AirContext.getTableConfig(db, table);
		JSONObject columnsConfig = tableConfig.getObject(TableConfig.COLUMNS);
		/*
		 * 该join的表是否与主表关联, 循环主表的每个列, 找到配置了主表关联关系的配置
		 */
		for (Entry<String, Object> columnConfigObject : columnsConfig.entrySet()) {
			String column = columnConfigObject.getKey();
			JSONObject columnConfig = (JSONObject) columnConfigObject.getValue();
			if (columnConfig.containsKey(TableConfig.Column.ASSOCIATION)) {
				JSONObject association = columnConfig.getObject(TableConfig.Column.ASSOCIATION);
				String targetTable = association.getString(TableConfig.Association.TARGET_TABLE);
				// 如果主表的该字段配置的关联表不是joinTable
				if (!joinTable.equals(targetTable)) {
					continue;
				}
				// 主表的关联字段为join表中配置的target_column
				String targetColumn = association.getString(TableConfig.Association.TARGET_COLUMN);
				return new Join(table, alias, column, joinTable, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable,
						targetColumn, joinType);
			}
		}
		/*
		 * 改join的表与其他join的表关联, 比如 A join B, B join C
		 */
		for (String tab : tables) {
			/*
			 * 跳过主表和join表自己, 
			 */
			if (tab.equals(table) || tab.equals(joinTable)) {
				continue;
			}
			JSONObject otherJoinTableConfig = AirContext.getTableConfig(db, tab);
			JSONObject otherJoinColumnsConfig = otherJoinTableConfig.getObject(TableConfig.COLUMNS);
			for (Entry<String, Object> otherJoinColumnConfigObject : otherJoinColumnsConfig.entrySet()) {
				String otherJoinColumn = otherJoinColumnConfigObject.getKey();
				JSONObject otherJoinColumnConfig = (JSONObject) otherJoinColumnConfigObject.getValue();
				if (otherJoinColumnConfig.containsKey(TableConfig.Column.ASSOCIATION)) {
					JSONObject association = otherJoinColumnConfig.getObject(TableConfig.Column.ASSOCIATION);
					String targetTable = association.getString(TableConfig.Association.TARGET_TABLE);
					if (!joinTable.equals(targetTable)) {
						continue;
					}
					String targetColumn = association.getString(TableConfig.Association.TARGET_COLUMN);
					return new Join(tab, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + tab, otherJoinColumn, joinTable,
							DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable, targetColumn,
							joinType);
				}
			}
		}
		return null;
	}

	private List<Condition> parseWhereNew(Object where) {
		if (where == null) {
			return Collections.emptyList();
		}
		List<Condition> conditions = new ArrayList<>();
		if (where instanceof JSONArray) { // 多个条件
			Object[] whereArray = ((JSONArray) where).array();
			for (Object condition : whereArray) {
				if (condition instanceof JSONArray) {
					conditions.add(new Condition(null, null, null, Operator.AND, null, null, parseWhereNew(condition)));
				} else if (condition instanceof JSONObject) {
					Entry<String, Object> entry = ((JSONObject) condition).entrySet().iterator().next();
					Operator connector = Operator.from(entry.getKey());
					Object innerCondition = entry.getValue();

					if (innerCondition instanceof JSONArray) {
						conditions.add(
								new Condition(null, null, null, connector, null, null, parseWhereNew(innerCondition)));
					} else {
						conditions.add(parseCondition(innerCondition.toString(), connector));
					}
				} else {
					conditions.add(parseCondition(condition.toString(), Operator.AND));
				}
			}
		} else {
			conditions.add(parseCondition(where.toString(), Operator.AND));
		}
		return conditions;
	}

	private Condition parseCondition(String conditionString, Operator connector) {
		String table = null;
		String alias = null;
		Operator operator = parseOperatorNew(conditionString);
		String[] kv;
		/*
		 * 对in和between做特殊处理
		 */
		switch (operator) {
		case IN:
		case BETWEEN:
			kv = conditionString.split(EQUAL.op());
			break;
		case NOT_IN:
			kv = conditionString.split(NOT_EQUAL.op());
			break;

		default:
			kv = conditionString.split(operator.op());
			break;
		}
		String column = kv[0];
		Object value = kv[1];
		/*
		 * 判断列所属的表
		 * 1. column1=1 // 主表的列
		 * 2. table1.column1=1 // 指定表的列
		 */
		if (column.indexOf(".") != -1) {
			String[] tableColumn = column.split("\\.");
			String specifiedTable = tableColumn[0];
			if (specifiedTable.equals(this.table)) { // 主表的列
				table = this.table;
				alias = this.alias;
			} else { // 指定表的列
				table = specifiedTable;
				alias = aliases.get(specifiedTable);
			}
		} else {
			table = this.table;
			alias = this.alias;
		}
		return new Condition(db, table, alias, connector, operator, column, value);
	}

	/**
	 * 解析条件运算符
	 * 
	 * @param condition
	 * @return
	 */
	private Operator parseOperatorNew(String conditionString) {
		if (conditionString.indexOf(LIKE.op()) > 0) { // like
			return LIKE;
		} else if (conditionString.indexOf(LTE.op()) > 0) {// 小于等于
			return LTE;
		} else if (conditionString.indexOf(GTE.op()) > 0) { // 大于等于
			return GTE;
		} else if (conditionString.indexOf(LT.op()) > 0) { // 小于
			return LT;
		} else if (conditionString.indexOf(GT.op()) > 0) { // 大于
			return GT;
		} else if (conditionString.indexOf(NOT_EQUAL.op()) > 0) {
			String[] kv = conditionString.split(NOT_EQUAL.op());
			//			String column = kv[0];
			Object value = kv[1];
			if (value.toString().indexOf(",") != -1) { // not in
				return NOT_IN;
			} else { // 不等于
				return NOT_EQUAL;
			}
		} else if (conditionString.indexOf(EQUAL.op()) > 0) {
			String[] kv = conditionString.split(EQUAL.op());
			//			String column = kv[0];
			Object value = kv[1];
			if (value.toString().indexOf(",") != -1) { // in
				return IN;
			} else if (value.toString().indexOf("~") != -1) { // between
				return BETWEEN;
			} else { // 等于
				return EQUAL;
			}
		}
		return null;
	}

	/**
	 * 解析where
	 * 
	 * @param where
	 * @return
	 */
	private SimpleImmutableEntry<String, List<Object>> parseWhere(Object where) {
		if (where == null) {
			return new SimpleImmutableEntry<String, List<Object>>(null, Collections.emptyList());
		}
		if (where instanceof JSONArray) { // 多个条件
			StringBuilder builder = new StringBuilder();
			List<Object> params = new ArrayList<>();
			Object[] whereArray = ((JSONArray) where).array();
			// 是否使用连接符
			boolean connect = false;
			for (Object element : whereArray) {
				// 默认条件连接符为and
				String connector = AND.op();
				Object condition = element;
				if (element instanceof JSONObject) { // 指定连接符的情况, 提取连接符和条件
					Entry<String, Object> entry = ((JSONObject) element).entrySet().iterator().next();
					connector = entry.getKey();
					condition = entry.getValue();
				}
				if (connect) { // 第一个条件不添加连接符, 其他都添加
					builder.append(' ').append(connector).append(' ');
				}
				if (condition instanceof JSONArray) { // 数组类型条件, 相当于另一个嵌入的where条件, 所以这里做递归处理, 并且用括号包裹
					SimpleImmutableEntry<String, List<Object>> innerWhere = parseWhere(condition);
					builder.append('(').append(innerWhere.getKey()).append(')');
					params.addAll(innerWhere.getValue());
				} else { // 字符串类型条件
					SimpleImmutableEntry<String, List<Object>> innerCondition = parseWhere(condition);

					builder.append(innerCondition.getKey());
					params.addAll(innerCondition.getValue());
				}
				connect = true;
			}
			return new SimpleImmutableEntry<String, List<Object>>(builder.toString(), params);
		} else { // 字符串类型, 单个条件
			return parseCondition(where.toString());
		}
	}

	/**
	 * 解析具体条件. =, !=, >, >=, <, <=, ,, ~, =~
	 * 
	 * @param condition
	 *            条件
	 * @return
	 */
	private SimpleImmutableEntry<String, List<Object>> parseCondition(String condition) {
		if (condition == null || condition.isEmpty()) {
			return new SimpleImmutableEntry<String, List<Object>>(null, Collections.emptyList());
		}
		String sql;
		List<Object> params = new ArrayList<>();
		Operator op = parseOperator(condition);
		String[] kv = condition.split(op.op());
		String field = kv[0];
		/*
		 * 如果where条件字段没有指定表别名, 添加表别名
		 * ***********临时方法, 待优化***********
		 */
		if (field.indexOf(".") == -1) {
			JSONObject tableColumnConfig = AirContext.getAllTableColumnConfig(db, table);
			if (tableColumnConfig.containsKey(field)) {
				field = DEFAULT_ALIAS + "." + field;
			} else {
				if (joins != null) {
					for (Join join : joins) {
						JSONObject joinTableColumnConfig = AirContext.getAllTableColumnConfig(db,
								join.getTargetTable());
						if (joinTableColumnConfig.containsKey(field)) {
							field = join.getTargetAlias() + "." + field;
							break;
						}
					}
				}
			}
		} else {
			String[] whereTableField = field.split("\\.");
			String whereTable = whereTableField[0];
			String whereField = whereTableField[1];
			if (whereTable.equals(table)) {
				field = (alias == null ? DEFAULT_ALIAS : alias) + "." + whereField;
			} else {
				if (joins != null) {
					for (Join join : joins) {
						if (whereTable.equals(join.getTargetTable())) {
							field = join.getTargetAlias() + "." + whereField;
							break;
						}
					}
				}
			}
		}
		String value = kv[1];
		if (op == EQUAL || op == NOT_EQUAL) { // 等于的情况包括: 直接相等, between(~分割的范围值), in(,分割的多值)
			if (value.indexOf(BETWEEN.op()) > 0) { // between
				String[] values = value.split(BETWEEN.op());
				params.add(processValue(values[0]));
				params.add(processValue(values[1]));
				sql = field + " " + (op == NOT_EQUAL ? NOT.sql() + " " : "") + BETWEEN.sql() + " ? and ?";
				//
				//				conditions.add(
				//						new Condition(op, field, new Object[] { processValue(values[0]), processValue(values[1]) }));
			} else if (value.indexOf(IN.op()) > 0) { // in
				List<Object> inParams = Arrays.stream(value.split(IN.op())).map(v -> processValue(v))
						.collect(Collectors.toList());
				params.addAll(inParams);
				// 将所有元素替换为?占位符 a,b,c -> ?,?,?
				value = value.replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				sql = field + " " + (op == NOT_EQUAL ? NOT.sql() + " " : "") + IN.sql() + " (" + value + ")";
				//
				//				conditions.add(new Condition(op, field, inParams));
			} else {
				Object processValue = processValue(value);
				params.add(processValue);
				sql = field + " " + op.sql() + " ?";
				//
				//				conditions.add(new Condition(op, field, processValue));
			}
		} else {
			Object processValue = processValue(value);
			params.add(processValue);
			sql = field + " " + op.sql() + " ?";
			//
			//			conditions.add(new Condition(op, field, processValue));
		}
		return new SimpleImmutableEntry<String, List<Object>>(sql, params);
	}

	/**
	 * 解析条件运算符
	 * 
	 * @param condition
	 * @return
	 */
	private Operator parseOperator(String condition) {
		if (condition.indexOf(LIKE.op()) > 0) { // like
			return LIKE;
		} else if (condition.indexOf(LTE.op()) > 0) {// 小于等于
			return LTE;
		} else if (condition.indexOf(GTE.op()) > 0) { // 大于等于
			return GTE;
		} else if (condition.indexOf(LT.op()) > 0) { // 小于
			return LT;
		} else if (condition.indexOf(GT.op()) > 0) { // 大于
			return GT;
		} else if (condition.indexOf(NOT_EQUAL.op()) > 0) { // 不等于
			return NOT_EQUAL;
		} else if (condition.indexOf(EQUAL.op()) > 0) { // 等于
			return EQUAL;
		}
		return null;
	}

	/**
	 * 处理过滤条件中的值, 将值转换为具体类型.原始值类型为String
	 * 整数类型转换为int或long
	 * 小数类型转换为double
	 * 单引号包裹的去掉单引号, 返回String类型
	 * 其他情况返回原始值
	 * 
	 * @param value
	 * @return
	 */
	private Object processValue(String value) {
		char initial = value.charAt(0);
		if ((initial >= '0' && initial <= '9') || initial == '-' || initial == '+') { // 数值类型
			if (value.contains(".") || value.contains("e") || value.contains("E") || value.contains("-0")) { // 小数
				return Double.valueOf(value);
			} else {
				long l = Long.valueOf(value);
				if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
					return (int) l;
				}
				return l;
			}
		} else if (value.startsWith("'") && value.endsWith("'")) { // 字符串类型
			return value.substring(1, value.length() - 1);
		}
		return value;
	}

	/**
	 * 解析group
	 * 
	 * @param group
	 * @return
	 */
	private String[] parseGroup(Object group) {
		if (group == null) {
			return null;
		}
		String[] groupArray;
		if (group instanceof JSONArray) {
			groupArray = ((JSONArray) group).toStringArray();
		} else {
			groupArray = new String[] { group.toString() };
		}
		List<String> newGroup = new ArrayList<>();
		for (String field : groupArray) {
			/*
			 * 如果字段没有指定表别名, 添加表别名
			 * ***********临时方法, 待优化***********
			 */
			if (field.indexOf(".") == -1) {
				JSONObject tableColumnConfig = AirContext.getAllTableColumnConfig(db, table);
				if (tableColumnConfig.containsKey(field)) {
					field = DEFAULT_ALIAS + "." + field;
				} else {
					if (joins != null) {
						for (Join join : joins) {
							JSONObject joinTableColumnConfig = AirContext.getAllTableColumnConfig(db,
									join.getTargetTable());
							if (joinTableColumnConfig.containsKey(field)) {
								field = join.getTargetAlias() + "." + field;
								break;
							}
						}
					}
				}
			} else {
				String[] whereTableField = field.split("\\.");
				String whereTable = whereTableField[0];
				String whereField = whereTableField[1];
				if (whereTable.equals(table)) {
					field = (alias == null ? DEFAULT_ALIAS : alias) + "." + whereField;
				} else {
					if (joins != null) {
						for (Join join : joins) {
							if (whereTable.equals(join.getTargetTable())) {
								field = join.getTargetAlias() + "." + whereField;
								break;
							}
						}
					}
				}
			}
			newGroup.add(field);
		}
		return newGroup.toArray(new String[] {});
	}

	/**
	 * 解析order
	 * 
	 * @param order
	 * @return
	 */
	private String[] parseOrder(Object order) {
		if (order == null) {
			return null;
		}
		String[] orderArray;
		if (order instanceof JSONArray) {
			String[] orderFields = ((JSONArray) order).toStringArray();
			orderArray = Arrays.stream(orderFields).map(this::parseOrderField).toArray(String[]::new);
		} else {
			orderArray = new String[] { parseOrderField(order.toString()) };
		}
		List<String> newOrder = new ArrayList<>();
		for (String field : orderArray) {
			/*
			 * 如果字段没有指定表别名, 添加表别名
			 * ***********临时方法, 待优化***********
			 */
			String realField = field;
			if (field.indexOf(" ") != -1) {
				realField = field.split(" +")[0];
			}
			if (realField.indexOf(".") == -1) {
				JSONObject tableColumnConfig = AirContext.getAllTableColumnConfig(db, table);
				if (tableColumnConfig.containsKey(realField)) {
					field = DEFAULT_ALIAS + "." + field;
				} else {
					if (joins != null) {
						for (Join join : joins) {
							JSONObject joinTableColumnConfig = AirContext.getAllTableColumnConfig(db,
									join.getTargetTable());
							if (joinTableColumnConfig.containsKey(realField)) {
								field = join.getTargetAlias() + "." + field;
								break;
							}
						}
					}
				}
			} else {
				String[] whereTableField = field.split("\\.");
				String whereTable = whereTableField[0];
				String whereField = whereTableField[1];
				if (whereTable.equals(table)) {
					field = (alias == null ? DEFAULT_ALIAS : alias) + "." + whereField;
				} else {
					if (joins != null) {
						for (Join join : joins) {
							if (whereTable.equals(join.getTargetTable())) {
								field = join.getTargetAlias() + "." + whereField;
								break;
							}
						}
					}
				}
			}
			newOrder.add(field);
		}
		return newOrder.toArray(new String[] {});
	}

	/**
	 * 解析单个字段排序的字符串
	 * 
	 * @param orderStr
	 * @return
	 */
	private String parseOrderField(String orderStr) {
		// orderStr = orderStr.trim().replaceAll(" +", " ");
		orderStr = orderStr.trim();
		if (orderStr.startsWith(PLUS.mark())) {
			return orderStr.substring(1) + " " + PLUS.sql();
		} else if (orderStr.startsWith(MINUS.mark())) {
			return orderStr.substring(1) + " " + MINUS.sql();
		} else { // 单个字段, 默认升序, asc关键字可省略
			return orderStr;
		}

	}

	public SQLBuilder getBuilder() {
		return builder;
	}

	public Type getType() {
		return type;
	}

}
