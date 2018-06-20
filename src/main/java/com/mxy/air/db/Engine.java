package com.mxy.air.db;

import static com.mxy.air.db.Structure.FIELDS;
import static com.mxy.air.db.Structure.GROUP;
import static com.mxy.air.db.Structure.JOIN;
import static com.mxy.air.db.Structure.LIMIT;
import static com.mxy.air.db.Structure.NATIVE;
import static com.mxy.air.db.Structure.ORDER;
import static com.mxy.air.db.Structure.VALUES;
import static com.mxy.air.db.Structure.WHERE;
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
import static com.mxy.air.db.Structure.Order.MINUS;
import static com.mxy.air.db.Structure.Order.PLUS;
import static com.mxy.air.db.Structure.Type.DELETE;
import static com.mxy.air.db.Structure.Type.DETAIL;
import static com.mxy.air.db.Structure.Type.INSERT;
import static com.mxy.air.db.Structure.Type.QUERY;
import static com.mxy.air.db.Structure.Type.SELECT;
import static com.mxy.air.db.Structure.Type.TRANSACTION;
import static com.mxy.air.db.Structure.Type.UPDATE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Native;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Association;
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

	private List<Join> joins = new ArrayList<>();

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
		aliases.put(table, alias);
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
		// 检查
		aliases.keySet().forEach(t -> AirContext.check(db, t));
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
		// where
		List<Condition> conditions = parseWhere(object.get(WHERE));
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
		return SQLBuilder.select(table, alias, joins, fields, conditions, groups, orders, limit);
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
		// 插入或更新的值
		Map<String, Object> values = object.containsKey(VALUES) ? object.getObject(VALUES).map() : null;
		// where
		List<Condition> conditions = parseWhere(object.get(WHERE));
		return SQLBuilder.update(table, alias, values, conditions);
	}

	/**
	 * 删除
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder delete(JSONObject object) {
		// where
		List<Condition> conditions = parseWhere(object.get(WHERE));
		return SQLBuilder.delete(table, alias, conditions);
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
		 * 先解析出所有操作的表
		 */
		if (join instanceof JSONArray) { // join多个表
			JSONArray joinArray = (JSONArray) join;
			for (Object joinObject : joinArray.list()) {
				if (joinObject instanceof JSONObject) {
					String joinTable = ((JSONObject) joinObject).entrySet().iterator().next().getValue().toString();
					aliases.put(joinTable, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable);
				} else {
					aliases.put(joinObject.toString(), DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinObject.toString());
				}
			}
		} else if (join instanceof JSONObject) {
			String joinTable = ((JSONObject) join).entrySet().iterator().next().getValue().toString();
			aliases.put(joinTable, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable);
		} else {
			aliases.put(join.toString(), DEFAULT_JOIN_TABLE_ALIAS_PREFIX + join.toString());
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
		// 1. 查看主表配置文件中是否配置了关联
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
				Association.Type associationType;
				String type = association.getString(TableConfig.Association.TYPE);
				if (type == null) {
					associationType = Association.Type.ONE_TO_ONE;
				} else {
					associationType = Association.Type.from(type);
				}
				return new Join(table, alias, column, joinTable, aliases.get(joinTable),
						targetColumn, joinType, associationType);
			}
		}
		// 2. 查看join表的配置文件中时候配置了关联
		JSONObject joinTableConfig = AirContext.getTableConfig(db, joinTable);
		JSONObject joinColumnsConfig = joinTableConfig.getObject(TableConfig.COLUMNS);
		for (Entry<String, Object> joinColumnConfigObject : joinColumnsConfig.entrySet()) {
			String joinTableColumn = joinColumnConfigObject.getKey();
			JSONObject joinTableColumnConfig = (JSONObject) joinColumnConfigObject.getValue();
			if (joinTableColumnConfig.containsKey(TableConfig.Column.ASSOCIATION)) {
				JSONObject association = joinTableColumnConfig.getObject(TableConfig.Column.ASSOCIATION);
				String targetTable = association.getString(TableConfig.Association.TARGET_TABLE);
				// 如果join表的该字段配置的关联表不是主表
				if (!table.equals(targetTable)) {
					continue;
				}
				// join表配置的target_column为主表的字段
				String targetColumn = association.getString(TableConfig.Association.TARGET_COLUMN);
				Association.Type associationType;
				String type = association.getString(TableConfig.Association.TYPE);
				if (type == null) {
					associationType = Association.Type.ONE_TO_ONE;
				} else {
					associationType = Association.Type.from(type);
				}
				/*
				 * join表对主表的一对多反过来就是主表对join表的多对一
				 */
				switch (associationType) {
				case ONE_TO_MANY:
					associationType = Association.Type.MANY_TO_ONE;
					break;
				case MANY_TO_ONE:
					associationType = Association.Type.ONE_TO_MANY;
					break;

				default:
					break;
				}
				return new Join(table, alias, targetColumn, joinTable, aliases.get(joinTable), joinTableColumn,
						joinType, associationType);
			}
		}
		/*
		 * 该join的表与其他join的表关联, 比如 A join B, B join C
		 */
		for (String tab : aliases.keySet()) {
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
					Association.Type associationType;
					String type = association.getString(TableConfig.Association.TYPE);
					if (type == null) {
						associationType = Association.Type.ONE_TO_ONE;
					} else {
						associationType = Association.Type.from(type);
					}
					return new Join(tab, aliases.get(tab), otherJoinColumn, joinTable, aliases.get(joinTable),
							targetColumn, joinType, associationType);
				}
			}
		}
		return null;
	}

	private List<Condition> parseWhere(Object where) {
		if (where == null) {
			return Collections.emptyList();
		}
		List<Condition> conditions = new ArrayList<>();
		if (where instanceof JSONArray) { // 多个条件
			Object[] whereArray = ((JSONArray) where).array();
			for (Object condition : whereArray) {
				if (condition instanceof JSONArray) {
					conditions.add(new Condition(null, null, null, Operator.AND, null, null, parseWhere(condition)));
				} else if (condition instanceof JSONObject) {
					Entry<String, Object> entry = ((JSONObject) condition).entrySet().iterator().next();
					Operator connector = Operator.from(entry.getKey());
					Object innerCondition = entry.getValue();

					if (innerCondition instanceof JSONArray) {
						conditions.add(
								new Condition(null, null, null, connector, null, null, parseWhere(innerCondition)));
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
		Operator operator = parseOperator(conditionString);
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
			column = tableColumn[1];
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
	private Operator parseOperator(String conditionString) {
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
