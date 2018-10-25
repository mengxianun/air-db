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
import static com.mxy.air.db.Structure.Operator.NOT_LIKE;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Delete;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Select;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.builder.es.EsSelect;
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
				if (object.containsKey(Structure.SOURCE)) {
					db = object.getString(Structure.SOURCE);
				} else {
					db = AirContext.getDefaultDb();
				}
				String sql = object.getString(Structure.NATIVE);
				sql = sql.trim();
				if (sql.startsWith("select")) {
					type = SELECT;
					builder = new Select();
				} else if (sql.startsWith("insert")) {
					type = INSERT;
					builder = new Insert();
				} else if (sql.startsWith("update")) {
					type = UPDATE;
					builder = new Update();
				} else if (sql.startsWith("delete")) {
					type = DELETE;
					builder = new Delete();
				}
				builder.db(db);
				builder.sql(sql);
				return this;
			} else {
				throw new DbException("属性[" + NATIVE + "]被禁用");
			}
		}
		// 操作类型
		parseType();
		// 数据源/表/别名
		parseDbTable();
		/*
		 * Elasticsearch 不添加别名
		 */
		if (!AirContext.isElasticsearch(db) && Strings.isNullOrEmpty(alias)) {
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

	public void parseDbTable() {
		table = object.getString(type).trim();
		if (table.indexOf(".") != -1) { // 指定了数据源
			String[] dbTableString = table.split("\\.");
			db = dbTableString[0];
			table = dbTableString[1];
			if (table.indexOf(" ") != -1) {
				String[] tableAlias = table.split(" ");
				table = tableAlias[0];
				alias = tableAlias[1];
			}
		} else if (table.indexOf(" ") != -1) {
			String[] tableAlias = table.split(" ");
			table = tableAlias[0];
			alias = tableAlias[1];
		}
		if (db == null) {
			db = AirContext.getDefaultDb();
		}
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
		if (AirContext.isElasticsearch(db)) {
			return new EsSelect(table, alias, joins, fields, conditions, groups, orders, limit);
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
					//					aliases.put(joinTable, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable);
					parseJoinTableAlias(joinTable);
				} else {
					parseJoinTableAlias(joinObject.toString());
					//					aliases.put(joinObject.toString(), DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinObject.toString());
				}
			}
		} else if (join instanceof JSONObject) {
			String joinTable = ((JSONObject) join).entrySet().iterator().next().getValue().toString();
			parseJoinTableAlias(joinTable);
			//			aliases.put(joinTable, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTable);
		} else {
			parseJoinTableAlias(join.toString());
			//			aliases.put(join.toString(), DEFAULT_JOIN_TABLE_ALIAS_PREFIX + join.toString());

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

	private void parseJoinTableAlias(String joinTableString) {
		if (joinTableString.contains(" as ")) {
			String[] joinTableNameAlias = joinTableString.split(" as ", 2);
			aliases.put(joinTableNameAlias[0], joinTableNameAlias[1]);
		} else if (joinTableString.contains(" ")) {
			String[] joinTableNameAlias = joinTableString.split("\\s+", 2);
			aliases.put(joinTableNameAlias[0], joinTableNameAlias[1]);
		} else {
			aliases.put(joinTableString, DEFAULT_JOIN_TABLE_ALIAS_PREFIX + joinTableString);
		}
	}

	private String getJoinTableName(String joinTableString) {
		if (joinTableString.contains(" as ")) {
			String[] joinTableNameAlias = joinTableString.split(" as ", 2);
			return joinTableNameAlias[0];
		} else if (joinTableString.contains(" ")) {
			String[] joinTableNameAlias = joinTableString.split("\\s+", 2);
			return joinTableNameAlias[0];
		} else {
			return joinTableString;
		}
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
		joinTable = getJoinTableName(joinTable);
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
			return new ArrayList<>();
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
		Cond cond = parseCond(conditionString);
		Operator operator = cond.getOperator();
		String column = cond.getColumn();
		Object value = cond.getValue();
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
	 * @param quote
	 * @return
	 */
	private Cond parseCond(String conditionString) {
		Operator operator = null;
		int pos = 0;
		int length = conditionString.length();
		over: while (pos < length) {
			switch (conditionString.charAt(pos++)) {
			case '=':
				String tail = conditionString.substring(pos);
				if (tail.contains(",")) { // in
					operator = IN;
				} else if (tail.contains("~")) { // between
					operator = BETWEEN;
				} else { // 等于
					operator = EQUAL;
				}
				break over;
			case '!':
				switch (conditionString.charAt(pos++)) {
				case '=':
					String notTail = conditionString.substring(pos);
					if (notTail.contains(",")) { // in
						operator = NOT_IN;
					} else { // 等于
						operator = NOT_EQUAL;
					}
					break over;

				case '%':
					switch (conditionString.charAt(pos++)) {
					case '=':
						operator = NOT_LIKE;
						break over;
					default:
						break;
					}
					break over;

				default:
					break;
				}
				break;
			case '>':
				switch (conditionString.charAt(pos++)) {
				case '=':
					operator = GTE;
					break over;
				default:
					operator = GT;
				}
				break;
			case '<':
				switch (conditionString.charAt(pos++)) {
				case '=':
					operator = LTE;
					break over;
				default:
					operator = LT;
				}
				break;
			case '%':
				switch (conditionString.charAt(pos++)) {
				case '=':
					operator = LIKE;
					break over;
				default:
					break;
				}
				break;

			default:
				break;
			}
		}
		if (operator == null) {
			return null;
		}

		String[] kv = null;
		/*
		 * 对in和between做特殊处理, 待优化
		 */
		switch (operator) {
		case IN:
		case BETWEEN:
			kv = conditionString.split(EQUAL.op(), 2);
			break;
		case NOT_IN:
			kv = conditionString.split(NOT_EQUAL.op(), 2);
			break;

		default:
			kv = conditionString.split(operator.op(), 2);
			break;
		}

		//		String[] kv = conditionString.split(operator.op(), 2);
		return new Cond(operator, kv[0], kv[1]);
	}

	private class Cond {

		private Operator operator;

		String column;

		Object value;

		public Cond(Operator operator, String column, Object value) {
			this.operator = operator;
			this.column = column;
			this.value = value;
		}

		public Operator getOperator() {
			return operator;
		}

		public String getColumn() {
			return column;
		}

		public Object getValue() {
			return value;
		}

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
		JSONObject tableColumnsConfig = AirContext.getColumnsConfig(db, table);
		String[] newGroup = new String[groupArray.length];
		for (int i = 0; i < groupArray.length; i++) {
			String column = groupArray[i];
			/*
			 * 判断列所属的表
			 * 1. column1 // 主表的列
			 * 2. table1.column1 // 指定表的列
			 */
			if (column.indexOf(".") != -1) {
				String[] tableColumn = column.split("\\.");
				newGroup[i] = aliases.get(tableColumn[0]) + "." + tableColumn[1];
			} else {
				if (!Strings.isNullOrEmpty(alias) && tableColumnsConfig.containsKey(column)) {
					newGroup[i] = alias + "." + column;
				} else {
					newGroup[i] = column;
				}
			}
		}
		return newGroup;
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
		JSONObject tableColumnsConfig = AirContext.getColumnsConfig(db, table);
		String[] newOrder = new String[orderArray.length];
		for (int i = 0; i < orderArray.length; i++) {
			String column = orderArray[i];
			/*
			 * 判断列所属的表
			 * 1. column1 // 主表的列
			 * 2. table1.column1 // 指定表的列
			 */
			if (column.indexOf(".") != -1) {
				String[] tableColumn = column.split("\\.");
				newOrder[i] = aliases.get(tableColumn[0]) + "." + tableColumn[1];
			} else {
				if (!Strings.isNullOrEmpty(alias) && tableColumnsConfig.containsKey(column)) {
					newOrder[i] = alias + "." + column;
				} else {
					newOrder[i] = column;
				}
			}
		}
		return newOrder;
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

	public String getDb() {
		return db;
	}

	public String getTable() {
		return table;
	}

	public String getAlias() {
		return alias;
	}

}
