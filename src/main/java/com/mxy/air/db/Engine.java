package com.mxy.air.db;

import static com.mxy.air.db.Structure.ASSOCIATION;
import static com.mxy.air.db.Structure.FIELDS;
import static com.mxy.air.db.Structure.GROUP;
import static com.mxy.air.db.Structure.JOIN;
import static com.mxy.air.db.Structure.LIMIT;
import static com.mxy.air.db.Structure.NATIVE;
import static com.mxy.air.db.Structure.ORDER;
import static com.mxy.air.db.Structure.VALUES;
import static com.mxy.air.db.Structure.WHERE;
import static com.mxy.air.db.Structure.Order.MINUS;
import static com.mxy.air.db.Structure.Order.PLUS;
import static com.mxy.air.db.Structure.Type.DELETE;
import static com.mxy.air.db.Structure.Type.DETAIL;
import static com.mxy.air.db.Structure.Type.INSERT;
import static com.mxy.air.db.Structure.Type.QUERY;
import static com.mxy.air.db.Structure.Type.SELECT;
import static com.mxy.air.db.Structure.Type.UPDATE;
import static com.mxy.air.db.Structure.Where.AND;
import static com.mxy.air.db.Structure.Where.BETWEEN;
import static com.mxy.air.db.Structure.Where.EQUAL;
import static com.mxy.air.db.Structure.Where.GT;
import static com.mxy.air.db.Structure.Where.GTE;
import static com.mxy.air.db.Structure.Where.IN;
import static com.mxy.air.db.Structure.Where.LIKE;
import static com.mxy.air.db.Structure.Where.LT;
import static com.mxy.air.db.Structure.Where.LTE;
import static com.mxy.air.db.Structure.Where.NOT;
import static com.mxy.air.db.Structure.Where.NOT_EQUAL;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;
import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.Structure.Where;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Native;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.TableConfig;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * 引擎
 * 
 * @author mengxiangyun
 *
 */
public class Engine {

	@Inject
	@Named("config")
	private JSONObject config;

	@Inject
	@Named("tableConfigs")
	private JSONObject tableConfigs;

	/**
	 * 解析JSON对象, 返回SQLBuilder
	 * 
	 * @param object
	 * @return
	 */
	public SQLBuilder parse(JSONObject object) {
		if (object.containsKey(NATIVE)) {
			if (config.getBoolean(DatacolorConfig.NATIVE)) {
				return new Native(object.getString(NATIVE));
			} else {
				throw new DbException("属性[" + NATIVE + "]被禁用");
			}
		}
		return build(object);
	}

	/**
	 * 构建SQLBuilder
	 * 
	 * @param object
	 * @return
	 */
	public SQLBuilder build(JSONObject object) {
		// 操作类型
		Type type = getType(object);
		String table = object.getString(type);

		switch (type) {
		case DETAIL:
		case QUERY:
		case SELECT:
			return select(table, object);
		case INSERT:
			return insert(table, object);
		case UPDATE:
			return update(table, object);
		case DELETE:
			return delete(table, object);

		default:
			return null;
		}
	}

	/**
	 * 获取操作类型
	 * 
	 * @param object
	 * @return
	 */
	public Type getType(JSONObject object) {
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
		if (types.size() != 1) { // 操作类型只能是一个
			throw new DbException("未指定操作类型或指定了多个操作类型");
		}
		return types.get(0);
	}

	/**
	 * 查询
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder select(String table, JSONObject object) {
		// join
		List<Join> joins = parseJoin(table, object.get(JOIN));
		// 字段, 默认查询所有
		String[] fields = object.containsKey(FIELDS) ? object.getArray(FIELDS).toStringArray() : new String[] { "*" };
		// SQL参数
		List<Object> params = new ArrayList<>();
		// where
		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		String where = whereEntry.getKey();
		params.addAll(whereEntry.getValue());
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
		return SQLBuilder.select(table, joins, fields, where, params, groups, orders, limit);
	}

	/**
	 * 插入
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder insert(String table, JSONObject object) {
		// 插入的值
		Map<String, Object> values = object.containsKey(VALUES) ? object.getObject(VALUES).map() : null;
		List<SQLBuilder> associations = parseAssociations(object.getObject(ASSOCIATION), INSERT);
		return SQLBuilder.insert(table, values, associations);

	}

	/**
	 * 更新
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder update(String table, JSONObject object) {
		// SQL参数
		List<Object> params = new ArrayList<>();
		// 插入或更新的值
		Map<String, Object> values = object.containsKey(VALUES) ? object.getObject(VALUES).map() : null;
		// where
		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		String where = whereEntry.getKey();
		params.addAll(whereEntry.getValue());
		return SQLBuilder.update(table, values, where, params);
	}

	/**
	 * 删除
	 * 
	 * @param table
	 * @param object
	 * @return
	 */
	private SQLBuilder delete(String table, JSONObject object) {
		// SQL参数
		List<Object> params = new ArrayList<>();
		// where
		SimpleImmutableEntry<String, List<Object>> whereEntry = parseWhere(object.get(WHERE));
		String where = whereEntry.getKey();
		params.addAll(whereEntry.getValue());
		return SQLBuilder.delete(table, where, params);
	}

	/**
	 * 解析join
	 * 
	 * @param join
	 *            包含join信息的JSON对象
	 * @return
	 */
	private List<Join> parseJoin(String table, Object join) {
		if (join == null) {
			return null;
		}
		List<Join> joins = new ArrayList<>();
		if (join instanceof JSONArray) { // join多个表
			JSONArray joinArray = (JSONArray) join;
			for (Object joinObject : joinArray.list()) {
				if (joinObject instanceof JSONObject) {
					Join parseJoin = parseJoin(table, (JSONObject) joinObject);
					joins.add(parseJoin);
				} else { // 字符串, join单个表, 默认left
					Join parseJoin = parseJoin(table, joinObject.toString(), JoinType.LEFT);
					joins.add(parseJoin);
				}
			}
		} else if (join instanceof JSONObject) { // join单个表, key为JoinType, value为join的表
			Join parseJoin = parseJoin(table, (JSONObject) join);
			joins.add(parseJoin);
		} else { // 字符串, join单个表, 默认left
			Join parseJoin = parseJoin(table, join.toString(), JoinType.LEFT);
			joins.add(parseJoin);
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
	private Join parseJoin(String table, JSONObject join) {
		Entry<String, Object> entry = join.entrySet().iterator().next();
		String joinType = entry.getKey();
		String joinTable = entry.getValue().toString();
		Join parseJoin = parseJoin(table, joinTable, JoinType.from(joinType));
		return parseJoin;
	}

	/**
	 * 解析String类型join, 从join表的配置中找到关联关系
	 * 
	 * @param table
	 * @param joinTable
	 * @param joinType
	 * @return
	 */
	private Join parseJoin(String table, String joinTable, JoinType joinType) {
		JSONObject joinTableConfig = tableConfigs.getObject(joinTable);
		JSONObject joinColumnsConfig = joinTableConfig != null ? joinTableConfig.getObject(TableConfig.COLUMNS) : null;
		if (joinColumnsConfig != null) {
			// 循环join表的每个列, 找到配置了主表关联关系的配置
			for (Entry<String, Object> columnConfigObject : joinColumnsConfig.entrySet()) {
				JSONObject columnConfig = (JSONObject) columnConfigObject.getValue();
				if (columnConfig.containsKey(TableConfig.Column.ASSOCIATION)) {
					JSONObject association = columnConfig.getObject(TableConfig.Column.ASSOCIATION);
					String targetTable = association.getString(TableConfig.Association.TARGET_TABLE);
					// 如果join表的该字段配置的关联表不是主表table
					if (!table.equals(targetTable)) {
						continue;
					}
					// 主表的关联字段为join表中配置的target_column
					String column = association.getString(TableConfig.Association.TARGET_COLUMN);
					String targetColumn = columnConfigObject.getKey();
					return new Join(table, column, joinTable, targetColumn, joinType);
				}
			}
		}
		throw new DbException("配置文件中没有发现关联关系配置");
	}

	/**
	 * 解析关联
	 * 
	 * @param association
	 * @param type
	 * @return
	 */
	private List<SQLBuilder> parseAssociations(JSONObject association, Type type) {
		if (association == null) {
			return null;
		}
		List<SQLBuilder> associations = new ArrayList<>();
		for (Entry<String, Object> entry : association.entrySet()) {
			String table = entry.getKey();
			JSONObject values = (JSONObject) entry.getValue();
			switch (type) {
			case INSERT:
				Insert insert = SQLBuilder.insert(table, values.map());
				associations.add(insert);
				break;
			case UPDATE:
				// Update update = SQLBuilder.update(table, values, where, params);
				// associations.add(update);
				break;
			case DELETE:
				// Delete delete = SQLBuilder.delete(table, where, params);
				// associations.add(delete);
				break;

			default:
				break;
			}
		}
		return associations;
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
		Where op = parseOperator(condition);
		String[] kv = condition.split(op.op());
		String field = kv[0];
		String value = kv[1];
		if (op == EQUAL || op == NOT_EQUAL) { // 等于的情况包括: 直接相等, between(~分割的范围值), in(,分割的多值)
			if (value.indexOf(BETWEEN.op()) > 0) { // between
				String[] values = value.split(BETWEEN.op());
				params.add(processValue(values[0]));
				params.add(processValue(values[1]));
				sql = field + " " + (op == NOT_EQUAL ? NOT.sql() + " " : "") + BETWEEN.sql() + " ? and ?";
			} else if (value.indexOf(IN.op()) > 0) { // in
				List<Object> inParams = Arrays.stream(value.split(IN.op())).map(v -> processValue(v)).collect(Collectors.toList());
				params.addAll(inParams);
				// 将所有元素替换为?占位符  a,b,c -> ?,?,?
				value = value.replaceAll("((?!,).)*", "?").replaceAll("(\\?)+", "?");
				sql = field + " " + (op == NOT_EQUAL ? NOT.sql() + " " : "") + IN.sql() + " (" + value + ")";
			} else {
				params.add(processValue(value));
				sql = field + " " + op.sql() + " ?";
			}
		} else {
			params.add(processValue(value));
			sql = field + " " + op.sql() + " ?";
		}
		return new SimpleImmutableEntry<String, List<Object>>(sql, params);
	}

	/**
	 * 解析条件运算符
	 * 
	 * @param condition
	 * @return
	 */
	private Where parseOperator(String condition) {
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
		if (group instanceof JSONArray) {
			return ((JSONArray) group).toStringArray();
		}
		return new String[] { group.toString() };
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
		if (order instanceof JSONArray) {
			String[] orderFields = ((JSONArray) order).toStringArray();
			return Arrays.stream(orderFields).map(this::parseOrderField).toArray(String[]::new);
		}
		return new String[] { parseOrderField(order.toString()) };
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

}
