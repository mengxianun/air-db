package com.mxy.air.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.mxy.air.db.builder.Delete;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.builder.Select;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.jdbc.Dialect;

/**
 * SQL构造器
 * 
 * @author mengxiangyun
 *
 */
public abstract class SQLBuilder {
    
	// 数据库表名
    protected String table;
	protected List<Join> joins;
	// 数据库表的列
	protected String[] columns;
    protected String where;
	protected String[] groups;
	protected String[] orders;
	// 分页参数: [start, end]
	protected long[] limit;
	// 插入或更新数据
	protected Map<String, Object> values;
    
    protected String sql;
	protected List<Object> params = new ArrayList<>();
	protected List<Object> whereParams = new ArrayList<>();
	// 关联
	protected List<SQLBuilder> associations;
    
	protected static Dialect dialect;

	public static void dialect(Dialect dialect) {
		SQLBuilder.dialect = dialect;
	}

	public static SQLBuilder select(String table) {
		return new Select(table);
	}

	public static Select select(String table, List<Join> joins, String[] columns, String where,
			List<Object> params, String[] groups,
			String[] orders,
			long[] limit) {
		return new Select(table, joins, columns, where, params, groups, orders, limit);
    }

	public static Insert insert(String table, Map<String, Object> values) {
		return insert(table, values, null);
	}
	public static Insert insert(String table, Map<String, Object> values, List<SQLBuilder> associations) {
		return new Insert(table, values, associations);
	}

	public static Update update(String table, Map<String, Object> values, String where, List<Object> params) {
		return new Update(table, values, where, params);
    }

	public static Delete delete(String table, String where, List<Object> params) {
		return new Delete(table, where, params);
    }
    
	protected abstract SQLBuilder build();
    
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

	public boolean containsAssociations() {
		return associations != null && !associations.isEmpty();
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
		// 插入或更新, SQL参数由values重新构成
		this.params.clear();
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

	public List<Object> whereParams() {
		return whereParams;
	}

	public SQLBuilder whereParams(List<Object> whereParams) {
		this.whereParams = whereParams;
		this.params = whereParams;
		return this;
	}

	public List<SQLBuilder> associations() {
		return associations;
	}

}
