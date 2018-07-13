package com.mxy.air.db.jdbc;

/**
 * 数据库方言
 * 
 * @author mengxiangyun
 *
 */
public interface Dialect {
	
	/**
	 * 生成分页查询sql, 参数用占位符替代
	 * 
	 * @param sql
	 *            原始sql
	 * @param page
	 *            分页对象
	 * @return
	 */
	public String processLimit(String sql);
	
	/**
	 * 返回分页参数
	 * 
	 * @param sql
	 *            原始sql
	 * @param page
	 *            分页对象
	 * @return
	 */
	public Object[] processLimitParams(Page page);
	
	/**
	 * 关键字转义符号
	 * @return
	 */
	default String getKeywordSymbol() {
		return "";
	}

}
