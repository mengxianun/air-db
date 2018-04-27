package com.mxy.air.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * SQL语句运行器
 * 
 * @author mengxiangyun
 *
 */
public interface SQLRunner {

	/**
	 * 查询单行数据, 返回column-value的Map
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Object> detail(String sql, Object[] params) throws SQLException;

	/**
	 * 查询多行数据, 返回column-value的Map集合
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> list(String sql, Object[] params) throws SQLException;

	/**
	 * 查询数量
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return
	 * @throws SQLException
	 */
	public long count(String sql, Object[] params) throws SQLException;

	/**
	 * 插入一条记录
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return 插入数据的主键
	 * @throws SQLException
	 */
	public Object insert(String sql, Object[] params) throws SQLException;

	/**
	 * 更新一条记录
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return 更新记录数
	 * @throws SQLException
	 */
	public int update(String sql, Object[] params) throws SQLException;

	/**
	 * 删除一条记录
	 * 
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            SQL参数
	 * @return 更新记录数
	 * @throws SQLException
	 */
	public int delete(String sql, Object[] params) throws SQLException;

}
