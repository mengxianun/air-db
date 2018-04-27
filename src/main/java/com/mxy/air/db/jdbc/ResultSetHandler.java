package com.mxy.air.db.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet处理器接口
 * 
 * @author mengxiangyun
 *
 * @param <T>
 */
public interface ResultSetHandler<T> {

	/**
	 * 将ResultSet转换为指定对象
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	T handle(ResultSet rs) throws SQLException;

}
