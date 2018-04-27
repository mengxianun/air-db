package com.mxy.air.db.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import com.mxy.air.db.DbException;
import com.mxy.air.db.jdbc.dialect.MySQLDialect;
import com.mxy.air.db.jdbc.dialect.OracleDialect;

public class DialectFactory {

	// 数据库方言集合. key: 数据库关键字 value: 数据库方言类
	private static Map<String, Class<? extends Dialect>> dialectMap = new HashMap<>();

	static {
		dialectMap.put("mysql", MySQLDialect.class);
		dialectMap.put("oracle", OracleDialect.class);
	}

	/**
	 * 通过DataSource判断数据库类型
	 * 
	 * @param dataSource
	 *            数据源
	 * @return
	 */
	public static Dialect getDialect(DataSource dataSource) {
		try (Connection connection = dataSource.getConnection()) {
			DatabaseMetaData metaData = connection.getMetaData();
			String url = metaData.getURL();
			return getDialect(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 通过url判断数据库类型
	 * 
	 * @param url
	 *            数据库连接字符串
	 * @return
	 * @throws DbException
	 */
	public static Dialect getDialect(String url) {
		for (Entry<String, Class<? extends Dialect>> entry : dialectMap.entrySet()) {
			String dbName = entry.getKey();
			if (url.indexOf(dbName) != -1) {
				try {
					return entry.getValue().newInstance();
				} catch (Exception e) {
					// 不会出现异常, 这里不做处理
				}
			}
		}
		return null;
	}

}
