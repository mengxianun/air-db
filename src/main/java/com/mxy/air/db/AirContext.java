package com.mxy.air.db;

import javax.sql.DataSource;

import com.google.common.base.Strings;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.DatacolorConfig.Datasource;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONObject;

/**
 * 全局配置信息类, 提供获取各种信息的静态方法
 * @author mengxiangyun
 *
 */
public class AirContext {

	private static JSONObject config;

	private static Injector injector;

	public static void init(JSONObject config, Injector injector) {
		AirContext.config = config;
		AirContext.injector = injector;
		// 设置每个SQLSession的数据源
		for (String db : config.getObject(DatacolorConfig.DATASOURCES).keySet()) {
			DataSource dataSource = (DataSource) config.getObject(DatacolorConfig.DATASOURCES).getObject(db)
					.get(Datasource.SOURCE);
			getSqlSession(db).setDataSource(dataSource);
		}
	}

	public static JSONObject getConfig() {
		return config;
	}

	public static JSONObject getAllTableConfig() {
		return config.getObject(DatacolorConfig.DB_TABLE_CONFIG);
	}

	public static JSONObject getAllTableConfig(String db) {
		return config.getObject(DatacolorConfig.DB_TABLE_CONFIG).getObject(db);
	}

	public static JSONObject getTableConfig(String table) {
		return getTableConfig(getDefaultDb(), table);
	}

	public static JSONObject getTableConfig(String db, String table) {
		return getAllTableConfig(db).getObject(table);
	}

	public static JSONObject getAllTableColumnConfig(String db, String table) {
		return getAllTableConfig(db).getObject(table).getObject(TableConfig.COLUMNS);
	}

	public static String getDefaultDb() {
		String defaultDb = config.getString(DatacolorConfig.DEFAULT_DATASOURCE);
		if (Strings.isNullOrEmpty(defaultDb)) {
			return config.getObject(DatacolorConfig.DATASOURCES).getFirst().getKey();
		} else {
			return defaultDb;
		}
	}

	public static SQLSession getDefaultSqlSession() {
		return injector.getInstance(Key.get(SQLSession.class, Names.named(getDefaultDb())));
	}

	public static SQLSession getSqlSession(String db) {
		return injector.getInstance(Key.get(SQLSession.class, Names.named(db)));
	}
	
	public static void addDbTableConfig(String db, JSONObject dbTableConfig) {
		getAllTableConfig().put(db, dbTableConfig);
	}

	/**
	 * 检查数据源和数据库表
	 * @param db
	 * @param table
	 */
	public static void check(String db, String table) {
		if (Strings.isNullOrEmpty(db) || Strings.isNullOrEmpty(table)) {
			return;
		}
		if (!getAllTableConfig().containsKey(db)) {
			throw new DbException(String.format("数据源 [%s] 不存在", db));
		}
		if (!getAllTableConfig(db).containsKey(table)) {
			throw new DbException(String.format("数据库表 [%s] 不存在", table));
		}
	}

	enum Association {
		
		PRIMARY_TABLE, PRIMARY_COLUMN, TARGET_TABLE, TARGET_COLUMN;

	}

}
