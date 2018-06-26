package com.mxy.air.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapListHandler;
import com.mxy.air.json.JSONObject;

public class ConfigHelper {

	private static final String TABLE_INFO_SQL = "select t.table_name, t.table_comment, c.column_name, c.data_type, c.column_comment from information_schema.tables t left join information_schema.columns c on t.table_name = c.table_name where t.table_schema = ?";

	public static void createTablesConfig(DataSource dataSource, String database, String path)
			throws SQLException, IOException {
		createTablesConfig(dataSource, database, path, false);
	}

	/**
	 * 生成数据库表配置文件
	 * 
	 * @param dataSource
	 *            数据源
	 * @param database
	 *            数据库名称
	 * @param path
	 *            配置文件生成路径
	 * @param deleteOldConfig
	 *            是否删除已存在的配置文件
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void createTablesConfig(DataSource dataSource, String database, String path,
			boolean deleteOldConfig) throws SQLException, IOException {
		Path dir = Paths.get(path);
		if (!dir.toFile().exists()) {
			Files.createDirectories(dir);
		}
		JSONObject dbTableConfig = new JSONObject();
		JdbcRunner runner = new JdbcRunner(dataSource);
		List<Map<String, Object>> tableInfos = runner.query(TABLE_INFO_SQL, new MapListHandler(),
				new Object[] { database });
		for (Map<String, Object> tableInfo : tableInfos) {
			String tableName = tableInfo.get("table_name").toString();
			String column = tableInfo.get("column_name").toString();
			if (dbTableConfig.containsKey(tableName)) {
				JSONObject tableConfig = dbTableConfig.getObject(tableName);
				if (tableConfig.containsKey(TableConfig.COLUMNS)) {
					JSONObject columns = tableConfig.getObject(TableConfig.COLUMNS);
					if (columns.containsKey(column)) {
						JSONObject columnConfig = columns.getObject(column);
						columnConfig.put(Column.DISPLAY, "");
					} else {
						JSONObject columnConfig = new JSONObject();
						columnConfig.put(Column.DISPLAY, "");
						columns.put(column, columnConfig);
					}
				} else {
					JSONObject columns = new JSONObject();
					JSONObject columnConfig = new JSONObject();
					columnConfig.put(Column.DISPLAY, "");
					columns.put(column, columnConfig);
					tableConfig.put(TableConfig.COLUMNS, columns);
				}
			} else {
				JSONObject tableConfig = new JSONObject();
				JSONObject columns = new JSONObject();
				JSONObject columnConfig = new JSONObject();
				columnConfig.put(Column.DISPLAY, "");
				columns.put(column, columnConfig);
				tableConfig.put(TableConfig.COLUMNS, columns);
				tableConfig.put(TableConfig.DISPLAY, "");
				dbTableConfig.put(tableName, tableConfig);
			}
		}
		for (Map.Entry<String, Object> entry : dbTableConfig.entrySet()) {
			String table = entry.getKey();
			JSONObject value = (JSONObject) entry.getValue();
			Path tableConfigFile = Files.createFile(Paths.get(path + "\\" + table + ".json"));
			if (tableConfigFile.toFile().exists() && deleteOldConfig) {
				tableConfigFile.toFile().delete();
			}
			Files.write(tableConfigFile, value.toString(2).getBytes());
		}
	}

}
