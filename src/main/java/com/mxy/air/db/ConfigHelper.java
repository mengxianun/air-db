package com.mxy.air.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.mxy.air.json.JSONObject;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapListHandler;

public class ConfigHelper {

	private static final String COLUMNS_ATTRIBUTE = "columns";

	private static final String TABLE_COLUMNS_SQL = "select table_name, group_concat(column_name separator ',') columns from information_schema.COLUMNS where table_schema = ? group by table_name";

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
	public static void createTablesConfig(DataSource dataSource, String database, String path, boolean deleteOldConfig)
			throws SQLException, IOException {
		Path dir = Paths.get(path);
		if (!dir.toFile().exists()) {
			Files.createDirectories(dir);
		}
		JdbcRunner runner = new JdbcRunner(dataSource);
		List<Map<String, Object>> result = runner.query(TABLE_COLUMNS_SQL, new MapListHandler(),
				new Object[] { database });
		for (Map<String, Object> map : result) {
			String table = map.get("table_name").toString();
			if (table.equals("schema_version")) {
				continue;
			}
			String columnsStr = map.get("columns").toString();
			String[] columns = columnsStr.split(",");
			Path tableConfigFile = Files.createFile(Paths.get(path + "\\" + table + ".json"));
			if (tableConfigFile.toFile().exists() && deleteOldConfig) {
				tableConfigFile.toFile().delete();
			}
			JSONObject columnsConfig = new JSONObject();
			Arrays.stream(columns).forEach(column -> columnsConfig.put(column, new JSONObject()));
			JSONObject tableConfig = new JSONObject();
			tableConfig.put(COLUMNS_ATTRIBUTE, columnsConfig);
			Files.write(tableConfigFile, tableConfig.toString(2).getBytes());
		}
	}

}
