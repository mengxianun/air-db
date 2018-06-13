package com.mxy.air.db;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.PoolProperties;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.DatacolorConfig.Datasource;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.interceptors.SQLLogInterceptor;
import com.mxy.air.db.jdbc.DialectFactory;
import com.mxy.air.json.JSON;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * 转换器, 将Json转换为SQL并执行数据库操作, 然后返回结果
 * 
 * @author mengxiangyun
 *
 */
public class Translator {

	public static final String DEFAULT_CONFIG_FILE = "xiaolongnv.json";

	public static final String DEFAULT_DATASOURCE_POOL = "org.apache.tomcat.jdbc.pool.DataSource";

	private SQLHandler handler;

	/**
	 * 无参构造器, 从默认的配置文件构建SQLTranslator
	 */
	public Translator() {
		this(null, (DataSource[]) null);
	}

	/**
	 * 从配置文件构建SQLTranslator
	 * 
	 * @param configFile
	 */
	public Translator(String configFile) {
		this(configFile, (DataSource[]) null);
	}

	/**
	 * 从数据源基本参数中构建DataSource, 该DataSource为默认的数据源DEFAULT_DATASOURCE_POOL
	 * 
	 * @param driverClassName
	 * @param url
	 * @param username
	 * @param password
	 */
	public Translator(String driverClassName, String url, String username, String password) {
		this(defaultDataSource(driverClassName, url, username, password));
	}

	public Translator(DataSource... dataSources) {
		this(null, dataSources);
	}

	/**
	 * 如果用户指定了配置文件, 则按该配置文件解析配置
	 * 如果指定的配置文件不存在, 则抛出异常
	 * 如果用户没有指定配置文件, 则按默认配置文件解析配置.
	 * 如果默认配置文件不存在, 则不做处理也不抛出异常. 用户没有指定配置文件的情况下, 默认配置文件可有可无 数据源: 用户通过构造函数传入的数据源优先,
	 * 如果没有传入则使用配置文件中配置的数据源信息
	 */
	public Translator(String configFile, DataSource... dataSources) {
		/*
		 * 读取全局配置文件
		 */
		// Datacolor配置, 初始为默认配置
		JSONObject config = new JSONObject(DatacolorConfig.toMap());
		JSONObject customConfig;
		try {
			customConfig = JSON.readObject(configFile == null ? DEFAULT_CONFIG_FILE : configFile);
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		}
		// 覆盖默认配置
		config.merge(customConfig);
		/**
		 * 读取数据库表配置文件
		 */
		String tablesConfigPath = config.containsKey(DatacolorConfig.DB_TABLE_CONFIG_PATH)
				? config.getString(DatacolorConfig.DB_TABLE_CONFIG_PATH)
				: DatacolorConfig.DB_TABLE_CONFIG_PATH.value().toString();
		// 默认数据源
		String defaultDb = config.getString(DatacolorConfig.DEFAULT_DATASOURCE);
		JSONObject dbObject = config.getObject(DatacolorConfig.DATASOURCES);
		if (defaultDb == null && dbObject != null) {
			defaultDb = dbObject.getFirst().getKey();
		}
		JSONObject dbTableConfig;
		try {
			Set<String> dbs = dbObject == null ? new HashSet<>() : dbObject.keySet();
			dbTableConfig = readAllDbTablesConfig(tablesConfigPath, dbs, defaultDb);
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		}
		config.put(DatacolorConfig.DB_TABLE_CONFIG, dbTableConfig);
		/*
		 * 数据源
		 */
		JSONObject dss;
		if (dataSources == null || dataSources.length == 0) {
			dss = parseDataSource(config);
			if (dss == null) {
				throw new DbException("数据源为空");
			}
			config.put(DatacolorConfig.DATASOURCES, dss);
		} else {
			// 手动指定数据源时, 暂时只考虑第一个数据源, 多数据源指定待实现
			DataSource dataSource = dataSources[0];
			dss = new JSONObject().put(DatacolorConfig.DEFAULT_DATASOURCE.value().toString(), dataSource);
		}
		config.put(DatacolorConfig.DATASOURCES, dss);

		/*
		 * 绑定依赖注入对象
		 */
		Injector injector = Guice.createInjector(new AbstractModule() {

			@Override
			protected void configure() {
				for (String db : dss.keySet()) {
					/*
					 * 由于自己创建的(即非Guice创建的实例)Guice绑定后无法进行AOP操作, 
					 * 所以先绑定SQLSession对象(由Guice创建), 
					 * 然后再获取Guice创建的每个SQLSession进行数据源设置, 设置数据源的操作在Aircontext.init()方法中执行
					 */
					//					bind(SQLSession.class).annotatedWith(Names.named(db)).toInstance(new SQLSession(dataSource));
					bind(SQLSession.class).annotatedWith(Names.named(db)).to(SQLSession.class).in(Singleton.class);
				}
				bind(SQLHandler.class).in(Singleton.class);
				bind(DataProcessor.class).in(Singleton.class);
				bind(DataRenderer.class).in(Singleton.class);
				// 是否开启日志
				if (config.getBoolean(DatacolorConfig.LOG)) {
					// 类级别
					bindInterceptor(Matchers.annotatedWith(SQLLog.class), Matchers.any(), new SQLLogInterceptor());
					// 方法级别
					bindInterceptor(Matchers.any(), Matchers.annotatedWith(SQLLog.class), new SQLLogInterceptor());
				}
				// 绑定拦截器对象，使其可以注入其他绑定对象，如SQLSession
				//				bind(TransactionInterceptor.class).toInstance(new TransactionInterceptor());
				// 全局配置
				//				bind(JSONObject.class).annotatedWith(Names.named("config")).toInstance(config);
				//				bind(JSONObject.class).annotatedWith(Names.named("dbTableConfig")).toInstance(dbTableConfig);
			}

		});
		this.handler = injector.getInstance(SQLHandler.class);
		AirContext.init(config, injector);;
		try {
			initTableInfo();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 初始数据库表信息
	 * @param dataSource
	 * @param jdbcRunner
	 * @param tableConfigs 用户配置的数据库表配置
	 * @throws SQLException
	 */
	private void initTableInfo() throws SQLException {
		JSONObject dbsConfig = AirContext.getConfig().getObject(DatacolorConfig.DATASOURCES);
		Set<String> dbs = dbsConfig.keySet();
		for (String db : dbs) {
			JSONObject dbTableConfig = AirContext.getAllTableConfig(db);
			if (dbTableConfig == null) {
				dbTableConfig = new JSONObject();
				AirContext.addDbTableConfig(db, dbTableConfig);
			}
			SQLSession sqlSession = AirContext.getSqlSession(db);
			String realDbName = sqlSession.getDbName();
			String sql = "select * from information_schema.columns where table_schema = ?";
			List<Map<String, Object>> tableInfos = sqlSession.list(sql, new String[] { realDbName });
			for (Map<String, Object> tableInfo : tableInfos) {
				String tableName = tableInfo.get("TABLE_NAME").toString();
				String column = tableInfo.get("COLUMN_NAME").toString();
				String dataType = tableInfo.get("DATA_TYPE").toString();
				if (dbTableConfig.containsKey(tableName)) {
					JSONObject tableConfig = dbTableConfig.getObject(tableName);
					if (tableConfig.containsKey(TableConfig.COLUMNS)) {
						JSONObject columns = tableConfig.getObject(TableConfig.COLUMNS);
						if (columns.containsKey(column)) {
							JSONObject columnConfig = columns.getObject(column);
							columnConfig.put(Column.TYPE, dataType);
						} else {
							columns.put(column, new JSONObject().put(Column.TYPE, dataType));
						}
					} else {
						JSONObject columns = new JSONObject();
						columns.put(column, new JSONObject().put(Column.TYPE, dataType));
						tableConfig.put(TableConfig.COLUMNS, columns);
					}
				} else {
					JSONObject tableConfig = new JSONObject();
					JSONObject columns = new JSONObject();
					columns.put(column, new JSONObject().put(Column.TYPE, dataType));
					tableConfig.put(TableConfig.COLUMNS, columns);
					dbTableConfig.put(tableName, tableConfig);
				}
			}
		}
	}

	/**
	 * 读取所有数据库表配置文件
	 * 结构
	 *   tablePath
	 *     - db1
	 *       - table1.json
	 *       - table2.json
	 *     - db2
	 *       - table1.json
	 *       - table2.json
	 * 
	 * @param tablesPath 数据表配置路径
	 * @param dbs 数据源名称集合
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	private JSONObject readAllDbTablesConfig(String tablesConfigPath, Set<String> dbs, String defaultDb)
			throws IOException, URISyntaxException {
		JSONObject allDbTablesConfig = new JSONObject();
		// 数据库表配置路径的根路径下的数据库表配置, 该路径下的数据库表配置将被当作为默认数据源的数据库表配置
		// 当该路径下的数据库表配置和数据源路径下的数据库表配置同时存在时, 数据源路径下的数据库表配置优先
		JSONObject rootDbTablesConfig = new JSONObject();
		Path configPath = JSON.getPath(tablesConfigPath);
		if (configPath == null)
			return allDbTablesConfig;
		try (Stream<Path> stream = Files.walk(configPath, 2)) { // 这里循环2层, 由结构决定
			stream.filter(Files::isRegularFile).forEach(path -> {
				Path parentPath = path.getParent();
				try {
					// 根目录下的表配置文件, 默认为默认数据源的表配置
					if (Files.isSameFile(parentPath, configPath)) {
						JSONObject dbConfig = allDbTablesConfig.getObject(defaultDb);
						if (dbConfig == null) {
							dbConfig = new JSONObject();
							allDbTablesConfig.put(defaultDb, dbConfig);
						}
						readDbTableConfig(dbConfig, defaultDb, path);
						rootDbTablesConfig.put(defaultDb, dbConfig);
						return;
					} else {
						String parentPathString = parentPath.getFileName().toString();
						if (!dbs.contains(parentPathString)) { // 文件名不是数据源
							return;
						}
						JSONObject dbConfig = readDbTableConfig(allDbTablesConfig.getObject(parentPathString),
								parentPathString, path);
						allDbTablesConfig.put(parentPathString, dbConfig);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			throw new DbException(e);
		}
		/*
		 * 待优化
		 */
		if (!JSON.isEmpty(rootDbTablesConfig)) {
			JSONObject defaultDbTablesConfig = rootDbTablesConfig.getObject(defaultDb);
			defaultDbTablesConfig.merge(allDbTablesConfig.getObject(defaultDb));
			allDbTablesConfig.put(defaultDb, defaultDbTablesConfig);
		}
		return allDbTablesConfig;
	}

	/**
	 * 读取单个数据库表配置文件
	 * @param dbConfig
	 * @param dbName
	 * @param tableFilePath
	 * @return
	 */
	private JSONObject readDbTableConfig(JSONObject dbConfig, String dbName, Path tableFilePath) {
		if (dbConfig == null)
			dbConfig = new JSONObject();
		// 默认表名为文件名
		String filename = tableFilePath.getFileName().toString();
		String tableName = filename.substring(0, filename.lastIndexOf("."));
		JSONObject tableConfig = null;
		try {
			tableConfig = JSON.readObject(tableFilePath);
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
		if (tableConfig == null)
			return dbConfig;
		// 如果用户配置了数据库表名
		if (tableConfig.containsKey(TableConfig.TABLE)) {
			tableName = tableConfig.getString(TableConfig.TABLE);
		}
		dbConfig.put(tableName, tableConfig);
		return dbConfig;
	}

	/**
	 * 接收JSON请求 -> 解析JSON为SQL -> 执行SQL -> 返回SQL执行后的结果
	 *
	 * @param json
	 *            描述请求的JSON字符串
	 * @return JSON字符串形式的结果
	 * @throws SQLException
	 */
	public String translate(String json) throws SQLException {
		JSONObject object = new JSONObject(json);
		if (object.containsKey(Type.STRUCT)) {
			String table = object.getString(Type.STRUCT);
			String db = AirContext.getDefaultDb();
			if (table.indexOf(".") != -1) {
				String[] dbTable = table.split("\\.");
				db = dbTable[0];
				table = dbTable[1];
			}
			AirContext.check(db, table);
			return AirContext.getTableConfig(db, table).toString();
		}
		if (object.containsKey(Type.TRANSACTION)) { // 事务操作
			String db = null;
			List<Engine> engines = new ArrayList<>();
			JSONArray transArray = object.getArray(Type.TRANSACTION);
			for (Object transObject : transArray.list()) {
				Engine transEngine = new Engine((JSONObject) transObject).parse();
				if (db == null) {
					db = transEngine.getBuilder().db();
				} else {
					if (!db.equals(transEngine.getBuilder().db())) {
						throw new DbException("暂不支持跨数据库事务");
					}
				}
				engines.add(transEngine);
			}
			return handler.transaction(db, engines).toString();
		}
		Engine engine = new Engine(object).parse();
		return handler.handle(engine).toString();
	}

	/**
	 * 获取数据源, 读取配置文件, 如果配置文件中配置了数据源类型, 则根据配置文件参数构建数据源对象, 否则构建默认的数据源对象并设置参数
	 * 格式:
	 *   {
	 *     "db1":{
	 *       "type":"org.apache.tomcat.jdbc.pool.DataSource", // 连接池类型
	 *       "source":DataSource, // 数据源
	 *       "dialect":"mysql" // 方言
	 *     },
	 *     ...
	 *   }
	 * 
	 * @return
	 */
	private static JSONObject parseDataSource(JSONObject config) {
		JSONObject dssJSONObject = config.getObject(DatacolorConfig.DATASOURCES);
		if (JSON.isEmpty(dssJSONObject)) {
			return null;
		}
		JSONObject dataSources = new JSONObject();
		for (Map.Entry<String, Object> entry : dssJSONObject.entrySet()) {
			String dbName = entry.getKey();
			JSONObject dsJSONOjbect = (JSONObject) entry.getValue();
			String type = dsJSONOjbect.containsKey(DatacolorConfig.Datasource.TYPE)
					? dsJSONOjbect.getString(DatacolorConfig.Datasource.TYPE)
					: DEFAULT_DATASOURCE_POOL;
			// 删除TYPE属性, datasource转换成DataSource. TYPE只是datacolor的标识, 不是DataSource的属性值
			dsJSONOjbect.remove(DatacolorConfig.Datasource.TYPE);
			Class<?> clazz = null;
			try {
				clazz = Class.forName(type);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			DataSource ds = (DataSource) dsJSONOjbect.toBean(clazz);
			JSONObject dataSource = new JSONObject();
			dataSource.put(Datasource.TYPE, type);
			dataSource.put(Datasource.SOURCE, ds);
			dataSource.put(Datasource.DIALECT, DialectFactory.getDialect(ds));
			dataSources.put(dbName, dataSource);
		}
		return dataSources;
	}

	/**
	 * 默认数据库连接池
	 * 
	 * @param url
	 * @param username
	 * @param password
	 * @return
	 */
	private static org.apache.tomcat.jdbc.pool.DataSource defaultDataSource(String driverClassName, String url,
			String username, String password) {
		PoolProperties p = new PoolProperties();
		p.setUrl(url);
		p.setDriverClassName(driverClassName);
		p.setUsername(username);
		p.setPassword(password);

		// 其他默认参数待添加

		org.apache.tomcat.jdbc.pool.DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();
		dataSource.setPoolProperties(p);

		return dataSource;
	}

}
