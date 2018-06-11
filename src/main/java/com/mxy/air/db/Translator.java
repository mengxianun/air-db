package com.mxy.air.db;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import com.mxy.air.db.annotation.Transactional;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.interceptors.SQLLogInterceptor;
import com.mxy.air.db.interceptors.TransactionInterceptor;
import com.mxy.air.db.jdbc.DialectFactory;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.mxy.air.db.jdbc.handlers.MapListHandler;
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

	public static final String DEFAULT_CONFIG_FILE = "datacolor.json";

	public static final String DEFAULT_DATASOURCE_POOL = "org.apache.tomcat.jdbc.pool.DataSource";

	private SQLHandler handler;

	private Engine engine;

	/**
	 * 无参构造器, 从默认的配置文件构建SQLTranslator
	 */
	public Translator() {
		this(null, null);
	}

	/**
	 * 从配置文件构建SQLTranslator
	 * 
	 * @param configFile
	 */
	public Translator(String configFile) {
		this(null, configFile);
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

	public Translator(DataSource dataSource) {
		this(dataSource, null);
	}

	/**
	 * 如果用户指定了配置文件, 则按该配置文件解析配置
	 * 如果指定的配置文件不存在, 则抛出异常
	 * 如果用户没有指定配置文件, 则按默认配置文件解析配置.
	 * 如果默认配置文件不存在, 则不做处理也不抛出异常. 用户没有指定配置文件的情况下, 默认配置文件可有可无 数据源: 用户通过构造函数传入的数据源优先,
	 * 如果没有传入则使用配置文件中配置的数据源信息
	 */
	public Translator(DataSource dataSource, String configFile) {
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
		String tablesConfigPath = config.containsKey(DatacolorConfig.TABLES_CONFIG_PATH)
				? config.getString(DatacolorConfig.TABLES_CONFIG_PATH)
				: DatacolorConfig.TABLES_CONFIG_PATH.value().toString();
		JSONObject tablesConfig;
		try {
			tablesConfig = readTablesConfig(tablesConfigPath);
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		}
		// 数据源
		if (dataSource == null) {
			dataSource = getDataSource(config);
			if (dataSource == null) {
				throw new DbException("数据源为空");
			}
		}
		SQLBuilder.dialect(DialectFactory.getDialect(dataSource));
		final DataSource finalDataSource = dataSource;
		/*
		 * 绑定依赖注入对象
		 */
		Injector injector = Guice.createInjector(new AbstractModule() {

			@Override
			protected void configure() {
				bind(DataSource.class).toInstance(finalDataSource);
				bind(Engine.class).in(Singleton.class);
				bind(SQLHandler.class).in(Singleton.class);
				bind(JdbcRunner.class).toInstance(new JdbcRunner(finalDataSource));
				try {
					bind(SQLSession.class).toConstructor(SQLSession.class.getConstructor(DataSource.class))
							.in(Singleton.class);
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				}
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
				TransactionInterceptor transactionInterceptor = new TransactionInterceptor();
				bind(TransactionInterceptor.class).toInstance(transactionInterceptor);
				// 是否开启事务
				if (config.getBoolean(DatacolorConfig.TRANSACTION)) {
					bindInterceptor(Matchers.any(), Matchers.annotatedWith(Transactional.class),
							transactionInterceptor);
					bindInterceptor(Matchers.annotatedWith(Transactional.class), Matchers.any(),
							transactionInterceptor);
				}
				// 全局配置
				bind(JSONObject.class).annotatedWith(Names.named("config")).toInstance(config);
				bind(JSONObject.class).annotatedWith(Names.named("tableConfigs")).toInstance(tablesConfig);
			}

		});
		this.handler = injector.getInstance(SQLHandler.class);
		this.engine = injector.getInstance(Engine.class);
		try {
			initTableInfo(dataSource, injector.getInstance(JdbcRunner.class), tablesConfig);
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
	private void initTableInfo(DataSource dataSource, JdbcRunner jdbcRunner, JSONObject tableConfigs)
			throws SQLException {
		String dbName = getDatabaseName(dataSource);
		String sql = "select * from information_schema.columns where table_schema = ?";
		List<Map<String, Object>> tableInfos = jdbcRunner.query(sql, new MapListHandler(), dbName);
		for (Map<String, Object> tableInfo : tableInfos) {
			String tableName = tableInfo.get("TABLE_NAME").toString();
			String column = tableInfo.get("COLUMN_NAME").toString();
			if (tableConfigs.containsKey(tableName)) {
				JSONObject tableConfig = tableConfigs.getObject(tableName);
				if (tableConfig.containsKey(TableConfig.COLUMNS)) {
					JSONObject columns = tableConfig.getObject(TableConfig.COLUMNS);
					if (!columns.containsKey(column)) {
						columns.put(column, new JSONObject());
					}
				} else {
					JSONObject columns = new JSONObject();
					columns.put(column, new JSONObject());
					tableConfig.put(TableConfig.COLUMNS, columns);
				}
			} else {
				JSONObject tableConfig = new JSONObject();
				JSONObject columns = new JSONObject();
				columns.put(column, new JSONObject());
				tableConfig.put(TableConfig.COLUMNS, columns);
				tableConfigs.put(tableName, tableConfig);
			}
		}
	}

	/**
	 * 获取数据库名称
	 * @param dataSource
	 * @return
	 */
	public String getDatabaseName(DataSource dataSource) {
		try (Connection conn = dataSource.getConnection()) {
			return conn.getCatalog();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 读取数据库表配置文件
	 * 
	 * @param tablesPath
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	private JSONObject readTablesConfig(String tablesConfigPath) throws IOException, URISyntaxException {
		JSONObject tableConfigs = new JSONObject();
		Path configPath = JSON.getPath(tablesConfigPath);
		if (configPath == null)
			return tableConfigs;
		try (Stream<Path> stream = Files.list(configPath)) {
			stream.filter(Files::isRegularFile).forEach(path -> {
				try {
					String tableConfigString = new String(Files.readAllBytes(path), Charset.defaultCharset());
					if (tableConfigString.isEmpty()) {
						return;
					}
					String filename = path.getFileName().toString();
					// 默认表名为文件名
					String tableName = filename.substring(0, filename.lastIndexOf("."));
					JSONObject tableConfig = new JSONObject(tableConfigString);
					// 如果用户配置了数据库表名
					if (tableConfig.containsKey(TableConfig.TABLE)) {
						tableName = tableConfig.getString(TableConfig.TABLE);
					}
					tableConfigs.put(tableName, tableConfig);
				} catch (IOException e) {
					throw new DbException(e);
				}
			});
		} catch (IOException e) {
			throw new DbException(e);
		}
		return tableConfigs;
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
		Type type = engine.getType(object);
		if (type == Type.TRANSACTION) { // 事务操作
			List<RequestAction> actions = new ArrayList<>();
			JSONArray transArray = object.getArray(Type.TRANSACTION);
			for (Object transObject : transArray.list()) {
				actions.add(engine.parse((JSONObject) transObject));
			}
			return handler.transaction(actions).toString();
		}
		RequestAction action = engine.parse(object);
		return handler.handle(action).toString();
	}

	/**
	 * 获取数据源, 读取配置文件, 如果配置文件中配置了数据源类型, 则根据配置文件参数构建数据源对象, 否则构建默认的数据源对象并设置参数
	 * 
	 * @return
	 */
	private static DataSource getDataSource(JSONObject config) {
		JSONObject datasource = config.getObject(DatacolorConfig.DATASOURCE);
		String type = datasource.containsKey(DatacolorConfig.Datasource.TYPE)
				? datasource.getString(DatacolorConfig.Datasource.TYPE)
				: DEFAULT_DATASOURCE_POOL;
		// 删除TYPE属性, datasource转换成DataSource. TYPE只是datacolor的标识, 不是DataSource的属性值
		datasource.remove(DatacolorConfig.Datasource.TYPE);
		Class<?> clazz = null;
		try {
			clazz = Class.forName(type);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		DataSource dataSource = (DataSource) datasource.toBean(clazz);
		return dataSource;
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
