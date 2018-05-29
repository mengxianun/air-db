package com.mxy.air.db;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.PoolProperties;

import com.mxy.air.json.JSONObject;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.annotation.Transactional;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.interceptors.SQLLogInterceptor;
import com.mxy.air.db.interceptors.TransactionInterceptor;
import com.mxy.air.db.jdbc.DialectFactory;
import com.mxy.air.db.jdbc.JdbcRunner;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;

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
	 * 初始化SQLEngine和JdbcRunner. 如果用户指定了配置文件, 则按该配置文件解析配置, 如果指定的配置文件不存在, 则抛出异常.
	 * 如果用户没有指定配置文件, 则按默认配置文件解析配置. 如果默认配置文件不存在, 则不做处理也不抛出异常. 用户没有指定配置文件的情况下,
	 * 默认配置文件可有可无 数据源: 用户通过构造函数传入的数据源优先, 如果没有传入则使用配置文件中配置的数据源信息
	 */
	public Translator(DataSource dataSource, String configFile) {
		// Datacolor配置, 初始为默认配置
		JSONObject config = new JSONObject(DatacolorConfig.toMap());
		// 读取配置文件
		JSONObject customConfig = read(configFile);
		// 覆盖默认配置
		config.copy(customConfig);
		// 读取数据库表配置文件
		JSONObject tableConfigs = readTableConfigs(config.getString(DatacolorConfig.TABLE_CONFIG_PATH));
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
					bind(SQLSession.class).toConstructor(SQLSession.class.getConstructor(DataSource.class));
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
				// 是否开启事务
				if (config.getBoolean(DatacolorConfig.TRANSACTION)) {
					bindInterceptor(Matchers.any(), Matchers.annotatedWith(Transactional.class),
						new TransactionInterceptor());
					bindInterceptor(Matchers.annotatedWith(Transactional.class), Matchers.any(),
						new TransactionInterceptor());
				}
				// 全局配置
				bind(JSONObject.class).annotatedWith(Names.named("config")).toInstance(config);
				bind(JSONObject.class).annotatedWith(Names.named("tableConfigs")).toInstance(tableConfigs);
			}

		});
		this.handler = injector.getInstance(SQLHandler.class);
		this.engine = injector.getInstance(Engine.class);
		
	}

	/**
	 * 读取配置文件
	 * 
	 * @param configFile
	 * @return
	 */
	private JSONObject read(String configFile) {
		URL url = this.getClass().getClassLoader().getResource(configFile == null ? DEFAULT_CONFIG_FILE : configFile);
		if (url == null) {
			return new JSONObject();
		}
		FileSystem fileSystem = null;
		try {
			URI uri = url.toURI();
			Path configPath = null;
			if (uri.toString().indexOf("!") != -1) { // jar
				String[] pathArray = uri.toString().split("!", 2);
				fileSystem = FileSystems.newFileSystem(URI.create(pathArray[0]), new HashMap<>());
				configPath = fileSystem.getPath(pathArray[1].replaceAll("!", ""));
			} else {
				configPath = Paths.get(uri);
			}
			String json = new String(Files.readAllBytes(configPath), Charset.defaultCharset());
			return new JSONObject(json);
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		} finally {
			try {
				if (fileSystem != null) {
					fileSystem.close();
				}
			} catch (IOException e) {
				//
			}
		}
	}

	/**
	 * 读取数据库表配置文件
	 * 
	 * @param tablesPath
	 */
	private JSONObject readTableConfigs(String tablesPath) {
		JSONObject tableConfigs = new JSONObject();
		URL url = this.getClass().getClassLoader()
				.getResource(tablesPath == null ? DatacolorConfig.TABLE_CONFIG_PATH.value().toString() : tablesPath);
		if (url == null) {
			return tableConfigs;
		}
		FileSystem fileSystem = null;
		Stream<Path> stream = null;
		try {
			URI uri = url.toURI();
			Path configPath = null;
			if (uri.toString().indexOf("!") != -1) { // jar
				String[] pathArray = uri.toString().split("!", 2);
				fileSystem = FileSystems.newFileSystem(URI.create(pathArray[0]), new HashMap<>());
				configPath = fileSystem.getPath(pathArray[1].replaceAll("!", ""));
			} else {
				configPath = Paths.get(uri);
			}
			stream = Files.list(configPath);
			stream.filter(Files::isRegularFile).forEach(path -> {
				try {
					String tableConfig = new String(Files.readAllBytes(path), Charset.defaultCharset());
					if (tableConfig.isEmpty()) {
						return;
					}
					String filename = path.getFileName().toString();
					// 文件名为表名
					String tableName = filename.substring(0, filename.lastIndexOf("."));
					tableConfigs.put(tableName, new JSONObject(tableConfig));
				} catch (IOException e) {
					throw new DbException(e);
				}
			});
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		} finally {
			try {
				if (fileSystem != null) {
					fileSystem.close();
				}
			} catch (IOException e) {
				//
			}
			if (stream != null) {
				stream.close();
			}
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
		SQLBuilder builder = engine.parse(object);
		Type type = engine.getType(object);
		return handler.handle(type, builder);
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
