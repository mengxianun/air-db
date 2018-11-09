package com.mxy.air.db;

import static com.mxy.air.db.Structure.FIELDS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.mxy.air.db.Structure.Operator;
import com.mxy.air.db.Structure.Template;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.ResultConverter.ExcelResultConverter;
import com.mxy.air.db.ResultConverter.HtmlResultConverter;
import com.mxy.air.db.ResultConverter.PDFResultConverter;
import com.mxy.air.db.ResultConverter.WordResultConverter;
import com.mxy.air.db.annotation.SQLLog;
import com.mxy.air.db.builder.Condition;
import com.mxy.air.db.builder.Join;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.DatacolorConfig.Datasource;
import com.mxy.air.db.config.DatacolorConfig.Es;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.es.EsHandler;
import com.mxy.air.db.interceptors.SQLLogInterceptor;
import com.mxy.air.db.jdbc.Dialect;
import com.mxy.air.db.jdbc.DialectFactory;
import com.mxy.air.db.jdbc.dialect.ElasticsearchDialect;
import com.mxy.air.json.JSON;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

/**
 * 转换器, 将Json转换为SQL并执行数据库操作, 然后返回结果
 * 
 * @author mengxiangyun
 *
 */
public class Translator {

	private static final Logger logger = LoggerFactory.getLogger(Translator.class);

	public static final String DEFAULT_CONFIG_FILE = "xiaolongnv.json";

	public static final String DEFAULT_DATASOURCE_POOL = "com.alibaba.druid.pool.DruidDataSource";

	private SQLHandler handler;

	private EsHandler esHandler;

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
		// 默认数据源
		String defaultDb = config.getString(DatacolorConfig.DEFAULT_DATASOURCE);
		JSONObject dbObject = config.getObject(DatacolorConfig.DATASOURCES);
		if (defaultDb == null && dbObject != null) {
			defaultDb = dbObject.getFirst().getKey();
			config.put(DatacolorConfig.DEFAULT_DATASOURCE, defaultDb);
		}
		Set<String> dbs = dbObject == null ? new HashSet<>() : dbObject.keySet();
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
			JSONObject dataSourceObject = new JSONObject();
			dataSourceObject.put(Datasource.SOURCE, dataSource);
			if (dataSource instanceof DruidDataSource) {
				String url = ((DruidDataSource) dataSource).getUrl();
				dataSourceObject.put(Datasource.DIALECT, DialectFactory.getDialect(url));
			} else {
				dataSourceObject.put(Datasource.DIALECT, DialectFactory.getDialect(dataSource));
			}
			dss = new JSONObject().put(DatacolorConfig.DEFAULT_DATASOURCE.toString().toLowerCase(), dataSourceObject);
			defaultDb = DatacolorConfig.DEFAULT_DATASOURCE.toString().toLowerCase();
			dbs.add(defaultDb);

		}
		config.put(DatacolorConfig.DATASOURCES, dss);
		/**
		 * 读取数据库表配置文件
		 */
		String tablesConfigPath = config.containsKey(DatacolorConfig.DB_TABLE_CONFIG_PATH)
				? config.getString(DatacolorConfig.DB_TABLE_CONFIG_PATH)
				: DatacolorConfig.DB_TABLE_CONFIG_PATH.value().toString();
		JSONObject dbTableConfig;
		try {
			dbTableConfig = readAllDbTablesConfig(tablesConfigPath, dbs, defaultDb);
		} catch (IOException | URISyntaxException e) {
			throw new DbException(e);
		}
		config.put(DatacolorConfig.DB_TABLE_CONFIG, dbTableConfig);

		/*
		 * 绑定依赖注入对象
		 */
		Injector injector = Guice.createInjector(new AbstractModule() {

			@Override
			protected void configure() {
				for (String db : dss.keySet()) {
					JSONObject dataSourceConfig = dss.getObject(db);
					Dialect dialect = (Dialect) dataSourceConfig.get(Datasource.DIALECT);
					/*
					 * ES
					 */
					if (dialect instanceof ElasticsearchDialect) {
						String url = dataSourceConfig.getString(Datasource.URL);
						List<HttpHost> httpHosts = parseEsHttpHost(url, dataSourceConfig.getInt(Es.HTTPPORT));
						RestClient client = RestClient.builder(httpHosts.toArray(new HttpHost[] {})).build();
						bind(RestClient.class).annotatedWith(Names.named(db)).toInstance(client);
					}
					/*
					 * 由于自己创建的(即非Guice创建的实例)Guice绑定后无法进行AOP操作, 
					 * 所以先绑定SQLSession对象(由Guice创建), 
					 * 然后再获取Guice创建的每个SQLSession进行数据源设置, 设置数据源的操作在Aircontext.init()方法中执行
					 */
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
				/*
				 * ES
				 */
				bind(EsHandler.class).in(Singleton.class);
			}

		});
		this.handler = injector.getInstance(SQLHandler.class);
		this.esHandler = injector.getInstance(EsHandler.class);
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
			JSONObject dbTableConfig = AirContext.getDbConfig(db);
			if (dbTableConfig.size() == 0) {
				AirContext.addDbTableConfig(db, dbTableConfig);
			}
			Dialect dialect = AirContext.getDialect(db);

			if (dialect instanceof ElasticsearchDialect) { // ES数据源
				initESTableInfo(db, dbTableConfig);
				break;
			} else { // 传统数据源
				initRMDBTableInfo(db, dbTableConfig);
			}

		}
	}

	/**
	 * 初始化传统数据源数据表信息
	 * @param db
	 * @param dbTableConfig
	 * @throws SQLException
	 */
	private void initRMDBTableInfo(String db, JSONObject dbTableConfig) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(db);
		String realDbName = sqlSession.getDbName();
		String sql = "select t.table_name, t.table_comment, c.column_name, c.data_type, c.column_key, c.extra, c.column_comment from information_schema.tables t left join information_schema.columns c on t.table_name = c.table_name where t.table_schema = ? and c.table_schema = ?";
		List<Map<String, Object>> tableInfos = sqlSession.list(sql, new String[] { realDbName, realDbName });
		for (Map<String, Object> tableInfo : tableInfos) {
			String tableName = tableInfo.get("table_name").toString();
			String tableComment = tableInfo.get("table_comment").toString();
			String column = tableInfo.get("column_name").toString();
			String dataType = tableInfo.get("data_type").toString();
			String columnKey = tableInfo.get("column_key").toString();
			//				String extra = tableInfo.get("extra").toString();
			String columnComment = tableInfo.get("column_comment").toString();
			// 获取数据库表配置
			JSONObject tableConfig = null;
			if (dbTableConfig.containsKey(tableName)) {
				tableConfig = dbTableConfig.getObject(tableName);
			} else {
				tableConfig = new JSONObject();
				dbTableConfig.put(tableName, tableConfig);
			}
			// 主键
			if (columnKey.equals("PRI")) {
				tableConfig.put(TableConfig.PRIMARY_KEY, column);
			}
			// 数据库表注释
			tableConfig.put(TableConfig.COMMENT, tableComment);
			// 获取所有列配置
			JSONObject columns = null;
			if (tableConfig.containsKey(TableConfig.COLUMNS)) {
				columns = tableConfig.getObject(TableConfig.COLUMNS);
			} else {
				columns = new JSONObject();
				tableConfig.put(TableConfig.COLUMNS, columns);
			}
			// 获取列配置
			JSONObject columnConfig = null;
			if (columns.containsKey(column)) {
				columnConfig = columns.getObject(column);
			} else {
				columnConfig = new JSONObject();
				columns.put(column, columnConfig);
			}
			// 更新列配置
			columnConfig.put(Column.TYPE, dataType);
			columnConfig.put(Column.COMMENT, columnComment);
		}
	}

	private void initESTableInfo(String db, JSONObject dbTableConfig) {
		Injector injector = AirContext.getInjector();
		RestClient client = injector.getInstance(Key.get(RestClient.class, Names.named(db)));
		try {
			// 查询所有索引的mapping
			Response response = client.performRequest("GET", "/_all/_mapping");
			String responseBody = EntityUtils.toString(response.getEntity());
			JSONObject allMappings = new JSONObject(responseBody);
			for (String index : allMappings.keySet()) {
				JSONObject indexConfig = new JSONObject();
				dbTableConfig.put(index, indexConfig);
				JSONObject indexObject = allMappings.getObject(index);
				JSONObject mappings = indexObject.getObject("mappings");
				if (mappings.size() > 0) {
					JSONObject type = (JSONObject) mappings.entrySet().iterator().next().getValue();
					indexConfig.put(TableConfig.COLUMNS, type.getObject("properties"));
				}
			}
		} catch (IOException e) {
			logger.error(String.format("读取ES索引mapping失败"), e);
		}
	}

	private List<HttpHost> parseEsHttpHost(String url, int httpPort) {
		List<HttpHost> httpHosts = new ArrayList<>();
		String pattern = "\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d{1,5})?";
		Pattern compiledPattern = Pattern.compile(pattern);
		Matcher matcher = compiledPattern.matcher(url);
		while (matcher.find()) {
			String[] ipPort = matcher.group().split(":");
			String ip = ipPort[0];
			int port = httpPort != 0 ? httpPort : 9200;
			HttpHost httpHost = new HttpHost(ip, port);
			httpHosts.add(httpHost);
		}
		return httpHosts;
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
		return translateToJson(json).toString();
	}

	public JSON translateToJson(String json, Condition... conditions)
			throws SQLException {
		AirParser parser = new AirParser(json);
		JSONObject object = parser.getObject();
		if (object.containsKey(Type.STRUCT)) {
			String table = object.getString(Type.STRUCT);
			String db = AirContext.getDefaultDb();
			if (table.indexOf(".") != -1) {
				String[] dbTable = table.split("\\.");
				db = dbTable[0];
				table = dbTable[1];
			}
			AirContext.check(db, table);
			return AirContext.getTableConfig(db, table);
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
			AirContext.inState(db);
			JSONArray result = handler.transaction(db, engines);
			AirContext.outState();
			return result;
		}
		// ES原生JSON查询
		if (AirContext.isElasticsearch(parser.getDb())) {
			if (object.containsKey(Structure.NATIVE)
					&& (object.containsKey(Type.SELECT) || object.containsKey(Type.QUERY))) {
				try {
					JSON result = esHandler.handle(parser.getDb(), parser.getTable(),
							object.getObject(Structure.NATIVE));
					return result;
				} catch (IOException e) {
					e.printStackTrace();
					throw new DbException(e.getMessage());
				}
			}
		}
		Engine engine = new Engine(object).parse();
		SQLBuilder sqlBuilder = engine.getBuilder();
		String db = sqlBuilder.db();
		/*
		 * 额外条件处理
		 */
		if (conditions != null && conditions.length > 0) {
			List<Condition> filterConditions = new ArrayList<>();
			for (Condition condition : conditions) {
				if (condition == null) {
					continue;
				}
				condition.setDb(db);
				boolean joinAdded = false;
				if (sqlBuilder.table().equals(condition.getTable())) {
					joinAdded = true;
					condition.setAlias(sqlBuilder.alias);
					filterConditions.add(condition);
				} else if (sqlBuilder.joins() != null) {
					for (Join join : sqlBuilder.joins()) {
						if (join.getTargetTable().equals(condition.getTable())) {
							joinAdded = true;
							condition.setAlias(join.getTargetAlias());
							filterConditions.add(condition);
							break;
						}
					}
				}
				if (!joinAdded) {
					String tableAlias = SQLBuilder.getRandomString(6) + "_" + condition.getTable();
					condition.setAlias(tableAlias);
					boolean addJoin = sqlBuilder.addJoin(db, condition.getTable(), tableAlias);
					if (addJoin) {
						filterConditions.add(condition);
					}
				}
			}
			if (!filterConditions.isEmpty()) {
				sqlBuilder.addCondition(new Condition(null, null, null, Operator.AND, null, null, filterConditions));
			}
		}
		AirContext.inState(db);
		JSON result = handler.handle(engine);
		AirContext.outState();
		return result;
	}

	/**
	 * 将请求JSON解析后的结果写入流, 
	 * @param json
	 * @return 输入流
	 * @throws SQLException
	 * @throws IOException
	 */
	public InputStream translateToStream(String json, Condition... conditions) throws SQLException, IOException {
		AirParser parser = new AirParser(json);
		JSONObject jsonObject = parser.getObject();
		String db = parser.getDb();
		String table = parser.getTable();
		if (parser.getTemplate() != null && parser.getTemplate() == Template.CSV) { // 导出CSV模板
			JSONObject tableConfig = AirContext.getTableConfig(db, table);
			String primaryKey = tableConfig.getString(TableConfig.PRIMARY_KEY);
			JSONObject columnsConfig = tableConfig.getObject(TableConfig.COLUMNS);
			String[] columns = columnsConfig.keySet().toArray(new String[] {});
			if (jsonObject.containsKey(Structure.FIELDS)) { // 指定了列
				columns = jsonObject.getArray(FIELDS).toStringArray();
			}
			// CSV头部(列)
			List<String> columnHeader = new ArrayList<>();
			// CSV头部(列显示名称)
			List<String> columnHeaderDisplay = new ArrayList<>();
			for (String column : columns) {
				if (column.equals(primaryKey)) { // CSV模板不导出主键
					continue;
				}
				columnHeader.add(column);
				JSONObject columnConfig = columnsConfig.getObject(column);
				String columnDisplay = columnConfig.getString(TableConfig.Column.DISPLAY);
				if (columnConfig.containsKey(TableConfig.Column.CODE)) {
					JSONObject columnCode = columnConfig.getObject(TableConfig.Column.CODE);
					StringBuilder builder = new StringBuilder();
					builder.append(columnDisplay).append("(");
					String[] codeValues = columnCode.keySet().stream().map(k -> k + ":" + columnCode.get(k))
							.toArray(String[]::new);
					builder.append(String.join(",", codeValues));
					builder.append(")");
					columnHeaderDisplay.add(builder.toString());
				} else {
					columnHeaderDisplay.add(columnDisplay);
				}
			}
			// CSV头部, 数据表每个列的DISPLAY
			//			String[] header = columnsConfig.values().stream()
			//					.map(o -> ((JSONObject) o).getString(TableConfig.Column.DISPLAY)).toArray(String[]::new);
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			byteArrayOutputStream.write(0xef);
			byteArrayOutputStream.write(0xbb);
			byteArrayOutputStream.write(0xbf);
			CSVWriterBuilder csvWriterBuilder = new CSVWriterBuilder(
					new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8));
			try (ICSVWriter icsvWriter = csvWriterBuilder.build()) {
				icsvWriter.writeNext(columnHeader.toArray(new String[] {}));
				icsvWriter.writeNext(columnHeaderDisplay.toArray(new String[] {}));
			}
			return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
		} else if (jsonObject.containsKey(Structure.RESULT)) { // 导出CSV数据
			String result = jsonObject.getString(Structure.RESULT);
			if (result.equalsIgnoreCase(Structure.Result.CSV.toString())) {
				JSON jsonResult = translateToJson(json, conditions);
				List<Map<String, Object>> resultList = null;
				if (jsonObject.containsKey(Structure.LIMIT)) { // 分页
					resultList = ((JSONObject) jsonResult).getArray(PageResult.ATTRIBUTE.DATA).toMapList();
				} else {
					if (jsonResult instanceof JSONObject) {
						resultList = new ArrayList<>();
						resultList.add(((JSONObject) jsonResult).toMap());
					} else {
						resultList = ((JSONArray) jsonResult).toMapList();
					}
				}
				/*
				 * CSV需要的格式数据
				 */
				List<String[]> csvData = new ArrayList<>();
				// CSV头部(列)
				List<String> columnHeader = new ArrayList<>();
				// CSV头部(列显示名称)
				List<String> columnHeaderDisplay = new ArrayList<>();

				if (resultList.size() > 0) {
					/*
					 * 构建头部数据
					 */
					Map<String, Object> firstRecord = resultList.iterator().next();
					for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
						String column = entry.getKey();
						Object value = entry.getValue();
						if (value instanceof Map) { // 关联对象的情况暂不处理
							// header.addAll(buildHeader((JSONObject) value, db, column));
						} else {
							columnHeader.add(column);
							JSONObject columnConfig = AirContext.getColumnsConfig(db, table).getObject(column);
							if (columnConfig == null || columnConfig.size() == 0) {
								columnHeaderDisplay.add(column);
							} else {
								String columnDisplay = Strings
										.nullToEmpty(columnConfig.getString(TableConfig.Column.DISPLAY));
								if (columnConfig.containsKey(TableConfig.Column.CODE)) {
									JSONObject columnCode = columnConfig.getObject(TableConfig.Column.CODE);
									StringBuilder builder = new StringBuilder();
									builder.append(columnDisplay).append("(");
									String[] codeValues = columnCode.keySet().stream()
											.map(k -> k + ":" + columnCode.get(k)).toArray(String[]::new);
									builder.append(String.join(",", codeValues));
									builder.append(")");
									columnHeaderDisplay.add(builder.toString());
								} else {
									columnHeaderDisplay.add(columnDisplay);
								}
							}
						}
					}
					// 构建具体数据
					if (!resultList.isEmpty()) {
						csvData = resultList.stream()
								.map(o -> buildRecord(o, db, table).toArray(new String[] {}))
								.collect(Collectors.toList());
					}
				}
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				byteArrayOutputStream.write(0xef);
				byteArrayOutputStream.write(0xbb);
				byteArrayOutputStream.write(0xbf);
				CSVWriterBuilder csvWriterBuilder = new CSVWriterBuilder(
						new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8));
				try (ICSVWriter icsvWriter = csvWriterBuilder.build()) {
					icsvWriter.writeNext(columnHeader.toArray(new String[] {}));
					icsvWriter.writeNext(columnHeaderDisplay.toArray(new String[] {}));
					icsvWriter.writeAll(csvData);
				}
				return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
			}
		}
		throw new DbException("请求JSON解析失败");
	}

	private static List<String> getHeaderDisplayNames(String db, String table, List<Map<String, Object>> resultList) {
		List<String> columnHeaderDisplay = new ArrayList<>();
		if (resultList.size() > 0) {
			Map<String, Object> firstRecord = resultList.iterator().next();
			for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
				String column = entry.getKey();
				Object value = entry.getValue();
				if (value instanceof Map) { // 关联对象的情况暂不处理
					// header.addAll(buildHeader((JSONObject) value, db, column));
				} else {
					JSONObject columnConfig = AirContext.getColumnsConfig(db, table).getObject(column);
					if (columnConfig == null || columnConfig.size() == 0) {
						columnHeaderDisplay.add(column);
					} else {
						String columnDisplay = Strings
							.nullToEmpty(columnConfig.getString(TableConfig.Column.DISPLAY));
						if (columnConfig.containsKey(TableConfig.Column.CODE)) {
							JSONObject columnCode = columnConfig.getObject(TableConfig.Column.CODE);
							StringBuilder builder = new StringBuilder();
							builder.append(columnDisplay).append("(");
							String[] codeValues = columnCode.keySet().stream()
								.map(k -> k + ":" + columnCode.get(k)).toArray(String[]::new);
							builder.append(String.join(",", codeValues));
							builder.append(")");
							columnHeaderDisplay.add(builder.toString());
						} else {
							columnHeaderDisplay.add(columnDisplay);
						}
					}
				}
			}
		}
		return columnHeaderDisplay;
	}
	public InputStream translateExcel(String json, Condition... conditions) throws SQLException, IOException {
		AirParser parser = new AirParser(json);
		String db = parser.getDb();
		String table = parser.getTable();
		List<Map<String, Object>> resultList = getListMapResult(json, conditions);
		List<String> header = getHeaderDisplayNames(db, table, resultList);

		ExcelResultConverter excelDC = ExcelResultConverter.getInstance(header, AirContext.getColumnsConfig(db, table));

		return excelDC.export(resultList);
	}
	public InputStream translateWord(String json, Condition... conditions) throws Exception {
		AirParser parser = new AirParser(json);
		String db = parser.getDb();
		String table = parser.getTable();
		List<Map<String, Object>> resultList = getListMapResult(json, conditions);
		List<String> header = getHeaderDisplayNames(db, table, resultList);

		WordResultConverter excelDC = WordResultConverter.getInstance(header, AirContext.getColumnsConfig(db, table));

		return excelDC.export(resultList);
	}
	public InputStream translatePdf(String json, Condition... conditions) throws SQLException, IOException {
		AirParser parser = new AirParser(json);
		String db = parser.getDb();
		String table = parser.getTable();
		List<Map<String, Object>> resultList = getListMapResult(json, conditions);
		List<String> header = getHeaderDisplayNames(db, table, resultList);

		PDFResultConverter excelDC = PDFResultConverter.getInstance(header, AirContext.getColumnsConfig(db, table));

		return excelDC.export(resultList);
	}
	public InputStream translateHtml(String json, Condition... conditions) throws Exception {
		AirParser parser = new AirParser(json);
		String db = parser.getDb();
		String table = parser.getTable();
		List<Map<String, Object>> resultList = getListMapResult(json, conditions);
		List<String> header = getHeaderDisplayNames(db, table, resultList);

		HtmlResultConverter excelDC = HtmlResultConverter.getInstance(header, AirContext.getColumnsConfig(db, table));

		return excelDC.export(resultList);
	}

	private List<Map<String, Object>> getListMapResult(String json, Condition... conditions) throws SQLException, IOException {
		AirParser parser = new AirParser(json);
		JSONObject jsonObject = parser.getObject();
		JSON jsonResult = translateToJson(json, conditions);
		List<Map<String, Object>> resultList = null;
		if (jsonObject.containsKey(Structure.LIMIT)) { // 分页
			resultList = ((JSONObject) jsonResult).getArray(PageResult.ATTRIBUTE.DATA).toMapList();
		} else {
			if (jsonResult instanceof JSONObject) {
				resultList = new ArrayList<>();
				resultList.add(((JSONObject) jsonResult).toMap());
			} else {
				resultList = ((JSONArray) jsonResult).toMapList();
			}
		}
		return resultList;
	}

	private List<String> buildRecord(Map<String, Object> record, String db, String table) {
		List<String> csvRecord = new ArrayList<>();
		for (Map.Entry<String, Object> entry : record.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			if (value == null) {
				csvRecord.add("");
			} else if (value instanceof Map) { // 关联对象的情况暂不处理
				// csvRecord.addAll(buildRecord((JSONObject) value));
			} else {
				JSONObject columnConfig = AirContext.getColumnsConfig(db, table).getObject(column);
				if (columnConfig == null || columnConfig.size() == 0) {
					csvRecord.add(value.toString());
				} else if (columnConfig.containsKey(TableConfig.Column.CODE)) {
					JSONObject columnCode = columnConfig.getObject(TableConfig.Column.CODE);
					csvRecord.add(columnCode.get(value.toString()).toString());
				} else if (columnConfig.containsKey(TableConfig.Column.FORMAT)) {
					String columnType = columnConfig.getString(TableConfig.Column.TYPE);
					JSONObject columnFormat = columnConfig.getObject(TableConfig.Column.FORMAT);
					if (columnFormat.containsKey(TableConfig.Format.DATETIME)) {
						String pattern = columnFormat.getString(TableConfig.Format.DATETIME);
						String formatedValue = "";
						if ("bigint".equals(columnType)) {
							Instant instant = Instant.ofEpochMilli(Long.valueOf(value.toString()));
							DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
							formatedValue = fmt.format(instant.atZone(ZoneId.systemDefault()));
						} else {
							try {
								LocalDateTime datetime = LocalDateTime.parse(value.toString());
								formatedValue = datetime.format(DateTimeFormatter.ofPattern(pattern));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						csvRecord.add(formatedValue);
					}
				} else {
					csvRecord.add(value.toString());
				}
			}
		}
		return csvRecord;
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
			// 删除TYPE属性, datasource转换成DataSource. TYPE只是air-db的标识, 不是DataSource的属性值
			dsJSONOjbect.remove(DatacolorConfig.Datasource.TYPE);
			JSONObject dataSourceConfig = new JSONObject();
			dataSourceConfig.put(Datasource.TYPE, type);
			String url = dsJSONOjbect.getString("url");
			Dialect dialect = DialectFactory.getDialect(url);
			dataSourceConfig.put(Datasource.DIALECT, dialect);
			DataSource dataSource = null;
			if (dialect instanceof ElasticsearchDialect) {
				Properties properties = new Properties();
				for (Map.Entry<String, Object> dsEntry : dsJSONOjbect.entrySet()) {
					if (dsEntry.getKey().equalsIgnoreCase(Es.HTTPPORT.toString())) {
						dataSourceConfig.put(Es.HTTPPORT, dsEntry.getValue());
						continue;
					}
					properties.put(dsEntry.getKey(), dsEntry.getValue());
				}
				try {
					dataSource = ElasticSearchDruidDataSourceFactory.createDataSource(properties);
				} catch (Exception e) {
					throw new DbException(e);
				}
			} else {
				Class<?> clazz = null;
				try {
					clazz = Class.forName(type);
				} catch (ClassNotFoundException e) {
					throw new DbException(e);
				}
				dataSource = (DataSource) dsJSONOjbect.toBean(clazz);
			}
			dataSourceConfig.put(Datasource.SOURCE, dataSource);
			dataSourceConfig.put(Datasource.URL, url);
			dataSources.put(dbName, dataSourceConfig);
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
	private static DruidDataSource defaultDataSource(String driverClassName, String url,
			String username, String password) {
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setDriverClassName(driverClassName);
		dataSource.setUrl(url);
		dataSource.setUsername(username);
		dataSource.setPassword(password);
		// 其他默认参数待添加
		return dataSource;
	}

}
