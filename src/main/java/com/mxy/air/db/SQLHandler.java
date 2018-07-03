package com.mxy.air.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Select;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.config.DatacolorConfig;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.jdbc.trans.Atom;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * SQL逻辑处理器
 * 
 * @author mengxiangyun
 *
 */
public class SQLHandler {

	@Inject
	private DataProcessor processor;

	@Inject
	private DataRenderer renderer;

	public Object handle(Engine engine) throws SQLException {
		Type type = engine.getType();
		SQLBuilder builder = engine.getBuilder();
		// 构建SQL语句
		builder.build();
		/////// ES 生成原生SQL
		if (AirContext.isElasticsearch(builder.db())) {
			builder.nativeSQL();
		}
		/////////////////////
		switch (type) {
		case DETAIL:
			return detail(builder);
		case QUERY:
		case SELECT:
			return query(builder);
		case INSERT:
			return insert(builder);
		case UPDATE:
			return update(builder);
		case DELETE:
			return delete(builder);

		default:
			break;
		}
		throw new DbException("操作类型错误[" + type + "]");
	}

	/**
	 * 查询单个记录
	 * 
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	public Object detail(SQLBuilder builder) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(builder.db());
		Map<String, Object> detail = sqlSession.detail(builder.sql(), builder.params().toArray());
		// 结果渲染
		JSONObject columnsConfig = AirContext.getColumnsConfig(builder.db(), builder.table());
		renderer.render(detail, columnsConfig);
		return new JSONObject(detail);
	}

	/**
	 * 查询多条记录
	 * 
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	public Object query(SQLBuilder builder) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(builder.db());
		List<Map<String, Object>> list = sqlSession.list(builder.sql(), builder.params().toArray());
		// 结果渲染
		JSONArray data = renderer.render(list, builder);
		// 分页查询, 查询总记录数
		if (builder.limit() != null) {
			String countSql = ((Select) builder).getCountSql();
			Object[] countParams = ((Select) builder).getWhereParams().toArray();
			long total = sqlSession.count(countSql, countParams);
			long[] limit = builder.limit();
			JSONObject result = PageResult.wrap(limit[0], limit[1], total, data);
			return result;
		} else {
			return data;
		}
	}

	/**
	 * 插入一条记录
	 * 
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	// @Transactional
	public Object insert(SQLBuilder builder) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(builder.db());
		JSONObject tableConfig = AirContext.getTableConfig(builder.db(), builder.table());
		// 验证并处理请求数据
		processor.process(builder);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		Object key = sqlSession.insert(builder.sql(), builder.params().toArray());
		// 方法返回值, 多个数据库生成的id组成的数组, 包括关联表id
		JSONObject result = new JSONObject(builder.values());
		// 返回插入的主键
		String primaryKey = tableConfig.containsKey(TableConfig.PRIMARY_KEY)
				? tableConfig.getString(TableConfig.PRIMARY_KEY)
				: TableConfig.PRIMARY_KEY.toString();
		result.put(primaryKey, key);
		return result;
	}

	/**
	 * 更新一条记录
	 * 
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	// @Transactional
	public Object update(SQLBuilder builder) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(builder.db());
		if (AirContext.getConfig().getBoolean(DatacolorConfig.UPSERT)) { // 如果不存在就新增记录
			// 查询数据库是否存在
			Select select = SQLBuilder.select(builder.table());
			select.where(builder.where()).params(((Update) builder).whereParams());
			select.build();
			Map<String, Object> detail = sqlSession.detail(select.sql(), select.params().toArray());
			if (detail == null) {
				Insert insert = SQLBuilder.insert(builder.table(), builder.values());
				insert.build();
				return insert(insert);
			}
		}
		// 验证并处理数据
		processor.process(builder);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		int updateCount = sqlSession.update(builder.sql(), builder.params().toArray());
		return new JSONObject("count", updateCount);
	}

	/**
	 * 删除一条记录
	 * 
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	// @Transactional
	public Object delete(SQLBuilder builder) throws SQLException {
		SQLSession sqlSession = AirContext.getSqlSession(builder.db());
		int deleteCount = sqlSession.delete(builder.sql(), builder.params().toArray());
		return new JSONObject("count", deleteCount);
	}

	/**
	 * 事务操作
	 * 
	 * @param actions
	 * @return
	 * @throws SQLException
	 */
	public Object transaction(String db, List<Engine> engines) throws SQLException {
		JSONArray result = new JSONArray();
		// 跨数据库事务暂不支持
		SQLSession sqlSession = AirContext.getSqlSession(db);
		sqlSession.trans(new Atom() {

			@Override
			public void run() {
				for (Engine engine : engines) {
					try {
						result.add(handle(engine));
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}

			}
		});

		return result;
	}

}
