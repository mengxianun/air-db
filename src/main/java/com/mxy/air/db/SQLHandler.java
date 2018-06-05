package com.mxy.air.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.builder.Select;
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
	private SQLSession sqlSession;

	@Inject
	private DataProcessor processor;

	@Inject
	private DataRenderer renderer;

	@Inject
	@Named("tableConfigs")
	private JSONObject tableConfigs;

	public Object handle(RequestAction action) throws SQLException {
		Type type = action.getType();
		SQLBuilder builder = action.getBuilder();
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
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		Map<String, Object> detail = sqlSession.detail(builder.sql(), builder.params().toArray());
		// 结果渲染
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
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		List<Map<String, Object>> list = sqlSession.list(builder.sql(), builder.params().toArray());
		// 结果渲染
		renderer.render(list, columnsConfig);
		// 分页查询, 查询总记录数
		if (builder.limit() != null) {
			String countSql = ((Select) builder).getCountSql();
			Object[] countParams = ((Select) builder).whereParams().toArray();
			long total = sqlSession.count(countSql, countParams);
			long[] limit = builder.limit();
			JSONObject result = PageResult.wrap(limit[0], limit[1], total, list);
			return result;
		} else {
			return new JSONArray(list);
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
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		// 验证并处理请求数据
		processor.process(builder, columnsConfig);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		Object key = sqlSession.insert(builder.sql(), builder.params().toArray());
		// 方法返回值, 多个数据库生成的id组成的数组, 包括关联表id
		JSONObject result = new JSONObject(builder.values());
		if (key != null) { // SQL操作返回主键为空, 则返回请求数据中的主键值
			String keyColumn = tableConfig != null ? tableConfig.getString(TableConfig.PRIMARY_KEY)
					: TableConfig.PRIMARY_KEY.toString();
			result.put(keyColumn, key);
		}
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
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		// 验证并处理数据
		processor.process(builder, columnsConfig);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		int updateCount = sqlSession.update(builder.sql(), builder.params().toArray());
		JSONArray result = new JSONArray();
		result.add(updateCount);
		return result;
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
		int deleteCount = sqlSession.delete(builder.sql(), builder.params().toArray());
		return new JSONArray().add(deleteCount);
	}

	/**
	 * 事务操作
	 * 
	 * @param actions
	 * @return
	 * @throws SQLException
	 */
	public Object transaction(List<RequestAction> actions) throws SQLException {
		JSONArray result = new JSONArray();

		sqlSession.trans(new Atom() {

			@Override
			public void run() {
				for (RequestAction action : actions) {
					try {
						result.add(handle(action));
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}

			}
		});

		return result;
	}

}
