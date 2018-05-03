package com.mxy.air.db;

import static com.mxy.air.db.PageResult.DATAS;
import static com.mxy.air.db.PageResult.END;
import static com.mxy.air.db.PageResult.START;
import static com.mxy.air.db.PageResult.TOTAL;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mxy.air.db.DbException;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;
import com.mxy.air.db.Structure.Type;
import com.mxy.air.db.annotation.Transactional;
import com.mxy.air.db.builder.Select;
import com.mxy.air.db.config.TableConfig;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * SQL逻辑处理器
 * 
 * @author mengxiangyun
 *
 */
public class SQLHandler {

	@Inject
	private SQLRunner runner;

	@Inject
	private DataProcessor processor;

	@Inject
	private DataRenderer renderer;

	@Inject
	@Named("config")
	private JSONObject config;

	@Inject
	@Named("tableConfigs")
	private JSONObject tableConfigs;

	public String handle(Type type, SQLBuilder builder) throws SQLException {
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

	@Transactional
	public String detail(SQLBuilder builder) throws SQLException {
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		Map<String, Object> detail = runner.detail(builder.sql(), builder.params().toArray());
		// 结果渲染
		renderer.render(detail, columnsConfig);
		return new JSONObject(detail).toString();
	}

	@Transactional
	public String query(SQLBuilder builder) throws SQLException {
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		List<Map<String, Object>> list = runner.list(builder.sql(), builder.params().toArray());
		// 结果渲染
		renderer.render(list, columnsConfig);
		// 分页查询, 查询总记录数
		if (builder.limit() != null) {
			String countSql = ((Select) builder).getCountSql();
			Object[] countParams = ((Select) builder).whereParams().toArray();
			long total = runner.count(countSql, countParams);
			long[] limit = builder.limit();
			JSONObject result = new JSONObject().put(START, limit[0]).put(END, limit[1]).put(TOTAL, total).put(DATAS,
					list);
			return result.toString();
		} else {
			return new JSONArray(list).toString();
		}
	}

	@Transactional
	public String insert(SQLBuilder builder) throws SQLException {
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		// 验证并处理请求数据
		processor.process(builder, columnsConfig);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		Object key = runner.insert(builder.sql(), builder.params().toArray());
		// 方法返回值, 多个数据库生成的id组成的数组, 包括关联表id
		JSONObject result = new JSONObject(builder.values());
		if (key != null) { // SQL操作返回主键为空, 则返回请求数据中的主键值
			String keyColumn = tableConfig != null ? tableConfig.getString(TableConfig.PRIMARY_KEY)
					: TableConfig.PRIMARY_KEY.toString();
			result.put(keyColumn, key);
		}
		return result.toString();
	}

	@Transactional
	public String update(SQLBuilder builder) throws SQLException {
		JSONObject tableConfig = tableConfigs.getObject(builder.table());
		JSONObject columnsConfig = tableConfig != null ? tableConfig.getObject(TableConfig.COLUMNS) : null;
		// 验证并处理数据
		processor.process(builder, columnsConfig);
		// 重新构建SQLBuilder, 生成新的SQL语句和参数
		builder.build();
		int updateCount = runner.update(builder.sql(), builder.params().toArray());
		JSONArray result = new JSONArray();
		result.add(updateCount);
		return result.toString();
	}

	@Transactional
	public String delete(SQLBuilder builder) throws SQLException {
		int deleteCount = runner.delete(builder.sql(), builder.params().toArray());
		return new JSONArray().add(deleteCount).toString();
	}

}
