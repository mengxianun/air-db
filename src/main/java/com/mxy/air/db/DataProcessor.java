package com.mxy.air.db;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import com.mxy.air.db.DbException;
import com.mxy.air.json.JSONObject;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.config.TableConfig.Keyword;
import com.google.inject.Inject;

/**
 * 请求数据处理器, 依据数据库表的配置对数据做验证和处理
 * 
 * @author mengxiangyun
 *
 */
public class DataProcessor {

	@Inject
	private SQLRunner runner;

	/**
	 * 对请求数据做验证和处理
	 * 
	 * @param builder
	 * @param columnsConfig
	 *            所有字段配置
	 * @return
	 * @throws SQLException
	 */
	public void process(SQLBuilder builder, JSONObject columnsConfig)
			throws SQLException {
		if (columnsConfig == null) {
			return;
		}
		// 原始值
		Map<String, Object> values = builder.values();
		// 经过处理的值, 初始为原始值
		Map<String, Object> processValues = new HashMap<>(values);
		for (Map.Entry<String, Object> columnConfig : columnsConfig.entrySet()) {
			String column = columnConfig.getKey();
			JSONObject config = (JSONObject) columnConfig.getValue();
			Object value = values.get(column);
			value = process(column, value, config, builder);
			if (value != null) {
				processValues.put(column, value);
			}
		}
		// 更新SQLBuilder的值
		builder.values(processValues);

	}

	/**
	 * 对单个字段做验证和处理
	 * 
	 * @param column
	 *            字段列名称
	 * @param value
	 *            字段值
	 * @param columnConfig
	 *            字段配置
	 * @param builder
	 * @return
	 * @throws SQLException
	 */
	public Object process(String column, Object value, JSONObject columnConfig, SQLBuilder builder)
			throws SQLException {
		if (value == null) { // 没有指定列的值
			if (columnConfig.containsKey(Column.DEFAULT)) { // 是否有默认值
				Object defaultValue = columnConfig.get(Column.DEFAULT);
				if (Keyword.NOW.toString().equalsIgnoreCase(defaultValue.toString())) {
					return LocalDateTime.now();
				} else if (Keyword.DATE.toString().equalsIgnoreCase(defaultValue.toString())) {
					return LocalDate.now();
				} else if (Keyword.TIME.toString().equalsIgnoreCase(defaultValue.toString())) {
					return LocalTime.now();
				}
				return defaultValue;
			} else if (columnConfig.containsKey(Column.REQUIRED) && columnConfig.getBoolean(Column.REQUIRED)) { // 必填
				throw new DbException("字段 [" + column + "] 必填");
			}
			return null;
		}
		if (columnConfig.containsKey(Column.UNIQUE) && columnConfig.getBoolean(Column.UNIQUE)) { // 字段唯一性验证
			boolean exist = false;
			if (builder instanceof Insert) {
				SQLBuilder select = SQLBuilder.select(builder.table()).equal(column, value).build();
				exist = runner.detail(select.sql(), select.params().toArray()) != null;
			} else if (builder instanceof Update) {
				// update操作唯一性验证排除自身, 通过where和params定位自身, 再reverse排除自身, 再equal查询其他包含该属性的记录
				SQLBuilder select = SQLBuilder.select(builder.table()).where(builder.where())
						.whereParams(builder.whereParams()).reverse().equal(column, value).build();
				exist = runner.detail(select.sql(), select.params().toArray()) != null;
			}
			if (exist) {
				throw new DbException("记录 " + column + "[" + value + "] 已存在");
			}
		}
		return value;
	}

}
