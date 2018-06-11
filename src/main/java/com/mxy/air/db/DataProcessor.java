package com.mxy.air.db;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mxy.air.db.SQLBuilder.StatementType;
import com.mxy.air.db.builder.Insert;
import com.mxy.air.db.builder.Update;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.db.config.TableConfig.Keyword;
import com.mxy.air.json.JSONObject;

/**
 * 请求数据处理器, 依据数据库表的配置对数据做验证和处理
 * 
 * @author mengxiangyun
 *
 */
public class DataProcessor {

	@Inject
	private SQLSession sqlSession;

	@Inject
	@Named("tableConfigs")
	private JSONObject tableConfigs;

	/**
	 * 对请求数据做验证和处理
	 * 
	 * @param builder
	 * @param columnsConfig
	 *            所有字段配置
	 * @return
	 * @throws SQLException
	 */
	public void process(SQLBuilder builder)
			throws SQLException {
		JSONObject tableConfig = tableConfigs.getObject(builder.table);
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		// 原始值
		Map<String, Object> values = builder.values();
		// 经过处理的值, 初始为原始值
		Map<String, Object> processValues = new HashMap<>(values);
		for (Map.Entry<String, Object> columnConfig : columnConfigs.entrySet()) {
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
				// 查询将要更新的数据库记录, 看该必填字段是否已经有值, 已经有值的情况下, update时该字段可以不填
				if (builder.statementType == StatementType.UPDATE) {
					SQLBuilder select = SQLBuilder.select(builder.table()).where(builder.where())
							.params(builder.whereParams());
					select.setTableConfigs(tableConfigs);
					select.build();
					List<Map<String, Object>> updateRecords = sqlSession.list(select.sql(), select.params().toArray());
					for (Map<String, Object> record : updateRecords) {
						Object recordColumnValue = record.get(column);
						if (recordColumnValue == null || recordColumnValue.toString().equals("")) {
							throw new DbException("字段 [" + column + "] 必填");
						}
					}
				} else {
					throw new DbException("字段 [" + column + "] 必填");
				}
			}
			return null;
		} else {
			// 字段类型转换, 请求传递的字段类型转为数据库的字段类型
			String dataType = columnConfig.getString(Column.TYPE);
			// 将请求字段类型转换为数据库表字段类型
			convertToDbType(value, dataType);
			// 字段唯一性验证
			if (columnConfig.containsKey(Column.UNIQUE) && columnConfig.getBoolean(Column.UNIQUE)) {
				boolean exist = false;
				if (builder instanceof Insert) {
					SQLBuilder select = SQLBuilder.select(builder.table()).equal(column, value);
					select.setTableConfigs(tableConfigs);
					select.build();
					exist = sqlSession.detail(select.sql(), select.params().toArray()) != null;
				} else if (builder instanceof Update) {
					// update操作唯一性验证排除自身, 通过where和params定位自身, 再reverse排除自身, 再equal查询其他包含该属性的记录
					SQLBuilder select = SQLBuilder.select(builder.table()).where(builder.where())
							.whereParams(Lists.newArrayList(builder.whereParams())).reverse().equal(column, value);

					select.setTableConfigs(tableConfigs);
					select.build();
					exist = sqlSession.detail(select.sql(), select.params().toArray()) != null;
				}
				if (exist) {
					throw new DbException("记录 " + column + "[" + value + "] 已存在");
				}
			}
		}
		return value;
	}

	private void convertToDbType(Object value, String dataType) {
		if (dataType.equals("int")) {
			value = Integer.parseInt(value.toString());
		} else if (dataType.equals("varchar")) {
			value = value.toString();
		}
	}

}
