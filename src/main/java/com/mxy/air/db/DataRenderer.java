package com.mxy.air.db;

import java.util.List;
import java.util.Map;

import com.mxy.air.json.JSONObject;
import com.mxy.air.db.config.TableConfig.Column;

/**
 * 返回结果渲染器
 * 
 * @author mengxiangyun
 *
 */
public class DataRenderer {

	/**
	 * 渲染集合对象
	 * 
	 * @param datas
	 *            数据集
	 * @param config
	 *            所有字段配置
	 */
	public void render(List<Map<String, Object>> datas, JSONObject columnsConfig) {
		if (columnsConfig == null)
			return;
		datas.forEach(data -> render(data, columnsConfig));
	}

	/**
	 * 渲染单个结果
	 * 
	 * @param data
	 *            数据
	 * @param config
	 *            所有字段配置
	 */
	public void render(Map<String, Object> data, JSONObject columnsConfig) {
		if (columnsConfig == null)
			return;
		data.replaceAll((k, v) -> render(v, columnsConfig.getObject(k)));
	}

	/**
	 * 渲染字段
	 * 
	 * @param value
	 * @param columnConfig
	 * @return
	 */
	private Object render(Object value, JSONObject columnConfig) {
		if (columnConfig == null) {
			return value;
		}
		if (columnConfig.containsKey(Column.DISPLAY)) {
			Object displayConfigObject = columnConfig.get(Column.DISPLAY);
			if (displayConfigObject instanceof JSONObject) {
				JSONObject displayConfig = (JSONObject) displayConfigObject;
				value = displayConfig.get(value.toString());
			}
		}
		return value;
	}

}
