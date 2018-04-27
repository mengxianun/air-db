package com.mxy.air.db.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Datacolor 配置 (datacolor.json)
 * 
 * @author mengxiangyun
 *
 */
public enum DatacolorConfig {
	
	/*
	 * 数据源
	 */
	DATASOURCE(null),
	
	/*
	 * 事务, 默认开启
	 */
	TRANSACTION(true),

	/*
	 * 是否启用原生SQL, 默认false
	 */
	NATIVE(false),

	/*
	 * 是否启用日志
	 */
	LOG(false),

	/*
	 * 表信息配置文件路径
	 */
	TABLE_CONFIG_PATH("tables");
	
	/*
	 * 属性默认值
	 */
	private Object value;

	private DatacolorConfig(Object value) {
		this.value = value;
	}
	
	public Object value() {
		return this.value;
	}
	
	public static Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		for (DatacolorConfig datacolor : values()) {
			map.put(datacolor.toString().toLowerCase(), datacolor.value);
		}
		return map;
	}
	
	/**
	 * 数据源
	 *
	 */
	public enum Datasource {
		
		/*
		 * 数据源类型
		 */
		TYPE
		
	}

}
