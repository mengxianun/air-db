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
	DATASOURCES(null),

	/*
	 * 如果数据不存在, 就新增一条
	 */
	UPSERT(true),

	/*
	 * 是否启用原生SQL, 默认false
	 */
	NATIVE(false),

	/*
	 * 是否启用日志
	 */
	LOG(false),

	/*
	 * 主数据源
	 */
	DEFAULT_DATASOURCE(null),

	/*
	 * 表信息配置文件路径
	 */
	DB_TABLE_CONFIG_PATH("tables"),

	/*
	 * 所有数据库表配置, 该属性项非配置文件配置, 是项目自动生成的属性, 目的是将数据库表的配置信息与项目全局的配置信息放在一个对象里
	 * 全局配置
	 * {
	 *   "datasources":{
	 *     "db1":{},
	 *     "db2":{}
	 *   },
	 *   "log":true,
	 *   "db_table_config":{
	 *     "db1":{
	 *       "table1":{},
	 *       "table2":{} 
	 *     }
	 *     "db2":{
	 *       "table1":{},
	 *       "table2":{} 
	 *     }
	 *   }
	 * }
	 */
	DB_TABLE_CONFIG(null);
	
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
		TYPE,

		/*
		 * 方言
		 */
		DIALECT,

		/**
		 * 数据源
		 */
		SOURCE;
		
	}

}
