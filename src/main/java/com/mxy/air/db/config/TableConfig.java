package com.mxy.air.db.config;

/**
 * 数据库表配置 (tableName.json)
 * 
 * @author mengxiangyun
 *
 */
public enum TableConfig {

	/**
	 * 数据库表名
	 */
	TABLE,

	/*
	 * 主键
	 */
	PRIMARY_KEY,

	/*
	 * 列
	 */
	COLUMNS;

	public enum Column {

		/*
		 * 显示
		 */
		DISPLAY,

		/*
		 * 唯一
		 */
		UNIQUE,

		/*
		 * 必需
		 */
		REQUIRED,

		/*
		 * 默认值
		 */
		DEFAULT,
		
		/*
		 * 关联表
		 */
		ASSOCIATION
	}

	public enum Association {

		/*
		 * 关联表
		 */
		TARGET_TABLE,

		/*
		 * 关联表的列
		 */
		TARGET_COLUMN
	}

	public enum Keyword {

		/*
		 * 当前日期时间
		 */
		NOW,

		/*
		 * 当前日期
		 */
		DATE,

		/*
		 * 当前时间
		 */
		TIME
	}

}
