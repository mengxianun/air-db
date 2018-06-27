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
	COLUMNS,

	/*
	 * 数据库表名含义(中文), 与COMMENT不同, COMMENT依赖数据库, 而DISPLAY可以应用自定义, 方便cvs导出时的文件名等功能
	 */
	DISPLAY,

	/*
	 * 注释, 数据库表的注释
	 */
	COMMENT;

	public enum Column {

		/*
		 * 数据类型
		 */
		TYPE,

		/*
		 * 列显示(中文), 方便cvs导出时的表头等功能
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
		ASSOCIATION,

		/*
		 * 字典编码, 如: 1男 2女
		 */
		CODE,

		/*
		 * 列格式
		 */
		FORMAT,

		/*
		 * 注释
		 */
		COMMENT;
	}

	public enum Association {

		/*
		 * 关联表
		 */
		TARGET_TABLE,

		/*
		 * 关联表的列
		 */
		TARGET_COLUMN,

		/*
		 * 关联类型, 一对一, 一对多, 多对一, 多对多
		 */
		TYPE;

		public enum Type {
			ONE_TO_ONE("one_to_one"), ONE_TO_MANY("one_to_many"), MANY_TO_ONE("many_to_one"), MANY_TO_MANY(
					"many_to_many");

			private String text;

			private Type(String text) {
				this.text = text;
			}

			public String text() {
				return text;
			}

			public static Type from(String text) {
				for (Type type : values()) {
					if (type.text().equalsIgnoreCase(text)) {
						return type;
					}
				}
				return null;
			}

		}
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

	public enum Format {
		DATETIME
	}

}
