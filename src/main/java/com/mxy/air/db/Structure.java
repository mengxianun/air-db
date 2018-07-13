package com.mxy.air.db;

/**
 * Json结构
 * @author mengxiangyun
 *
 */
public enum Structure {
	
	/*
	 * 操作类型
	 */
	TYPE,
	/*
	 * 关联查询
	 */
	JOIN,
	/*
	 * 字段
	 */
	FIELDS,
	/*
	 * 插入或更新的值
	 */
	VALUES,
	/*
	 * 过滤条件
	 */
	WHERE,
	/*
	 * 分组
	 */
	GROUP,
	/*
	 * 排序
	 */
	ORDER,
	/*
	 * 分页
	 */
	LIMIT,
	/*
	 * 原生SQL模式
	 */
	NATIVE,
	/*
	 * 数据源
	 */
	SOURCE,
	/*
	 * 返回结果
	 */
	RESULT,
	
	/*
	 * 模板
	 */
	TEMPLATE;
	
	/**
	 * 操作类型
	 */
	public enum Type {
		/*
		 * 详细, 单个结果
		 */
		DETAIL,
		/*
		 * 查询, 多个结果
		 */
		QUERY,
		/*
		 * 查询, 多个结果, 同QUERY
		 */
		SELECT,
		/*
		 * 插入
		 */
		INSERT,
		/*
		 * 更新
		 */
		UPDATE,
		/*
		 * 删除
		 */
		DELETE,

		/*
		 * 事务
		 */
		TRANSACTION,

		/*
		 * 数据库表配置结构
		 */
		STRUCT;
		
		public static Type from(String name) {
			for (Type type : values()) {
				// 不区分大小写
				if (type.toString().equalsIgnoreCase(name)) {
					return type;
				}
			}
			return null;
		}
	}
	
	/**
	 * 模板
	 */
	public enum Template {

		CSV;

		public static Template from(String name) {
			for (Template template : values()) {
				// 不区分大小写
				if (template.toString().equalsIgnoreCase(name)) {
					return template;
				}
			}
			return null;
		}
	}

	/**
	 * 关联查询
	 */
	public enum JoinType {

		/**
		 * inner join
		 */
		INNER("inner join"),

		/*
		 * left outer join
		 */
		LEFT("left outer join"),

		/*
		 * right outer join
		 */
		RIGHT("right outer join"),

		/*
		 * full outer join
		 */
		FULL("full outer join");

		private String text;

		private JoinType(String text) {
			this.text = text;
		}

		public String text() {
			return text;
		}

		public static JoinType from(String type) {
			for (JoinType joinType : values()) {
				if (joinType.toString().equalsIgnoreCase(type)) {
					return joinType;
				}
			}
			return null;
		}
	}

	/*
	 * 过滤条件
	 */
	public enum Where {
		
	}

	/*
	 * 运算符
	 */
	public enum Operator {

		/*
		 * 运算符
		 */
		EQUAL("=", "="),
		NOT_EQUAL("!=", "<>"),
		GT(">", ">"),
		GTE(">=", ">="),
		LT("<", "<"),
		LTE("<=", "<="),
		IN(",", "in"),
		/*
		 * op格式暂定未定
		 */
		NOT_IN("", "not in"),
		BETWEEN("~", "between"),
		LIKE("%=", "like"),
		NOT_LIKE("!%=", "not like"),
		
		NOT("!", "not"),
		
		/*
		 * 连接符
		 */
		AND("and", "and"),
		OR("or", "or");
		
		/*
		 * 运算符号
		 */
		private String op;
		
		/*
		 * 运算符对应的SQL写法
		 */
		private String sql;
		
		private Operator(String op, String sql) {
			this.op = op;
			this.sql = sql;
		}
		
		public String op() {
			return this.op;
		}
		
		public String sql() {
			return this.sql;
		}
		
		public static Operator from(String op) {
			for (Operator operator : values()) {
				if (operator.op().equalsIgnoreCase(op)) {
					return operator;
				}
			}
			return null;
		}

	}
	
	/**
	 * 排序
	 */
	public enum Order {
		/*
		 * 升序
		 */
		ASC("asc", "asc"),
		/*
		 * 降序
		 */
		DESC("desc", "desc"),
		
		/*
		 * 升序简写方式
		 */
		PLUS("+", "asc"),
		
		/*
		 * 降序简写方式
		 */
		MINUS("-", "desc");
		
		/*
		 * 名称
		 */
		private String mark;
		/*
		 * 简写方式
		 */
		private String sqlStyle;

		private Order(String mark, String sqlStyle) {
			this.mark = mark;
			this.sqlStyle = sqlStyle;
		}

		public String mark() {
			return this.mark;
		}

		public String sql() {
			return this.sqlStyle;
		}
		
	}

	/*
	 * 返回结果
	 */
	public enum Result {
		CSV
	}

}
