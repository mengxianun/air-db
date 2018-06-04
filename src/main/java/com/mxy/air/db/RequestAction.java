package com.mxy.air.db;

import com.mxy.air.db.Structure.Type;

/**
 * 请求动作封装
 * 
 * @author mengxiangyun
 *
 */
public class RequestAction {

	/*
	 * 操作类型
	 */
	private Type type;

	/*
	 * SQL构造器
	 */
	private SQLBuilder builder;

	public RequestAction(Type type, SQLBuilder builder) {
		this.type = type;
		this.builder = builder;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public SQLBuilder getBuilder() {
		return builder;
	}

	public void setBuilder(SQLBuilder builder) {
		this.builder = builder;
	}

}
