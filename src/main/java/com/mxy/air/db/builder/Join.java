package com.mxy.air.db.builder;

import com.mxy.air.db.Structure.JoinType;
import com.mxy.air.db.config.TableConfig.Association;

public class Join {

	// 主表名
	private String table;

	// 主表别名
	private String alias;

	// 主表关联字段
	private String column;

	// 关联表
	private String targetTable;

	// 关联表别名
	private String targetAlias;

	// 关联字段
	private String targetColumn;

	// Join类型
	private JoinType joinType;

	// 表关联类型
	private Association.Type associationType;

	public Join() {}

	public Join(String table, String alias, String column, String targetTable, String targetAlias, String targetColumn,
			JoinType joinType, Association.Type associationType) {
		this.table = table;
		this.alias = alias;
		this.column = column;
		this.targetTable = targetTable;
		this.targetAlias = targetAlias == null ? "j_" + targetTable : targetAlias;
		this.targetColumn = targetColumn;
		this.joinType = joinType;
		this.associationType = associationType;
	}


	public String getTable() {
		return table;
	}


	public void setTable(String table) {
		this.table = table;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getColumn() {
		return column;
	}


	public void setColumn(String column) {
		this.column = column;
	}


	public String getTargetTable() {
		return targetTable;
	}


	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getTargetAlias() {
		return targetAlias;
	}

	public void setTargetAlias(String targetAlias) {
		this.targetAlias = targetAlias;
	}

	public String getTargetColumn() {
		return targetColumn;
	}


	public void setTargetColumn(String targetColumn) {
		this.targetColumn = targetColumn;
	}


	public JoinType getJoinType() {
		return joinType;
	}


	public void setJoinType(JoinType joinType) {
		this.joinType = joinType;
	}

	public Association.Type getAssociationType() {
		return associationType;
	}

	public void setAssociationType(Association.Type associationType) {
		this.associationType = associationType;
	}

}
