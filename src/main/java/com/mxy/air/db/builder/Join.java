package com.mxy.air.db.builder;

import com.mxy.air.db.Structure.JoinType;

public class Join {

	private String table;
	private String column;
	private String targetTable;
	private String targetColumn;
	private JoinType joinType;

	public Join() {
	}

	public Join(String table, String column, String targetTable, String targetColumn, JoinType joinType) {
		this.table = table;
		this.column = column;
		this.targetTable = targetTable;
		this.targetColumn = targetColumn;
		this.joinType = joinType;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
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

}
