package com.mxy.air.db;

import com.mxy.air.db.Structure.Operator;

public class FilterItem {

	private String table;

	private String column;

	private Object value;

	private Operator connector;

	private Operator operator;

	public FilterItem() {
	}

	public FilterItem(String table, String column, Object value) {
		this.table = table;
		this.column = column;
		this.value = value;
		this.connector = Operator.OR;
		this.operator = Operator.EQUAL;
	}

	public FilterItem(String table, String column, Object value, Operator connector, Operator operator) {
		this.table = table;
		this.column = column;
		this.value = value;
		this.connector = connector;
		this.operator = operator;
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

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Operator getConnector() {
		return connector;
	}

	public void setConnector(Operator connector) {
		this.connector = connector;
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

}
