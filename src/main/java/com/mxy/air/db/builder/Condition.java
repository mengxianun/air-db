package com.mxy.air.db.builder;

import java.util.Collection;

import com.mxy.air.db.Structure.Operator;

/**
 * SQL条件
 * @author mengxiangyun
 *
 */
public class Condition {

	private Operator operator;

	private String column;

	private Object value;

	public Condition(Operator operator, String column, Object value) {
		this.operator = operator;
		this.column = column;
		this.value = value;
	}

	public String sql() {
		if (value instanceof Collection) {
			Collection<?> collection = (Collection<?>) value;
			for (Object o : collection) {
				if (o instanceof String) {
					quote(o);
				}
			}
		} else if (value.getClass().isArray()) {
			//
		} else {
			if (value instanceof String) {
				quote(value);
			}
		}

		String condition = "";
		switch (operator) {
		case EQUAL:
			condition = column + " = " + value;
			break;
		case NOT_EQUAL:
			condition = column + " != " + value;
			break;
		case GT:
			condition = column + " > " + value;
			break;
		case GTE:
			condition = column + " >= " + value;
			break;
		case LT:
			condition = column + " < " + value;
			break;
		case LTE:
			condition = column + " <= " + value;
			break;
		case IN:
			condition = column + " in (" + value + ")";
			break;
		case BETWEEN:

			break;
		case LIKE:

			break;
		case NOT:

			break;

		default:
			break;
		}
		return condition;
	}

	private void quote(Object string) {
		string = "'" + string + "'";
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
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

}
