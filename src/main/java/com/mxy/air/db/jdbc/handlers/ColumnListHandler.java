package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 单列ResultSet转换为Object类型的List
 * 
 * @author mengxiangyun
 *
 * @param <T>
 */
public class ColumnListHandler<T> extends AbstractListHandler<T> {

	/**
	 * 列号
	 */
	private final int columnIndex;

	/**
	 * 列名
	 */
	private final String columnName;

	/**
	 * 默认第一列
	 */
	public ColumnListHandler() {
		this(1, null);
	}

	/**
	 * 指定列号
	 * 
	 * @param columnIndex
	 */
	public ColumnListHandler(int columnIndex) {
		this(columnIndex, null);
	}

	/**
	 * 指定列名
	 * 
	 * @param columnName
	 */
	public ColumnListHandler(String columnName) {
		this(1, columnName);
	}

	/**
	 * 指定列号和列名
	 * 
	 * @param columnIndex
	 * @param columnName
	 */
	private ColumnListHandler(int columnIndex, String columnName) {
		this.columnIndex = columnIndex;
		this.columnName = columnName;
	}

	/**
	 * 返回指定列的值, Object类型. 优先根据列名获取, 列名不存在按列号获取
	 */
	@SuppressWarnings("unchecked")
	protected T handleRow(ResultSet rs) throws SQLException {
		if (this.columnName == null) {
			return (T) rs.getObject(this.columnIndex);
		}
		return (T) rs.getObject(this.columnName);
	}

}
