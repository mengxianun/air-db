package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.mxy.air.db.jdbc.BasicRowProcessor;
import com.mxy.air.db.jdbc.ResultSetHandler;
import com.mxy.air.db.jdbc.RowProcessor;

/**
 * 将ResultSet转换为Object[]
 * 
 * @author mengxiangyun
 *
 */
public class ArrayHandler implements ResultSetHandler<Object[]> {

	private RowProcessor processor;

	public ArrayHandler() {
		this(new BasicRowProcessor());
	}

	public ArrayHandler(RowProcessor processor) {
		this.processor = processor;
	}

	public Object[] handle(ResultSet rs) throws SQLException {
		return rs.next() ? processor.toArray(rs) : new Object[0];
	}

}
