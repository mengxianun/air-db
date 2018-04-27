package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.mxy.air.db.jdbc.BasicRowProcessor;
import com.mxy.air.db.jdbc.ResultSetHandler;
import com.mxy.air.db.jdbc.RowProcessor;

/**
 * 将ResultSet转换为Map, key为列名, value为列值
 * 
 * @author mengxiangyun
 *
 */
public class MapHandler implements ResultSetHandler<Map<String, Object>> {

	private RowProcessor processor;

	public MapHandler() {
		this(new BasicRowProcessor());
	}

	public MapHandler(RowProcessor processor) {
		this.processor = processor;
	}

	public Map<String, Object> handle(ResultSet rs) throws SQLException {
		return rs.next() ? processor.toMap(rs) : null;
	}

}
