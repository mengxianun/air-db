package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.mxy.air.db.jdbc.BasicRowProcessor;
import com.mxy.air.db.jdbc.RowProcessor;

/**
 * 将ResultSet转换为Map类型的List
 * 
 * @author mengxiangyun
 *
 */
public class MapListHandler extends AbstractListHandler<Map<String, Object>> {

	private final RowProcessor processor;

	public MapListHandler() {
		this(new BasicRowProcessor());
	}

	public MapListHandler(RowProcessor processor) {
		this.processor = processor;
	}

	protected Map<String, Object> handleRow(ResultSet rs) throws SQLException {
		return processor.toMap(rs);
	}

}
