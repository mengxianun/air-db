package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.mxy.air.db.jdbc.ResultSetHandler;

/**
 * 将ResultSet转换为单个结果
 * 
 * @author mengxiangyun
 *
 */
public class ObjectHandler implements ResultSetHandler<Object> {

	public Object handle(ResultSet rs) throws SQLException {
		if (rs.next()) {
			return rs.getObject(1);
		}
		return null;
	}

}
