package com.mxy.air.db.jdbc.dialect;

import com.mxy.air.db.jdbc.Dialect;
import com.mxy.air.db.jdbc.Page;

public class OracleDialect implements Dialect {

	public String processLimit(String sql) {
		// oracle rownum 从1开始
		return "select * from (select a.*, rownum rn from (" + sql + ") a where rownum <= ?) where rn >= ?";
	}

	public Object[] processLimitParams(Page page) {
		return new Object[] {page.getEnd(), page.getStart() + 1};
	}

}
