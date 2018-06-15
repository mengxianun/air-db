package com.mxy.air.db.jdbc.dialect;

import com.mxy.air.db.jdbc.Dialect;
import com.mxy.air.db.jdbc.Page;

public class ElasticsearchDialect implements Dialect {

	public String processLimit(String sql) {
		return sql + " limit ?,?";
	}

	public Object[] processLimitParams(Page page) {
		return new Object[] { page.getStart() == 0 ? 0 : page.getStart() - 1, page.getPageSize() };
	}

}
