package com.mxy.air.db.builder;

import java.util.List;

import com.mxy.air.db.AirContext;
import com.mxy.air.db.SQLBuilder;

public class Delete extends SQLBuilder {

	public Delete() {
		statementType = StatementType.DELETE;
	}

	public Delete(String table, String alias, String where, List<Object> params) {
		this();
		this.table = table;
		this.alias = alias;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
	}
    
	public Delete build() {
		if (db == null)
			db = AirContext.getDefaultDb();
		dialect = AirContext.getDialect(db);
		StringBuilder builder = new StringBuilder();
		builder.append("delete from ").append(table).append(" ").append(alias);
		if (!isEmpty(where)) {
			builder.append(" where ").append(where);
		}
		sql = builder.toString();
		return this;
    }

}
