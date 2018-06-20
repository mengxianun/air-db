package com.mxy.air.db.builder;

import java.util.List;

import com.mxy.air.db.AirContext;
import com.mxy.air.db.SQLBuilder;

public class Delete extends SQLBuilder {

	public Delete() {
		statementType = StatementType.DELETE;
	}

	public Delete(String table, String alias, List<Condition> conditions) {
		this();
		this.table = table;
		this.alias = alias;
		this.conditions = conditions;
	}
    
	public Delete toBuild() {
		if (db == null)
			db = AirContext.getDefaultDb();
		dialect = AirContext.getDialect(db);
		StringBuilder builder = new StringBuilder();
		builder.append("delete ").append(alias).append(" from ").append(table).append(" ").append(alias);
		// Where
		builder.append(buildWhere());
		sql = builder.toString();
		return this;
    }

}
