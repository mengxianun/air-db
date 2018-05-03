package com.mxy.air.db.builder;

import java.util.List;

import com.mxy.air.db.SQLBuilder;

public class Delete extends SQLBuilder {

	public Delete() {
	}

	public Delete(String table, String where, List<Object> params) {
		this.table = table;
		this.where = where;
		this.params.addAll(params);
		this.whereParams.addAll(params);
		build();
	}
    
	public Delete build() {
		StringBuilder builder = new StringBuilder();
		builder.append("delete from ").append(table);
		if (!isEmpty(where)) {
			builder.append(" where ").append(where);
		}
		sql = builder.toString();
		return this;
    }

}