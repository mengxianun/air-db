package com.mxy.air.db.builder;

import com.mxy.air.db.SQLBuilder;

public class Native extends SQLBuilder {

	public Native() {
	}

	public Native(String sql) {
		this.sql = sql;
	}

	public Native build() {
		// nothing to do
		return this;
	}

}
