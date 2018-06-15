package com.mxy.air.db;

import com.mxy.air.db.jdbc.RowProcessor;

public class AirState {

	private String db;

	private RowProcessor processor;

	public AirState(String db, RowProcessor processor) {
		this.db = db;
		this.processor = processor;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public RowProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(RowProcessor processor) {
		this.processor = processor;
	}

}
