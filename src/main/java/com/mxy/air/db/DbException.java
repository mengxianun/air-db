package com.mxy.air.db;

/**
 * @author mengxiangyun
 */
public class DbException extends RuntimeException {

	private static final long serialVersionUID = 5076209367292548427L;

	public DbException() {
		super();
	}

	public DbException(String message, Throwable cause) {
		super(message, cause);
	}

	public DbException(String message) {
		super(message);
	}

	public DbException(Throwable cause) {
		super(cause);
	}
}
