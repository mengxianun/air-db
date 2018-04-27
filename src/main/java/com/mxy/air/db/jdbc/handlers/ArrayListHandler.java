package com.mxy.air.db.jdbc.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.mxy.air.db.jdbc.BasicRowProcessor;
import com.mxy.air.db.jdbc.RowProcessor;

/**
 * 将ResultSet转换为Object[]类型的List
 * 
 * @author mengxiangyun
 *
 */
public class ArrayListHandler extends AbstractListHandler<Object[]> {

    private final RowProcessor processor;

    public ArrayListHandler() {
        this(new BasicRowProcessor());
    }

    public ArrayListHandler(RowProcessor processor) {
        this.processor = processor;
    }

    protected Object[] handleRow(ResultSet rs) throws SQLException {
        return this.processor.toArray(rs);
    }

}
