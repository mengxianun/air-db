package com.mxy.air.db;

import static com.mxy.air.db.Structure.NATIVE;
import static com.mxy.air.db.Structure.Type.DELETE;
import static com.mxy.air.db.Structure.Type.DETAIL;
import static com.mxy.air.db.Structure.Type.EXPORT_CSV_TPL;
import static com.mxy.air.db.Structure.Type.INSERT;
import static com.mxy.air.db.Structure.Type.QUERY;
import static com.mxy.air.db.Structure.Type.SELECT;
import static com.mxy.air.db.Structure.Type.STRUCT;
import static com.mxy.air.db.Structure.Type.TRANSACTION;
import static com.mxy.air.db.Structure.Type.UPDATE;

import java.util.ArrayList;
import java.util.List;

import com.mxy.air.db.Structure.Type;
import com.mxy.air.json.JSONObject;

/**
 * 请求JSON解析器
 * @author mengxiangyun
 *
 */
public class AirParser {
	
	private JSONObject object;

	private Type type;

	private String db;

	private String table;

	private String alias;

	public AirParser() {}

	public AirParser(String json) {
		parse(json);
	}

	public void parse(String json) {
		object = new JSONObject(json);
		if (object.containsKey(NATIVE)) {
			return;
		}
		// 操作类型
		parseType();
		table = object.getString(type).trim();
		if (table.indexOf(".") != -1) { // 指定了数据源
			String[] dbTableString = table.split("\\.");
			db = dbTableString[0];
			table = dbTableString[1];
			if (table.indexOf(" ") != -1) {
				String[] tableAlias = table.split(" ");
				table = tableAlias[0];
				alias = tableAlias[1];
			}
		} else if (table.indexOf(" ") != -1) {
			String[] tableAlias = table.split(" ");
			table = tableAlias[0];
			alias = tableAlias[1];
		}
		if (db == null) {
			db = AirContext.getDefaultDb();
		}
	}
	
	/**
	 * 获取操作类型
	 * 
	 * @param object
	 * @return
	 */
	public void parseType() {
		List<Type> types = new ArrayList<>();
		if (object.containsKey(DETAIL)) {
			types.add(DETAIL);
		}
		if (object.containsKey(QUERY)) {
			types.add(QUERY);
		}
		if (object.containsKey(SELECT)) {
			types.add(SELECT);
		}
		if (object.containsKey(INSERT)) {
			types.add(INSERT);
		}
		if (object.containsKey(UPDATE)) {
			types.add(UPDATE);
		}
		if (object.containsKey(DELETE)) {
			types.add(DELETE);
		}
		if (object.containsKey(TRANSACTION)) {
			types.add(TRANSACTION);
		}
		if (object.containsKey(STRUCT)) {
			types.add(STRUCT);
		}
		if (object.containsKey(EXPORT_CSV_TPL)) {
			types.add(EXPORT_CSV_TPL);
		}
		if (types.size() != 1) { // 操作类型只能是一个
			throw new DbException("未指定操作类型或指定了多个操作类型");
		}
		type = types.get(0);
	}

	public JSONObject getObject() {
		return object;
	}

	public String getTable() {
		return table;
	}

	public Type getType() {
		return type;
	}

	public String getDb() {
		return db;
	}

	public String getAlias() {
		return alias;
	}

}
