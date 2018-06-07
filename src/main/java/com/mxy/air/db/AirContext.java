package com.mxy.air.db;

import java.util.Map;

import com.google.inject.Inject;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.db.config.TableConfig.Column;
import com.mxy.air.json.JSONObject;

@Deprecated
public class AirContext {

	@Inject
	private JSONObject tables;

	public AirContext() {
	}

	enum Association {
		
		PRIMARY_TABLE, PRIMARY_COLUMN, TARGET_TABLE, TARGET_COLUMN;

	}

	/**
	 * 获取所有关联关系
	 * @return
	 */
	public JSONObject getAssociations() {
		JSONObject associations = new JSONObject();
		for (Map.Entry<String, Object> entry : tables.entrySet()) {
			associations.put(entry.getKey(), getAssociation(entry.getKey()));
		}
		return associations;
	}

	/**
	 * 获取指定表的关联关系
	 * @param table
	 * @return
	 */
	public JSONObject getAssociation(String table) {
		JSONObject tableConfig = tables.getObject(table);
		JSONObject columnConfigs = tableConfig.getObject(TableConfig.COLUMNS);
		for (Map.Entry<String, Object> columnEntry : columnConfigs.entrySet()) {
			String columnName = columnEntry.getKey();
			JSONObject columnConfig = (JSONObject) columnEntry.getValue();
			if (columnConfig.containsKey(Column.ASSOCIATION)) {
				JSONObject columnAssociation = columnConfig.getObject(Column.ASSOCIATION);
				JSONObject association = new JSONObject();
				association.put(Association.PRIMARY_TABLE, table);
				association.put(Association.PRIMARY_COLUMN, columnName);
				association.put(Association.TARGET_TABLE, columnAssociation.get(TableConfig.Association.TARGET_TABLE));
				association.put(Association.TARGET_COLUMN,
						columnAssociation.get(TableConfig.Association.TARGET_COLUMN));
				return association;
			}
		}
		return null;
	}

}
