package com.mxy.air.db;

import java.util.List;
import java.util.Map;

import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * 返回结果渲染器
 * 
 * @author mengxiangyun
 *
 */
public class DataRenderer {

	/**
	 * 渲染集合对象.
	 * join的情况下, 如果是一对一或多对一, 就嵌入join表对象数据作为属性添加到主表结果中. 如果是一对多或多对多, 就嵌入join表的数组数据作为属性添加到主表结果中
	 * {
	 *   "id":1,
	 *   "name":"li",
	 *   "join_table":{
	 *     "join_column":join_value
	 *     ...
	 *   }
	 *   "join_table":[
	 *     {
	 *       "join_column":join_value
	 *       ...
	 *     },
	 *     {
	 *       "join_column":join_value
	 *       ...
	 *     }
	 *   ]
	 * }
	 * 
	 * @param datas
	 *            数据集
	 * @param config
	 *            所有字段配置
	 */
	public JSONArray render(List<Map<String, Object>> data, SQLBuilder builder) {
		/*
		 * 单表操作时只做简单渲染, 提升性能
		 */
		if (builder.joins() == null || builder.joins().isEmpty()) {
			JSONObject columnsConfig = AirContext.getColumnsConfig(builder.db(), builder.table());
			data.forEach(d -> render(d, columnsConfig));
			return new JSONArray(data);
		}
		JSONArray result = new JSONArray();
		/*
		 * 存储每个主表记录下的join表数据, key为主表每条记录的唯一标识, value为该条记录的join表数据
		 * {
		 *   "table_unique":{
		 *     "table1":[
		 *       {
		 *         "column1":column1_value,
		 *         ...
		 *       }
		 *       ...
		 *     ]
		 *     ...
		 *   }
		 *   ...
		 * }
		 */
		JSONObject uniqueRecordTableData = new JSONObject();

		JSONObject columnsConfig = AirContext.getColumnsConfig(builder.db(), builder.table());

		for (int i = 0; i < data.size(); i++) {

			Map<String, Object> record = data.get(i);

			/*
			 * 获取主表记录唯一编号
			 */
			StringBuilder uniqueCode = new StringBuilder();
			for (Map.Entry<String, Object> entry : record.entrySet()) {
				String column = entry.getKey();
				if (column.indexOf(".") == -1) { // 主表的字段
					// 拼接唯一编号
					uniqueCode.append(entry.getValue());
				}
			}
			/*
			 * 主表唯一记录
			 */
			JSONObject uniqueRecord;
			/*
			 * 当前操作表的索引位置
			 */
			JSONObject currentTableIndexObject;
			// 是否已经出现过
			boolean appeared = false;
			if (uniqueRecordTableData.containsKey(uniqueCode.toString())) {
				uniqueRecord = uniqueRecordTableData.getObject(uniqueCode.toString()).getObject("record");
				currentTableIndexObject = uniqueRecordTableData.getObject(uniqueCode.toString())
						.getObject("tableIndex");
				appeared = true;
			} else {
				uniqueRecord = new JSONObject();
				currentTableIndexObject = new JSONObject();
				JSONObject uniqueRecordIndex = new JSONObject();
				uniqueRecordIndex.put("record", uniqueRecord);
				uniqueRecordIndex.put("tableIndex", currentTableIndexObject);
				uniqueRecordTableData.put(uniqueCode.toString(), uniqueRecordIndex);
			}
			currentTableIndexObject.put(builder.table(), i);

			/*
			 * 每条记录中的join表的数据, key为表名, value为关联表的数据
			 * {
			 *   "join_table1":{
			 *     "join_column1":join_column1_value,
			*       ...
			 *   }
			 *   ...
			 * }
			 */
			JSONObject tempJoinTablesData = new JSONObject();

			/*
			 * 每个主表记录的唯一编号, 以每行记录的所有主表字段值拼成的字符串作为唯一编号
			 */
			for (Map.Entry<String, Object> recordEntry : record.entrySet()) {
				String column = recordEntry.getKey();
				Object value = recordEntry.getValue();
				if (column.indexOf(".") == -1) { // 主表的字段
					if (!appeared) {
						JSONObject columnConfig = columnsConfig.getObject(column);
						uniqueRecord.put(column, render(value, columnConfig));
					}
				} else { // Join表的字段
					/*
					 * a.b.column -> [a, b, column], 表名为b
					 */
					String[] joinTableColumnParts = column.split("\\.");
					String joinTableName = joinTableColumnParts[joinTableColumnParts.length - 2];

					int lastComma = column.lastIndexOf(".");
					String joinColumn = column.substring(lastComma + 1);
					String joinTableString = column.substring(0, lastComma);

					/*
					 * 返回最内层表的数据, 假如join表的字符串joinTableString为a.b.c, 则返回c表的数据
					 */
					String[] prefixJoinTables = joinTableString.split("\\.");
					JSONObject innerJoinTable = uniqueRecord;
					JSONArray innerJoinTableArray;
					for (int j = 0; j < prefixJoinTables.length; j++) {
						String parentTable = j == 0 ? builder.table() : prefixJoinTables[j - 1];
						String currentTable = prefixJoinTables[j];
						TableConfig.Association.Type associationType = AirContext.getAssociation(builder.db(),
								parentTable, currentTable);

						if (innerJoinTable.containsKey(currentTable)) {

							switch (associationType) {
							case ONE_TO_ONE:
							case MANY_TO_ONE:
								innerJoinTable = innerJoinTable.getObject(currentTable);
								break;
							case ONE_TO_MANY:
							case MANY_TO_MANY:
								int currentTableIndex = currentTableIndexObject.containsKey(currentTable)
										? currentTableIndexObject.getInt(currentTable)
										: 0;
								innerJoinTableArray = innerJoinTable.getArray(currentTable);
								if (innerJoinTableArray.length() > currentTableIndex) {
									innerJoinTable = (JSONObject) innerJoinTableArray.get(currentTableIndex);
								} else {
									JSONObject joinTableObject = new JSONObject();
									innerJoinTableArray.add(joinTableObject);
									innerJoinTable = joinTableObject;
									currentTableIndexObject.put(currentTable, currentTableIndex);
								}
								break;

							default:
								break;
							}
						} else {
							JSONObject joinTableObject = new JSONObject();
							switch (associationType) {
							case ONE_TO_ONE:
							case MANY_TO_ONE:
								innerJoinTable.put(prefixJoinTables[j], joinTableObject);
								innerJoinTable = joinTableObject;
								break;
							case ONE_TO_MANY:
							case MANY_TO_MANY:
								innerJoinTableArray = new JSONArray();
								innerJoinTableArray.add(joinTableObject);
								innerJoinTable.put(prefixJoinTables[j], innerJoinTableArray);

								innerJoinTable = joinTableObject;

								currentTableIndexObject.put(currentTable, 0);
								break;

							default:
								break;
							}
						}
					}

					/*
					 * 保存每次循环的join表信息, 以便后续做处理, key为关联表及父表组成的字符串, value为关联表数据
					 */
					JSONObject joinColumnsConfig = AirContext.getColumnsConfig(builder.db(), joinTableName);
					JSONObject joinColumnConfig = joinColumnsConfig.getObject(joinColumn);

					innerJoinTable.put(joinColumn, render(value, joinColumnConfig));

					JSONObject tempJoinTableData;
					if (tempJoinTablesData.containsKey(joinTableString)) {
						tempJoinTableData = tempJoinTablesData.getObject(joinTableString);
					} else {
						tempJoinTableData = new JSONObject();
						tempJoinTablesData.put(joinTableString, tempJoinTableData);
					}
					tempJoinTableData.put(joinColumn, render(value, joinColumnConfig));
				}
			}
			/**
			 * 删除空关联
			 */
			for (String joinTableString : tempJoinTablesData.keySet()) {
				JSONObject tempJoinTableData = tempJoinTablesData.getObject(joinTableString);
				/*
				 * 全部字段为空, 则代表不存在该关联关系, 删除该关联属性
				 */
				boolean exist = tempJoinTableData.values().stream().anyMatch(v -> v != null);
				if (!exist) {
					String[] prefixJoinTables = joinTableString.split("\\.");
					JSONObject innerJoinTableObject = uniqueRecord;
					for (int k = 0; k < prefixJoinTables.length; k++) {
						String parentTable = k == 0 ? builder.table() : prefixJoinTables[k - 1];
						String currentTable = prefixJoinTables[k];
						TableConfig.Association.Type associationType = AirContext.getAssociation(builder.db(),
								parentTable, currentTable);
						switch (associationType) {
						case ONE_TO_ONE:
						case MANY_TO_ONE:
							innerJoinTableObject = innerJoinTableObject.getObject(currentTable);
							if (k == prefixJoinTables.length - 1) {
								innerJoinTableObject.remove(currentTable);
							}
							break;
						case ONE_TO_MANY:
						case MANY_TO_MANY:
							JSONArray innerJoinTableArray = innerJoinTableObject.getArray(currentTable);
							// 删除该循环刚刚添加的对象, 即最后一个
							if (k == prefixJoinTables.length - 1) {
								innerJoinTableArray.remove(innerJoinTableArray.length() - 1);
								if (innerJoinTableArray.length() == 0) {
									innerJoinTableObject.remove(currentTable);
								}
							}
							break;

						default:
							break;
						}
					}
				}

			}
			for (Map.Entry<String, Object> map : currentTableIndexObject.entrySet()) {
				map.setValue((int) map.getValue() + 1);
			}

		}

		for (String key : uniqueRecordTableData.keySet()) {
			result.add(uniqueRecordTableData.getObject(key).getObject("record"));
		}
		return result;
	}

	/**
	 * 渲染单个结果
	 * 
	 * @param data
	 *            数据
	 * @param config
	 *            所有字段配置
	 */
	public void render(Map<String, Object> record, JSONObject columnsConfig) {
		if (columnsConfig == null || columnsConfig.size() == 0)
			return;
		record.replaceAll((k, v) -> render(v, columnsConfig.getObject(k)));
	}

	/**
	 * 渲染字段
	 * 
	 * @param value
	 * @param columnConfig
	 * @return
	 */
	private Object render(Object value, JSONObject columnConfig) {
		if (value == null || columnConfig == null) {
			return value;
		}
		//		if (columnConfig.containsKey(Column.DISPLAY)) {
		//			Object displayConfigObject = columnConfig.get(Column.DISPLAY);
		//			if (displayConfigObject instanceof JSONObject) {
		//				JSONObject displayConfig = (JSONObject) displayConfigObject;
		//				value = displayConfig.get(value.toString());
		//			}
		//		}
		return value;
	}

}
