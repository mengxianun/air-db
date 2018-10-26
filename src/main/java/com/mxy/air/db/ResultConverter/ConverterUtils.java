package com.mxy.air.db.ResultConverter;

import com.mxy.air.db.AirContext;
import com.mxy.air.db.config.TableConfig;
import com.mxy.air.json.JSON;
import com.mxy.air.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ConverterUtils {

	List<String> headers;
	JSONObject config;

	public File getTemplateFile(String fileName) throws Exception {
		String file = ConverterUtils.class.getResource("")+fileName;
		if (file.startsWith("file:")) {
			file = file.substring(5);
		}
		return JSON.getPath(file).toFile();
	}
	public static String replaceParams(String str, String key, String value) {

		str = str.replaceAll("\\$\\{" + key + "\\}", String.valueOf(value));
		return str;
	}
	protected String buildColumn(String column, Object value) {

		JSONObject columnConfig = config.getObject(column);
		if (columnConfig == null || columnConfig.size() == 0) {
			return value.toString();
		} else if (columnConfig.containsKey(TableConfig.Column.CODE)) {
			JSONObject columnCode = columnConfig.getObject(TableConfig.Column.CODE);
			return columnCode.get(value.toString()).toString();
		} else if (columnConfig.containsKey(TableConfig.Column.FORMAT)) {
			String columnType = columnConfig.getString(TableConfig.Column.TYPE);
			JSONObject columnFormat = columnConfig.getObject(TableConfig.Column.FORMAT);
			if (columnFormat.containsKey(TableConfig.Format.DATETIME)) {
				String pattern = columnFormat.getString(TableConfig.Format.DATETIME);
				String formatedValue = "";
				if ("bigint".equals(columnType)) {
					Instant instant = Instant.ofEpochMilli(Long.valueOf(value.toString()));
					DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
					formatedValue = fmt.format(instant.atZone(ZoneId.systemDefault()));
				} else {
					try {
						LocalDateTime datetime = LocalDateTime.parse(value.toString());
						formatedValue = datetime.format(DateTimeFormatter.ofPattern(pattern));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				return formatedValue;
			} else {
				return "";
			}
		} else {
			return value.toString();
		}
	}
}
