package com.mxy.air.db.ResultConverter;

public class ConverterUtils {

	public String getTemplateFile(String fileName) {
		String file = ConverterUtils.class.getResource("")+fileName;
		if (file.startsWith("file:")) {
			file = file.substring(5);
		}
		return file;
	}
	public static String replaceParams(String str, String key, String value) {

		str = str.replaceAll("\\$\\{" + key + "\\}", String.valueOf(value));
		return str;
	}
}
