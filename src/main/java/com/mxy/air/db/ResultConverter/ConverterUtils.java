package com.mxy.air.db.ResultConverter;

import com.mxy.air.json.JSON;

import java.io.File;
import java.io.IOException;

public class ConverterUtils {

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
}
