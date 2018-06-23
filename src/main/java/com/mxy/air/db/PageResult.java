package com.mxy.air.db;

import com.mxy.air.json.JSONArray;
import com.mxy.air.json.JSONObject;

/**
 * 分页结果
 * 
 * @author mengxiangyun
 *
 */
public class PageResult {

	/**
	 * 分页结果属性
	 * 
	 * @author mengxiangyun
	 *
	 */
	public enum ATTRIBUTE {
		START, END, TOTAL, DATA
	}

	public PageResult() {
	}
	
	public static JSONObject wrap(long start, long end, long total, JSONArray data) {
		return new JSONObject().put(ATTRIBUTE.START, start).put(ATTRIBUTE.END, end).put(ATTRIBUTE.TOTAL, total)
				.put(ATTRIBUTE.DATA, data);
	}

}
