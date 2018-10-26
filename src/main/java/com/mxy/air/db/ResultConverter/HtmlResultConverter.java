package com.mxy.air.db.ResultConverter;

import com.mxy.air.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

public class HtmlResultConverter extends ConverterUtils {

	private static final String TEMPLATE_FILENAME = "html_template.html"; // 模板文件名
	private static final String TR = "<tr>${tr}</tr>"; // 模板文件名
	private static final String TD = "<td>${td}</td>";

	private HtmlResultConverter(List<String> header, JSONObject config) {
		this.headers = header;
		this.config = config;
	}
	public static HtmlResultConverter getInstance(List<String> header, JSONObject config) {
		return new HtmlResultConverter(header, config);
	}

	public InputStream export(List<Map<String, Object>> data) throws Exception {
		InputStream inputStream = this.getClass().getResourceAsStream(TEMPLATE_FILENAME);

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

		StringBuffer html = new StringBuffer();
		String str;
		while ((str = reader.readLine()) != null) {
			html.append(str);
		}
		String templateContent = html.toString();

		StringBuilder tableContent = new StringBuilder();

		if (headers != null && !headers.isEmpty()) {
			StringBuilder tds = new StringBuilder();
			for (String header : headers) {
				tds.append(replaceParams(TD, "td", header));
			}
			tableContent.append(replaceParams(TR, "tr", tds.toString()));
		}
		System.out.println("headers--->"+tableContent.toString());

		for (Map<String, Object> map : data) {
			StringBuilder tds = new StringBuilder();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				if (entry.getValue() instanceof Map) {
					//TODO
				} else {
					tds.append(replaceParams(TD, "td", buildColumn(entry.getKey(), entry.getValue())));
				}
			}
			tableContent.append(replaceParams(TR, "tr", tds.toString()));
		}
		templateContent = replaceParams(templateContent, "tableContent", tableContent.toString());
		System.out.println(templateContent);

		return create(templateContent);
	}
	public InputStream create(String html) throws Exception {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();//构建字节输出流
		// 追加写入日志
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(baos, "UTF-8"));
		bw.write(html);
		bw.flush();
		bw.close();

		return new ByteArrayInputStream(baos.toByteArray());
	}
}
