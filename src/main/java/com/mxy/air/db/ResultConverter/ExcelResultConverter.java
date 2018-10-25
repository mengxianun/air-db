package com.mxy.air.db.ResultConverter;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class ExcelResultConverter {

	List<String> headers;

	private ExcelResultConverter(List<String> header) {
		this.headers = header;
		this.init();
		this.initStyle();
	}
	public static ExcelResultConverter getInstance(List<String> header) {
		return new ExcelResultConverter(header);
	}

	private HSSFWorkbook wb;
	private HSSFSheet sheet;

	private HSSFCellStyle blankStyle;
	private HSSFCellStyle headerStyle;
	private HSSFCellStyle headerBottomStyle;
	private HSSFCellStyle titleStyle;
	private HSSFCellStyle titleStyle1;
	private HSSFCellStyle leftStyle;
	private HSSFCellStyle rightStyle;
	private HSSFCellStyle bottomStyle;
	private HSSFCellStyle leftBottomStyle;
	private HSSFCellStyle rightBottomStyle;
	private HSSFCellStyle labelStyle;
	private HSSFCellStyle valueStyle;
	private HSSFCellStyle valueStyle1;
	private HSSFCellStyle tableHeaderStyle;
	private HSSFCellStyle tableDataStyle;
	private HSSFCellStyle tableDataStyle1;

	public InputStream export(List<Map<String, Object>> data) {

		HSSFRow headerRow = sheet.createRow(0);
		int headerIndex = 0;
		for (String header : headers) {
			headerRow.createCell(headerIndex++).setCellValue(header);
		}
		for(int i = 0; i < data.size(); i++) {
			int rowNO = i+1;
			HSSFRow dataRow = sheet.createRow(rowNO);
			Map<String, Object> map = data.get(i);
			int j = 0;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				dataRow.createCell(j++).setCellValue(entry.getValue().toString());
			}
		}
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			wb.write(os);
			return new ByteArrayInputStream(os.toByteArray());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
			} catch (Exception e) {
			}
		}
		return null;
	}
	private void init() {
		wb = new HSSFWorkbook();
		sheet = wb.createSheet("报表内容");
//		sheet.setDefaultColumnWidth(4);

		/** 自定义颜色 */
//		HSSFPalette customPalette = wb.getCustomPalette();
//		customPalette.setColorAtIndex(HSSFColor.LIGHT_BLUE.index, (byte) 0, (byte) 176, (byte) 240);//自定义颜色1
//		customPalette.setColorAtIndex(HSSFColor.LIGHT_GREEN.index, (byte) 221, (byte) 235, (byte) 247);//自定义颜色2
	}
	private void initStyle() {
		//TODO 加载式样
	}
	public HSSFWorkbook createExcel() throws Exception {


		return wb;
	}
}
