package com.mxy.air.db.ResultConverter;

import com.itextpdf.text.*;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class PDFResultConverter {

	List<String> headers;

	private PDFResultConverter(List<String> header) {
		this.headers = header;
	}
	public static PDFResultConverter getInstance(List<String> header) {
		return new PDFResultConverter(header);
	}



	public InputStream export(List<Map<String, Object>> data) {

		Document doc = new Document();

		byte[] result=null;
		ByteArrayOutputStream baos = null;


		try {

			Rectangle rectPageSize = new Rectangle(PageSize.A4);
			doc = new Document(rectPageSize);// 可配其余4个参数，如（rectPageSize，60,60,60,60）页面边距

			baos = new ByteArrayOutputStream();//构建字节输出流
			PdfWriter.getInstance(doc,baos);//将PDF文档对象写入到流

			doc.open();

			// 解决中文问题
			BaseFont bfChinese = BaseFont.createFont("STSong-Light", "UniGB-UCS2-H", BaseFont.NOT_EMBEDDED);

			//信息数据表

			//创建PdfTable对象
			PdfPTable table=new PdfPTable(headers.size());

			//设置各列的列宽
			table.setTotalWidth(new float[]{100,350,350});

			Font tableTitleFont = new Font(bfChinese,18,Font.BOLD);//表头
			Font dataFont = new Font(bfChinese,12,Font.NORMAL);

			//表头添加表格内容
			for (String header : headers) {
				table.addCell(getPDFCell(header, tableTitleFont));
			}

			//遍历数据集合进行添加
			for (int i = 0; i < data.size(); i++) {
				for (Map.Entry<String, Object> entry : data.get(i).entrySet()) {
					table.addCell(getPDFCell(entry.getValue().toString(), dataFont));
				}
			}

			table.setWidthPercentage(110);//表宽度设置可不受页面边距影响
			doc.add(table);

			if(doc != null){
				doc.close();
			}
			return new ByteArrayInputStream(baos.toByteArray());
		} catch(Exception e) {
		} finally {
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}
	private static PdfPCell getPDFCell(String string, Font font) {
		//创建单元格对象，将内容与字体放入段落中作为单元格内容
		PdfPCell cell=new PdfPCell(new Paragraph(string,font));

		cell.setHorizontalAlignment(Element.ALIGN_CENTER);
		cell.setVerticalAlignment(Element.ALIGN_MIDDLE);

		//设置最小单元格高度
		cell.setMinimumHeight(30);
		return cell;
	}
}
