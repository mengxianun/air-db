package com.mxy.air.db.jdbc;

/**
 * Jdbc分页对象
 * 页码从1开始, 起始行也从1开始
 * 例: 第1页, 每页5条: pageNum=1, pageSize=5, start=1, end=5
 * 
 * @author mengxiangyun
 *
 */
public class Page {

	// 当前页, 从1开始
	private int pageNum;
	// 每页大小
	private int pageSize;
	// 起始行, 从1开始
	private long start;
	// 结束行
	private long end;
	// 总页数
	private int pages;
	// 总记录数
	private long total;

	public Page() {}

	/**
	 * 由当前页码和每页大小构建Page对象
	 * @param pageNum
	 * @param pageSize
	 */
	public Page(int pageNum, int pageSize) {
		this.pageNum = pageNum;
		this.pageSize = pageSize;
		this.start = pageNum > 0 ? ((pageNum - 1) * pageSize + 1) : 1;
		this.end = start + pageSize;
	}
	
	/**
	 * 由起始结束行构建Page对象
	 * @param start
	 * @param end
	 */
	public Page(long start, long end) {
		this.start = start;
		this.end = end;
		this.pageSize = (int) (end - start);
		this.pageNum = start/pageSize + (start % pageSize) > 0 ? 1 : 0;
	}

	public int getPageNum() {
		return pageNum;
	}

	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public int getPages() {
		return pages;
	}

	public void setPages(int pages) {
		this.pages = pages;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
		this.pages = (int) (total / pageSize + (total % pageSize == 0 ? 0 : 1));
	}

}
