package com.database.compare.tools.util;

import java.util.ArrayList;

public class CompareDataInfo {
	private String tableName;
	private String idColumn;
	private ArrayList<String> columns = new ArrayList<String>();
	/**
	 * 获取表名
	 * @return
	 */
	public String getTableName(){
		return tableName;
	}
	/**
	 * 获取表唯一字段（根据该字段进行对比）
	 * @return
	 */
	public String getIdColumn(){
		return idColumn;
	}
	/**
	 * 获取比较字段列表
	 * @return
	 */
	public ArrayList<String> getColumns(){
		return columns;
	}
	/**
	 * 构建一个数据对比信息
	 * @param tableName  待对比的表名
	 * @param idColumn  表的主键,可以是一个表达式
	 */
	public CompareDataInfo(String tableName,String idColumn){
		this.tableName = tableName;
		this.idColumn = idColumn;
	}
	/**
	 * 增加一个比较字段
	 * @param column
	 */
	public void addColumn(String column){
		columns.add(column);
	}

}
