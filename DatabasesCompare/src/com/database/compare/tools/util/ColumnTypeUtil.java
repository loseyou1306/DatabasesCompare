package com.database.compare.tools.util;

/**
 * 表格字段类型统一转换类
 * @author Cat_L
 *
 */
public class ColumnTypeUtil {
	/**
	 * 入参:数据库类型DBtype,
	 * @param dbType 数据库类型
	 * @param type 需要处理的数据库表格字段类型
	 * @return 返回通用数据类型
	 */
	public static String getGenericType(DBType dbType,String type){
		// TODO 对不同数据库进行处理
		
		
		//参数处理:字符串两端空格去掉,大小写统一处理
		if(StringUtil.isNotBlank(type)){
			return type.trim().toLowerCase();
		}
		return "";
	}

}
