package com.database.compare.tools.util;


/**
 * 字符串工具类
 * 
 * @author Cat_L
 *
 */
public class StringUtil {
	/**
	 * 去除字符串首位空格 字符串trim。如果字符串为null，返回空，
	 * 
	 * @param s
	 *            入参
	 * @return 返回字符串去除首位空格结果
	 */
	public static String trim(String s) {
		if (s == null)
			return "";
		return s.trim();
	}

	/**
	 * 去除字符串中的空格回车换行空行
	 * 
	 * @param str
	 *            入参
	 * @return 返回新的字符串
	 */
	public static String removeBlankSpaceOrLine(String str) {
		if (isNotBlank(str)) {
			str = str.replaceAll("\\s", "");
		}
		return str;
	}

	/**
	 * 判断charSequence类型参数是否为null,空或""
	 * @param cs 要判断的参数
	 * @return 当受检查的值时 null 或 ""或空时，返回值时true
	 */
	public static boolean isBlank(final CharSequence cs) {
	    int strLen;
	    if (cs == null || (strLen = cs.length()) == 0) {
	        return true;
	    }
	    for (int i = 0; i < strLen; i++) {
	        if (Character.isWhitespace(cs.charAt(i)) == false) {
	            return false;
	        }
	    }
	    return true;
	}
	
	/**
	 * 判断charSequence类型参数是否不为null,空和""
	 * @param cs 要判断的参数
	 * @return 当受检查的值不为 null 或,""和空时，返回值时true
	 */
	public static boolean isNotBlank(CharSequence cs){
		return !isBlank(cs);
	}
}
