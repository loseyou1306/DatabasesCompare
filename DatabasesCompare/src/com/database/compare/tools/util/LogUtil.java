package com.database.compare.tools.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
/**
 * 日志配置
 * @author Cat_L
 *
 */
public class LogUtil {
	//定义输出文件前缀(年-月-日-时-分-秒)
	private static String date;
	static{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		date = sdf.format(new Date());
		init();
	}
	
	public static String getLogFilePath(){
		return date + ".log";
	}
	
	/**
	 * 初始化日志配置
	 */
	public static void init(){
		LogManager.resetConfiguration();
		Properties ps = new Properties();
		
		ps.setProperty("log4j.rootLogger", "debug,Console,fileLog");
		
		ps.setProperty("log4j.appender.Console", "org.apache.log4j.ConsoleAppender");
		ps.setProperty("log4j.appender.Console.Target", "System.out");
		ps.setProperty("log4j.appender.Console.Threshold", "info");
		ps.setProperty("log4j.appender.Console.layout", "org.apache.log4j.PatternLayout");
		ps.setProperty("log4j.appender.Console.layout.ConversionPattern", "%m%n");
		
		ps.setProperty("log4j.appender.fileLog", "org.apache.log4j.FileAppender");
		ps.setProperty("log4j.appender.fileLog.File", getLogFilePath());
		ps.setProperty("log4j.appender.fileLog.encoding", "UTF-8");
		ps.setProperty("log4j.appender.fileLog.Threshold", "info");
		ps.setProperty("log4j.appender.fileLog.layout", "org.apache.log4j.PatternLayout");
		ps.setProperty("log4j.appender.fileLog.layout.ConversionPattern", "%d{HH:mm:ss,SSS} %-5p - %m%n");
		
		PropertyConfigurator.configure(ps);		
	}
	
	/**
	 * @param clazz 泛型类类型
	 * @return 返回日志对象
	 */
	public static Logger getLog(Class<?> clazz) {

		Logger logger = Logger.getLogger(clazz);
//		// 清空Appender，特別是不想使用現存實例時一定要初期化
//		logger.removeAllAppenders();
//		// 设定Logger級別。
//		logger.setLevel(Level.INFO);
//		// 设定是否继承父Logger。默认为true，继承root输出；设定false后将不出输出。
//		logger.setAdditivity(true);
//		// 生成新的Appender
//		FileAppender appender = new RollingFileAppender();
//		PatternLayout layout = new PatternLayout();
//		// log的输出形式
//		layout.setConversionPattern("[%d{yyyy-MM-dd HH:mm:ss}] %p %l : %m%n");
//		appender.setLayout(layout);
//		// log输出路径
//		appender.setFile("logs/"+date + ".log");
//		// log的字符编码
//		appender.setEncoding("UTF-8");
//		// 日志合并方式： true:在已存在log文件后面追加,false:新log覆盖以前的log
//		appender.setAppend(true);
//		// 适用当前配置
//		appender.activateOptions();
//		// 将新的Appender加到Logger中
//		logger.addAppender(appender);
		return logger;
	}
}
