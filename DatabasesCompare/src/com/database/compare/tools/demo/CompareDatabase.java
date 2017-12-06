package com.database.compare.tools.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.database.compare.tools.util.CompareDatabaseUtil;
import com.database.compare.tools.util.LogUtil;
import com.database.compare.tools.util.PropertiesUtil;


public class CompareDatabase{
	public static Logger logger = LogUtil.getLog(CompareDatabase.class);
	public static void main(String[] s) throws Exception{
		Connection conn1 = null;
		Connection conn2 = null;
		try{
			PropertiesUtil instance = PropertiesUtil.getInstance();
			instance.propertyConfigLoader(PropertiesUtil.getPath()+"/config/jdbc.properties");
//			instance.propertyConfigLoader("C:/Users/liang/Desktop/database/config/jdbc.properties");
			String url1 = instance.getConfig("url1", "");
			String user1 = instance.getConfig("user1", "");
			String passwd1 = instance.getConfig("passwd1", "");
			String database1 = instance.getConfig("database1", "");
			String url2 = instance.getConfig("url2", "");
			String user2 = instance.getConfig("user2", "");
			String passwd2 = instance.getConfig("passwd2", "");
			String database2 = instance.getConfig("database2", "");
			//加载数据库驱动
			Class.forName("com.mysql.jdbc.Driver");
			Class.forName("org.postgresql.Driver");
			
			//数据库一mysql与数据库二mysql对比
			
			//第一个库的连接信息
//			String url1 = "jdbc:mysql://211.95.11.159:9099/lwsmz_0619?relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8";
//			String user1 = "root";
//			String passwd1 = "MySq.125@lwsmz";
//			String database1 = "lwsmz_0619";//比较那个库或那个用户
//			String url1 = "jdbc:mysql://127.0.0.1:13306/lwsmz_0619?relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8";
//			String user1 = "root";
//			String passwd1 = "123456";
//			String database1 = "lwsmz_0619";//比较那个库或那个用户
////			String database1 = "abc";//比较那个库或那个用户
//			//第二个库的连接信息
////			String url2 = "jdbc:mysql://211.95.11.159:9099/lwsmz?relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8";
////			String user2 = "root";
////			String passwd2 = "MySq.125@lwsmz";
////			String database2 = "bcdm";//比较那个库或那个用户
//			String url2 = "jdbc:mysql://127.0.0.1:13306/lwsmz?relaxAutoCommit=true&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8";
//			String user2 = "root";
//			String passwd2 = "123456";
//			String database2 = "bcdm";//比较那个库或那个用户
////			String database2 = "test_proc";//比较那个库或那个用户
			
			conn1 = DriverManager.getConnection(url1,user1,passwd1);
			conn2 = DriverManager.getConnection(url2,user2,passwd2);
			logger.info("databases connect success,  database comparison started");
			System.out.println("===============>databases connect success,  database comparison started");
			System.out.println("===============>databases comparing ... ... ...");
			//比较表及表结构
			CompareDatabaseUtil.compareTables(conn1,conn2,database1,database2);
			//比较视图及结构
			CompareDatabaseUtil.compareViews(conn1,conn2,database1,database2);			
			//比较索引
			CompareDatabaseUtil.compareIndexs(conn1,conn2,database1,database2);
//			
//			ArrayList<CompareDataInfo> list = new ArrayList<CompareDataInfo>();
//			
//			CompareDataInfo di02 = new CompareDataInfo("t_sys_menu","id");
//			di02.addColumn("name");
//			di02.addColumn("parentid");
//			di02.addColumn("path");
//			list.add(di02);
//			
//			CompareDataInfo di01 = new CompareDataInfo("t_user","user_id");
//			di01.addColumn("user_name");
//			di01.addColumn("account");
//			list.add(di01);
//			
//			//比较表数据
//			CompareDatabaseUtil.compareDatas(conn1, conn2, list);
			//比较触发器
			CompareDatabaseUtil.compareTriggers(conn1,conn2,database1,database2);
			//比较存储过程
			CompareDatabaseUtil.compareProcedures(conn1,conn2,database1,database2);
			//比较存储函数
			CompareDatabaseUtil.compareFunctions(conn1,conn2,database1,database2);
		}
		catch(Exception e){
			logger.error(e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(conn1, null, null);
			CompareDatabaseUtil.close(conn2, null, null);
			System.out.println("===============>Database comparison completed!");
			logger.info("Database comparison completed!");
		}
		
	}
	
	
}

