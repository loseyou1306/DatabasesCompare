package com.database.compare.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;

import com.database.compare.tools.util.CompareDatabaseUtil;
import com.database.compare.tools.util.LogUtil;
import com.database.compare.tools.util.PropertiesUtil;


public class DatabaseCompare {
	public static void main(String[] args) throws Exception{
		System.out.println("====================程序说明====================");
		System.out.println("功能：比较两个数据库结构之间的差异。");
		System.out.println("      比较内容有：表、视图、索引、触发器、存储过程、函数。");
		System.out.println("      只支持MySQL和PostgreSQL两种数据库。");
		System.out.println("作者：Cat_L");
		System.out.println("发布日期：2017年11月09日");
		System.out.println("=================================================");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		File dir = new File(".");
		File defaultFile = new File(dir,"jdbc.properties");
		String flag = "请输入数据库连接配置文件路径（默认路径为"+defaultFile.getAbsolutePath()+",直接回车表示使用默认路径）:";
		String path = "";
		while(true){
			System.out.print(flag);
			path = br.readLine().trim();
			if(path.equals("")){
				path = defaultFile.getAbsolutePath();
			}
			File f = new File(path);
			if(!f.exists()){
				System.out.println("您输入的文件路径["+f.getAbsolutePath()+"]不存在！");
			}
			if(f.isDirectory()){
				System.out.println("您输入的文件路径["+f.getAbsolutePath()+"]是一个目录！");
			}
			if(f.exists() && f.isFile()){
				path = f.getAbsolutePath();
				break;
			}			
		}
		br.close();
		System.out.println("配置文件路径为："+path);
		String url1 = "";
		String user1 = "";
		String passwd1 = "";
		String database1 = "";
		String url2 = "";
		String user2 = "";
		String passwd2 = "";
		String database2 = "";
		try{
			PropertiesUtil instance = PropertiesUtil.getInstance();
			instance.propertyConfigLoader(path);
			url1 = instance.getConfig("url1", "").trim();
			user1 = instance.getConfig("user1", "").trim();
			passwd1 = instance.getConfig("passwd1", "").trim();
			database1 = instance.getConfig("database1", "").trim();
			url2 = instance.getConfig("url2", "").trim();
			user2 = instance.getConfig("user2", "").trim();
			passwd2 = instance.getConfig("passwd2", "").trim();
			database2 = instance.getConfig("database2", "").trim();
			if(url1.length()<=0){
				throw new Exception("第一个数据库的连接信息[url1]为空");
			}
			if(user1.length()<=0){
				throw new Exception("第一个数据库的用户名[user1]为空");
			}
			if(passwd1.length()<=0){
				throw new Exception("第一个数据库的密码[passwd1]为空");
			}
			if(database1.length()<=0){
				throw new Exception("第一个数据库的数据库名[database1]为空");
			}
			if(url2.length()<=0){
				throw new Exception("第二个数据库的连接信息[url2]为空");
			}
			if(user2.length()<=0){
				throw new Exception("第二个数据库的用户名[user2]为空");
			}
			if(passwd2.length()<=0){
				throw new Exception("第二个数据库的密码[passwd2]为空");
			}
			if(database2.length()<=0){
				throw new Exception("第二个数据库的数据库名[database2]为空");
			}
		}
		catch(Exception e){
			System.out.println("读取配置文件出错："+e.getMessage());
			System.exit(0);
		}
		
		
		
		
		//加载数据库驱动
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("org.postgresql.Driver");
		Connection conn1 = null;
		Connection conn2 = null;
		try{
			try{
				conn1 = DriverManager.getConnection(url1,user1,passwd1);
			}
			catch(Exception e){
				throw new Exception("第一个数据库连接不上");
			}
			try{
				conn2 = DriverManager.getConnection(url2,user2,passwd2);
			}
			catch(Exception e){
				throw new Exception("第二个数据库连接不上");
			}
			
			System.out.println("===============>databases connect success,  database comparison started");
			System.out.println("===============>databases comparing ... ... ...");
			//比较表及表结构
			CompareDatabaseUtil.compareTables(conn1,conn2,database1,database2);
			//比较视图及结构
			CompareDatabaseUtil.compareViews(conn1,conn2,database1,database2);			
			//比较索引
			CompareDatabaseUtil.compareIndexs(conn1,conn2,database1,database2);
			//比较触发器
			CompareDatabaseUtil.compareTriggers(conn1,conn2,database1,database2);
			//比较存储过程
			CompareDatabaseUtil.compareProcedures(conn1,conn2,database1,database2);
			//比较存储函数
			CompareDatabaseUtil.compareFunctions(conn1,conn2,database1,database2);
		}
		catch(Exception e){
			System.out.println("比较数据库出错："+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(conn1, null, null);
			CompareDatabaseUtil.close(conn2, null, null);
		}
		System.out.println("数据库对比结果保存在文件["+new File(dir,LogUtil.getLogFilePath())+"]中");

	}

}
