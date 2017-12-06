package com.database.compare.tools.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;



/**
 * 数据库比较工具类
 * @author liang
 *
 */
public class CompareDatabaseUtil {
	public static Logger logger = LogUtil.getLog(CompareDatabaseUtil.class);
	
	/**
	 * 出现问题的提示字符串
	 */
	public static final String ERROR = "===============>";

	/**
	 * 根据数据库连接判断数据库类型
	 * @param conn   数据连接
	 * @return 返回数据库类型
	 * @throws Exception 可能出现的异常
	 */
	public static DBType getDBTypeFromConn(Connection conn) throws Exception{
		if(conn==null){
			return DBType.Unknown;
		}
		DatabaseMetaData meta = conn.getMetaData();
		if(meta!=null){
			String name = StringUtil.trim(meta.getDatabaseProductName());
			if(name.toLowerCase().indexOf("mysql")>=0){
				return DBType.MySQL;
			}
//			if(name.toLowerCase().indexOf("oracle")>=0){
//				return DBType.Oracle;
//			}
			if(name.toLowerCase().indexOf("postgre")>=0){
				return DBType.Postgre;
			}
//			if(name.toLowerCase().indexOf("db2")>=0){
//				return DBType.DB2;
//			}			
		}		
		return DBType.Unknown;
	}
	
	/**
	 * 对比表结构
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2 数据库2
	 * @throws Exception 可能出现的异常
	 */
	public static void compareTables(Connection conn1,Connection conn2,String database1,String database2) throws Exception{
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare tables!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare tables!!");
			}
			
			//数据库一
			List<String> tables1 = new ArrayList<String>();//数据库表名
			HashMap<String,String> tableIds1 = new HashMap<String,String>();//数据库表名，表ID
			if(type1==DBType.MySQL){
				Util4MySQL.getTables(conn1,database1,tables1,tableIds1);
			}
			if(type1==DBType.Postgre){
				Util4Postgres.getTables(conn1,database1,tables1,tableIds1);
			}
			//数据库二
			List<String> tables2 = new ArrayList<String>();
			HashMap<String,String> tableIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getTables(conn2,database2,tables2,tableIds2);
			}
			if(type2==DBType.Postgre){
				Util4Postgres.getTables(conn2,database2,tables2,tableIds2);
			}
			//比较表数量
			if(tables1.size()!=tables2.size()){
				logger.info(ERROR+"表数量为："+tables1.size()+"/"+tables2.size());
			}
			else
				logger.info("表数量为："+tables1.size()+"/"+tables2.size());
			//比较表差异
			List<String> tables = new ArrayList<String>();			
			for(int i=0;i<tables1.size();i++){
				String name = tables1.get(i);
				if(!tables2.contains(name)){
					logger.info(ERROR+"第一个库中多表："+name);
				}
				else{
					if(!tables.contains(name))
						tables.add(name);
				}
			}
			
			for(int i=0;i<tables2.size();i++){
				String name = tables2.get(i);
				if(!tables1.contains(name)){
					logger.info(ERROR+"第二个库中多表："+name);
				}
				else{
					if(!tables.contains(name))
						tables.add(name);
				}
			}
			
			for(int i=0;i<tables.size();i++){
				String tablename = tables.get(i);
				compareOneTable("0",conn1,conn2,database1,database2,tablename,tableIds1.get(tablename),tableIds2.get(tablename));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}
	
	/**
	 * 对比视图结构
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @throws Exception
	 */
	public static void compareViews(Connection conn1,Connection conn2,String database1,String database2) throws Exception{
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare views!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare views!");
			}
			

			List<String> views1 = new ArrayList<String>();//数据库表名
			HashMap<String,String> viewIds1 = new HashMap<String,String>();//数据库表名，表ID
			if(type1==DBType.MySQL){
				Util4MySQL.getViews(conn1,database1,views1,viewIds1);
			}
			List<String> views2 = new ArrayList<String>();
			HashMap<String,String> viewIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getViews(conn2,database2,views2,viewIds2);
			}
			//比较视图数量
			if(views1.size()!=views2.size()){
				logger.info(ERROR+"视图数量为："+views1.size()+"/"+views2.size());
			}
			else
				logger.info("视图数量为："+views1.size()+"/"+views2.size());
			
			//比较试图差异
			List<String> views = new ArrayList<String>();			
			for(int i=0;i<views1.size();i++){
				String name = views1.get(i);
				if(!views2.contains(name)){
					logger.info(ERROR+"第一个库中多视图："+name);
				}
				else{
					if(!views.contains(name))
						views.add(name);
				}
			}
			
			for(int i=0;i<views2.size();i++){
				String name = views2.get(i);
				if(!views1.contains(name)){
					logger.info(ERROR+"第二个库中多视图："+name);
				}
				else{
					if(!views.contains(name))
						views.add(name);
				}
			}
			
			for(int i=0;i<views.size();i++){
				String tablename = views.get(i);
				compareOneTable("1",conn1,conn2,database1,database2,tablename,viewIds1.get(tablename),viewIds2.get(tablename));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}
	
	/**
	 * 比较单张表的字段
	 * @param type  0:表   1：视图
	 * @param conn1  数据库连接1
	 * @param conn2  数据库连接2
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @param tablename  表名
	 * @param oid1  表ID1
	 * @param oid2 表ID2
	 * @throws Exception
	 */
	public static void compareOneTable(String type,Connection conn1,Connection conn2,String database1,String database2,String tablename,String tableId1,String tableId2) throws Exception{
		try{
			String flag = "表";
			if(type.equals("1")){
				flag = "视图";
			}
			
			/**
			 * 查询mysql某个库中某个表中的所有字段：column_name	列名，dafault_value 默认值，nullable 是否为空，
			 * data_type 数据类型，data_length 数据长度，data_precision 小数点位数，data_scale 数字总位数（整数+小数）
			 * 字段属性：字段名称、默认值,是否非空,字段类型、字段长度、小数点位数,数字总位数
			 */
			// 字段属性：字段名称、字段类型、字段长度、是否非空
			//String[] info = new String[]{"名称","字段类型","字段长度","是否非空"};
			String[] info = new String[]{"名称","默认值","是否非空","字段类型","字段长度","小数点位数","数字总位数"};
			
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare column in table or view!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare column in table or view!");
			}			

			List<String> columns1 = new ArrayList<String>();//表字段名
			HashMap<String,String[]> columnInfos1 = new HashMap<String,String[]>();//表字段属性
			if(type1==DBType.MySQL){
				Util4MySQL.getOneTableColumnInfos(conn1,database1,type1,tablename,tableId1,columns1,columnInfos1);
			}
			if(type1==DBType.Postgre){
				Util4Postgres.getOneTableColumnInfos(conn1,database1,type1,tablename,tableId1,columns1,columnInfos1);
			}
			
			
			List<String> columns2 = new ArrayList<String>();//表字段名
			HashMap<String,String[]> columnInfos2 = new HashMap<String,String[]>();//表字段属性
			if(type2==DBType.MySQL){
				Util4MySQL.getOneTableColumnInfos(conn2,database2,type1,tablename,tableId2,columns2,columnInfos2);
			}
			if(type2==DBType.Postgre){
				Util4Postgres.getOneTableColumnInfos(conn2,database2,type1,tablename,tableId2,columns2,columnInfos2);
			}
			//比较表数量
			if(columns1.size()!=columns2.size()){
				logger.info(ERROR+flag+"["+tablename+"]的字段数量为："+columns1.size()+"/"+columns2.size());
			}
			else
				logger.info(flag+"["+tablename+"]的字段数量为："+columns1.size()+"/"+columns2.size());
//			System.out.println(flag+"["+tablename+"]的字段数量为："+columns1.size()+"/"+columns2.size());
			
			List<String> columns = new ArrayList<String>();
			
			for(int i=0;i<columns1.size();i++){
				String name = columns1.get(i);
				if(!columns2.contains(name)){
					logger.info(ERROR+"第一个库中"+flag+"["+tablename+"]多字段："+name);
				}
				else{
					if(!columns.contains(name))
						columns.add(name);
				}
			}
			
			for(int i=0;i<columns2.size();i++){
				String name = columns2.get(i);
				if(!columns1.contains(name)){
					logger.info(ERROR+"第二个库中"+flag+"["+tablename+"]多字段："+name);
//					System.out.println(ERROR+"第二个库中"+flag+"["+tablename+"]多字段："+name);
				}
				else{
					if(!columns.contains(name))
						columns.add(name);
				}
			}
			//对比指定表中各个列属性是否一致
			/*for(int i=0;i<columns.size();i++){
				String name = columns.get(i);
				String[] s1 = columnInfos1.get(name);
				String[] s2 = columnInfos2.get(name);			
				for(int k=0;k<info.length;k++){
					if(!s1[k].equals(s2[k])){
					logger.info(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
						System.out.println(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
					}
				}
			}*/
			for(int i=0;i<columns.size();i++){
				String name = columns.get(i);
				String[] s1 = columnInfos1.get(name);
				String[] s2 = columnInfos2.get(name);			
				for(int k=0;k<info.length;k++){
					if(StringUtil.isBlank(s1[k])){
						if(StringUtil.isNotBlank(s2[k])){
							logger.info(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//							System.out.println(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
						}
					}else{
						if(!s1[k].equals(s2[k])){
							logger.info(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//							System.out.println(ERROR+""+flag+"["+tablename+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
						}
					}
				}
			}
		}
		catch(Exception e){
			throw e;
		}	
	}	
	
	/**
	 * 对比索引
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2 数据库2
	 * @throws Exception
	 */
	public static void compareIndexs(Connection conn1,Connection conn2,String database1,String database2) throws Exception{
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare indexs!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare indexs!");
			}
			

			List<String> indexs1 = new ArrayList<String>();//数据库索引名
			HashMap<String,String> indexIds1 = new HashMap<String,String>();//数据库索引名，索引ID
			if(type1==DBType.MySQL){
				Util4MySQL.getIndexs(conn1,database1,indexs1,indexIds1);
			}
			List<String> indexs2 = new ArrayList<String>();
			HashMap<String,String> indexIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getIndexs(conn2,database2,indexs2,indexIds2);
			}
			//比较索引数量
			if(indexs1.size()!=indexs2.size()){
				logger.info(ERROR+"索引数量为："+indexs1.size()+"/"+indexs2.size());
			}
			else
				logger.info("索引数量为："+indexs1.size()+"/"+indexs2.size());
//			System.out.println("索引数量为："+indexs1.size()+"/"+indexs2.size());
			
			//比较索引差异
			List<String> indexs = new ArrayList<String>();			
			for(int i=0;i<indexs1.size();i++){
				String name = indexs1.get(i);
				if(!indexs2.contains(name)){
					logger.info(ERROR+"第一个库中多索引："+name);
//					System.out.println(ERROR+"第一个库中多索引："+name);
				}
				else{
					if(!indexs.contains(name))
						indexs.add(name);
				}
			}
			
			for(int i=0;i<indexs2.size();i++){
				String name = indexs2.get(i);
				if(!indexs1.contains(name)){
					logger.info(ERROR+"第二个库中多索引："+name);
//					System.out.println(ERROR+"第二个库中多索引："+name);
				}
				else{
					if(!indexs.contains(name))
						indexs.add(name);
				}
			}
			
			for(int i=0;i<indexs.size();i++){
				String name = indexs.get(i);
				compareOneIndex(conn1,conn2,database1,database2,name,indexIds1.get(name),indexIds2.get(name));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}
	
	/**
	 * 比较单个索引的字段
	 * @param conn1  数据库连接1
	 * @param conn2  数据库连接2
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @param indexName  索引名
	 * @param indexId1   索引ID1
	 * @param indexId2  索引ID2
	 * @throws Exception
	 */
	public static void compareOneIndex(Connection conn1,Connection conn2,String database1,String database2,String indexName,String indexId1,String indexId2) throws Exception{
		try{			
			/**
			 * 字段属性：字段名称、是否唯一、字段排序
			 */
			String[] info = new String[]{"字段名称","是否唯一","字段排序"};
			
			
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare IndexColumnInfos!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare IndexColumnInfos!");
			}			

			List<String> columns1 = new ArrayList<String>();//表字段名
			HashMap<String,String[]> columnInfos1 = new HashMap<String,String[]>();//表字段属性
			if(type1==DBType.MySQL){
				Util4MySQL.getOneIndexColumnInfos(conn1,database1,indexName,indexId1,columns1,columnInfos1);
			}
			List<String> columns2 = new ArrayList<String>();//表字段名
			HashMap<String,String[]> columnInfos2 = new HashMap<String,String[]>();//表字段属性
			if(type2==DBType.MySQL){
				Util4MySQL.getOneIndexColumnInfos(conn2,database2,indexName,indexId2,columns2,columnInfos2);
			}
			//比较表数量
			if(columns1.size()!=columns2.size()){
				logger.info(ERROR+"索引["+indexName+"]的字段数量为："+columns1.size()+"/"+columns2.size());
//				System.out.print(ERROR);
			}
			else
				logger.info("索引["+indexName+"]的字段数量为："+columns1.size()+"/"+columns2.size());
//			System.out.println("索引["+indexName+"]的字段数量为："+columns1.size()+"/"+columns2.size());
			
			List<String> columns = new ArrayList<String>();
			
			for(int i=0;i<columns1.size();i++){
				String name = columns1.get(i);
				if(!columns2.contains(name)){
					logger.info(ERROR+"第一个库中索引["+indexName+"]多字段："+name);
//					System.out.println(ERROR+"第一个库中索引["+indexName+"]多字段："+name);
				}
				else{
					if(!columns.contains(name))
						columns.add(name);
				}
			}
			
			for(int i=0;i<columns2.size();i++){
				String name = columns2.get(i);
				if(!columns1.contains(name)){
					logger.info(ERROR+"第二个库中索引["+indexName+"]多字段："+name);
//					System.out.println(ERROR+"第二个库中索引["+indexName+"]多字段："+name);
				}
				else{
					if(!columns.contains(name))
						columns.add(name);
				}
			}
			
			for(int i=0;i<columns.size();i++){
				String name = columns.get(i);
				String[] s1 = columnInfos1.get(name);
				String[] s2 = columnInfos2.get(name);			
				for(int k=0;k<info.length;k++){
					if(!s1[k].equals(s2[k])){
						logger.info(ERROR+"索引["+indexName+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//						System.out.println(ERROR+"索引["+indexName+"]字段["+name+"]的属性["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
					}
				}
				
			}
		}
		catch(Exception e){
			throw e;
		}	
	}	
	
	/**
	 * 比较表数据是否一致
	 * @param conn1  数据库连接1
	 * @param conn2 数据库连接2
	 * @param list  待比较的数据定义信息
	 * @throws Exception
	 */
	public static void compareDatas(Connection conn1,Connection conn2,ArrayList<CompareDataInfo> list) throws Exception{
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		try{
			stmt1 = conn1.createStatement();
			stmt2 = conn2.createStatement();
			
			for(int m=0;m<list.size();m++){
				CompareDataInfo cdi = list.get(m);
				String tableName = cdi.getTableName();
				String idColumn = cdi.getIdColumn();
				String tmpId = "C"+System.currentTimeMillis();//用于字段别名
				StringBuffer sb = new StringBuffer(" ("+idColumn+") as "+tmpId);//拼接的查询字段
				for(int j=0;j<cdi.getColumns().size();j++){
					if(sb.length()>0)
						sb.append(",");
					sb.append(" ("+cdi.getColumns().get(j)+") as "+tmpId+"_"+j); 
				}
				String sql = "select "+sb.toString()+" from "+tableName;
				List<String> ids1 = new ArrayList<String>();
				HashMap<String,String[]> values1 = new HashMap<String,String[]>();
				rs1 = stmt1.executeQuery(sql);
				while(rs1.next()){
					String id = rs1.getString(tmpId);
					String[] ss = new String[cdi.getColumns().size()];
					for(int j=0;j<cdi.getColumns().size();j++){
						ss[j] = StringUtil.trim(rs1.getString(tmpId+"_"+j));
					}
					ids1.add(id);
					values1.put(id, ss);
				}
				rs1.close();
				List<String> ids2 = new ArrayList<String>();
				HashMap<String,String[]> values2 = new HashMap<String,String[]>();
				rs2 = stmt2.executeQuery(sql);
				while(rs2.next()){
					String id = rs2.getString(tmpId);
					String[] ss = new String[cdi.getColumns().size()];
					for(int j=0;j<cdi.getColumns().size();j++){
						ss[j] = StringUtil.trim(rs2.getString(tmpId+"_"+j));
					}
					ids2.add(id);
					values2.put(id, ss);
				}
				rs2.close();
				
				if(ids1.size()!=ids2.size()){
					logger.info(ERROR+"表["+tableName+"]数据条数为："+ids1.size()+"/"+ids2.size());
				}
				else
					logger.info("表["+tableName+"]数据条数为："+ids1.size()+"/"+ids2.size());
				
				List<String> ids = new ArrayList<String>();
				
				for(int i=0;i<ids1.size();i++){
					String name = ids1.get(i);
					if(!ids2.contains(name)){
						logger.info(ERROR+"第一个库中表["+tableName+"]多数据："+name);
						System.out.println(ERROR+"第一个库中表["+tableName+"]多数据："+name);
					}
					else{
						if(!ids.contains(name))
							ids.add(name);
					}
				}
				
				for(int i=0;i<ids2.size();i++){
					String name = ids2.get(i);
					if(!ids1.contains(name)){
						logger.info(ERROR+"第二个库中表["+tableName+"]多数据："+name);
						System.out.println(ERROR+"第二个库中表["+tableName+"]多数据："+name);
					}
					else{
						if(!ids.contains(name))
							ids.add(name);
					}
				}
				
				for(int i=0;i<ids.size();i++){
					String id = ids.get(i);
					String[] s1 = values1.get(id);
					String[] s2 = values2.get(id);			
					for(int k=0;k<cdi.getColumns().size();k++){
						if(!s1[k].equals(s2[k])){
							logger.info(ERROR+"表["+tableName+"]中记录["+id+"]的字段["+cdi.getColumns().get(k)+"]值不同："+s1[k]+"/"+s2[k]);
							System.out.println(ERROR+"表["+tableName+"]中记录["+id+"]的字段["+cdi.getColumns().get(k)+"]值不同："+s1[k]+"/"+s2[k]);
						}
					}
				}
				
			}
			
		}
		catch(Exception e){
			throw e;
		}
		finally{
			CompareDatabaseUtil.close(null,stmt1,rs1);	
			CompareDatabaseUtil.close(null,stmt2,rs2);			
		}			
	}
	
	
	/**
	 * 关闭连接
	 * @param conn
	 * @param stmt
	 * @param rs
	 */
	public static void close(Connection conn,Statement stmt,ResultSet rs){
		if(rs!=null)
		try{
			rs.close();
			rs = null;
		}catch(Exception e){}

		if(stmt!=null)
		try{
			stmt.close();
			stmt = null;
		}catch(Exception e){}

		if(conn!=null)
		try{
			conn.close();
			conn = null;
		}catch(Exception e){}
	}
	
	/**
	 * 对比触发器
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2 数据库2
	 * @throws Exception
	 */
	public static void compareTriggers(Connection conn1, Connection conn2, String database1, String database2) throws Exception {
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare triggers!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare triggers!");
			}
			

			List<String> triggers1 = new ArrayList<String>();//数据库索引名
			HashMap<String,String> triggerIds1 = new HashMap<String,String>();//数据库索引名，索引ID
			if(type1==DBType.MySQL){
				Util4MySQL.getTriggers(conn1,database1,triggers1,triggerIds1);
			}
			List<String> triggers2 = new ArrayList<String>();
			HashMap<String,String> triggerIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getTriggers(conn2,database2,triggers2,triggerIds2);
			}
			//比较索引数量
			if(triggers1.size()!=triggers2.size()){
				logger.info(ERROR+"触发器数量为："+triggers1.size()+"/"+triggers2.size());
//				System.out.print(ERROR);
			}
			else
				logger.info("触发器数量为："+triggers1.size()+"/"+triggers2.size());
//			System.out.println("触发器数量为："+triggers1.size()+"/"+triggers2.size());
			
			//比较索引差异
			List<String> triggers = new ArrayList<String>();			
			for(int i=0;i<triggers1.size();i++){
				String name = triggers1.get(i);
				if(!triggers2.contains(name)){
					logger.info(ERROR+"第一个库中多触发器："+name);
//					System.out.println(ERROR+"第一个库中多触发器："+name);
				}
				else{
					if(!triggers.contains(name))
						triggers.add(name);
				}
			}
			
			for(int i=0;i<triggers2.size();i++){
				String name = triggers2.get(i);
				if(!triggers1.contains(name)){
					logger.info(ERROR+"第二个库中多触发器："+name);
//					System.out.println(ERROR+"第二个库中多触发器："+name);
				}
				else{
					if(!triggers.contains(name))
						triggers.add(name);
				}
			}
			
			for(int i=0;i<triggers.size();i++){
				String name = triggers.get(i);
				compareOneTrigger(conn1,conn2,database1,database2,name,triggerIds1.get(name),triggerIds2.get(name));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}

	/**
	 * 比较单个触发器的属性
	 * @param conn1  数据库连接1
	 * @param conn2  数据库连接2
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @param triggerName  触发器名称
	 * @param triggerId1  触发器ID1
	 * @param triggerId2  触发器ID2
	 * @throws Exception
	 */
	private static void compareOneTrigger(Connection conn1, Connection conn2, String database1, String database2,
			String triggerName, String triggerId1, String triggerId2) throws Exception {
		try{
			
			/**
			 * 查询mysql某个库中指定的触发器：TRIGGER_NAME name 触发器名称，EVENT_OBJECT_TABLE trigger_table 触发表名
			 * ACTION_STATEMENT trigger_statement 触发的操作，ACTION_ORIENTATION trigger_orientation 触发决策，ACTION_TIMING timing 触发时刻
			 * 字段属性：字段名称、触发表名、触发操作、是否每条记录触发、触发时刻
			 */
			String[] info = new String[]{"名称","触发表名","触发操作","是否每条记录触发","触发时刻"};
			
			
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare triggerColumnInfos!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare triggerColumnInfos!");
			}			

			List<String> columns1 = new ArrayList<String>();//触发器字段名
			HashMap<String,String[]> columnInfos1 = new HashMap<String,String[]>();//触发器属性
			if(type1==DBType.MySQL){
				Util4MySQL.getOneTriggerColumnInfos(conn1,database1,triggerName,triggerId1,columns1,columnInfos1);
			}
			List<String> columns2 = new ArrayList<String>();//触发器字段名
			HashMap<String,String[]> columnInfos2 = new HashMap<String,String[]>();//触发器属性
			if(type2==DBType.MySQL){
				Util4MySQL.getOneTriggerColumnInfos(conn2,database2,triggerName,triggerId2,columns2,columnInfos2);
			}
			//触发器字段名应一致
			if(columns1.equals(columns2)){
				//对比指定触发器属性是否一致
				for(int i=0;i<columns1.size();i++){
					String name = columns1.get(i);
					String[] s1 = columnInfos1.get(name);
					String[] s2 = columnInfos2.get(name);			
					for(int k=0;k<info.length;k++){
						if(StringUtil.isBlank(s1[k])){
							if(StringUtil.isNotBlank(s2[k])){
								logger.info(ERROR+"触发器["+triggerName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//								System.out.println(ERROR+"触发器["+triggerName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
							}
						}else{
							if(!s1[k].equals(s2[k])){
								logger.info(ERROR+"触发器["+triggerName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//								System.out.println(ERROR+"触发器["+triggerName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
							}
						}
					}
				}
			}
		}
		catch(Exception e){
			throw e;
		}	
	}
	
	
	/**
	 * 对比存储过程
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2 数据库2
	 * @throws Exception
	 */
	public static void compareProcedures(Connection conn1, Connection conn2, String database1, String database2) throws Exception {
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare procedures!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare procedures!");
			}
			
			List<String> procedures1 = new ArrayList<String>();//数据库索引名
			HashMap<String,String> procedureIds1 = new HashMap<String,String>();//数据库索引名，索引ID
			if(type1==DBType.MySQL){
				Util4MySQL.getProcedures(conn1,database1,procedures1,procedureIds1);
			}
			List<String> procedures2 = new ArrayList<String>();
			HashMap<String,String> procedureIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getProcedures(conn2,database2,procedures2,procedureIds2);
			}
			//比较索引数量
			if(procedures1.size() != procedures2.size()){
				logger.info(ERROR+"存储过程数量为："+procedures1.size()+"/"+procedures2.size());
			}
			else
				logger.info("存储过程数量为："+procedures1.size()+"/"+procedures2.size());
//			System.out.println("存储过程数量为："+procedures1.size()+"/"+procedures2.size());
			
			//比较索引差异
			List<String> procedures = new ArrayList<String>();			
			for(int i=0;i<procedures1.size();i++){
				String name = procedures1.get(i);
				if(!procedures2.contains(name)){
					logger.info(ERROR+"第一个库中多存储过程："+name);
//					System.out.println(ERROR+"第一个库中多存储过程："+name);
				}
				else{
					if(!procedures.contains(name))
						procedures.add(name);
				}
			}
			
			for(int i=0;i<procedures2.size();i++){
				String name = procedures2.get(i);
				if(!procedures1.contains(name)){
					logger.info(ERROR+"第二个库中多存储过程："+name);
//					System.out.println(ERROR+"第二个库中多存储过程："+name);
				}
				else{
					if(!procedures.contains(name))
						procedures.add(name);
				}
			}
			
			for(int i=0;i<procedures.size();i++){
				String name = procedures.get(i);
				compareOneProcedure(conn1,conn2,database1,database2,name,procedureIds1.get(name),procedureIds2.get(name));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}
	
	/**
	 * 比较单个存储过程的函数
	 * @param conn1  数据库连接1
	 * @param conn2  数据库连接2
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @param procedureName  存储过程名称
	 * @param procedureId1  存储过程ID1
	 * @param procedureId2  存储过程ID2
	 * @throws Exception
	 */
	private static void compareOneProcedure(Connection conn1, Connection conn2, String database1, String database2,
			String procedureName, String procedureId1, String procedureId2) throws Exception {
		try{
			
			/**
			 * 查询mysql某个库中指定的存储过程：SPECIFIC_NAME name ，ROUTINE_DEFINIION sql_str,SQL_MODE sql_mode
			 * 字段属性：字段名称、sql语句,sql模型
			 */
			String[] info = new String[]{"名称","sql语句","sql模型"};
			
			
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare procedureInfos!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare procedureInfos!");
			}			

			List<String> columns1 = new ArrayList<String>();//存储过程字段名
			HashMap<String,String[]> columnInfos1 = new HashMap<String,String[]>();//存储过程属性
			if(type1==DBType.MySQL){
				Util4MySQL.getOneProcedureColumnInfos(conn1,database1,procedureName,procedureId1,columns1,columnInfos1);
			}
			List<String> columns2 = new ArrayList<String>();//存储过程字段名
			HashMap<String,String[]> columnInfos2 = new HashMap<String,String[]>();//存储过程属性
			if(type2==DBType.MySQL){
				Util4MySQL.getOneProcedureColumnInfos(conn2,database2,procedureName,procedureId2,columns2,columnInfos2);
			}
			//存储过程字段名应一致
			if(columns1.equals(columns2)){
				//对比存储过程sql是否一致
				for(int i=0;i<columns1.size();i++){
					String name = columns1.get(i);
					String[] s1 = columnInfos1.get(name);
					String[] s2 = columnInfos2.get(name);			
					for(int k=0;k<info.length;k++){
						if(StringUtil.isBlank(s1[k])){
							if(StringUtil.isNotBlank(s2[k])){
								logger.info(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//								System.out.println(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
							}
						}else{
							//当k=1时，比较存储过程的sql是否相同
							if(1 == k){
								String sql1 = StringUtil.removeBlankSpaceOrLine(s1[k]);
								String sql2 = StringUtil.removeBlankSpaceOrLine(s2[k]);
								if(!sql1.equals(sql2)){
									logger.info(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//									System.out.println(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
								}
							}else{
								
								if(!s1[k].equals(s2[k])){
									logger.info(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
//									System.out.println(ERROR+"存储过程["+procedureName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e){
			throw e;
		}	
	}
	
	/**
	 * 对比存储函数
	 * @param conn1   第一个数据库连接
	 * @param conn2  第二个数据库连接
	 * @param database1  数据库1
	 * @param database2 数据库2
	 * @throws Exception
	 */
	public static void compareFunctions(Connection conn1, Connection conn2, String database1, String database2) throws Exception {
		try{
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare functions!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare functions!");
			}
			
			List<String> functions1 = new ArrayList<String>();//数据库索引名
			HashMap<String,String> functionIds1 = new HashMap<String,String>();//数据库索引名，索引ID
			if(type1==DBType.MySQL){
				Util4MySQL.getFunctions(conn1,database1,functions1,functionIds1);
			}
			List<String> functions2 = new ArrayList<String>();
			HashMap<String,String> functionIds2 = new HashMap<String,String>();
			if(type2==DBType.MySQL){
				Util4MySQL.getFunctions(conn2,database2,functions2,functionIds2);
			}
			//比较索引数量
			if(functions1.size() != functions2.size()){
				logger.info(ERROR+"存储函数数量为："+functions1.size()+"/"+functions2.size());
//				System.out.print(ERROR);
			}
			else
				logger.info("存储函数数量为："+functions1.size()+"/"+functions2.size());
//			System.out.println("存储函数数量为："+functions1.size()+"/"+functions2.size());
			
			//比较索引差异
			List<String> functions = new ArrayList<String>();			
			for(int i=0;i<functions1.size();i++){
				String name = functions1.get(i);
				if(!functions2.contains(name)){
					logger.info(ERROR+"第一个库中多存储函数："+name);
//					System.out.println(ERROR+"第一个库中多存储函数："+name);
				}
				else{
					if(!functions.contains(name))
						functions.add(name);
				}
			}
			
			for(int i=0;i<functions2.size();i++){
				String name = functions2.get(i);
				if(!functions1.contains(name)){
					logger.info(ERROR+"第二个库中多存储函数："+name);
//					System.out.println(ERROR+"第二个库中多存储函数："+name);
				}
				else{
					if(!functions.contains(name))
						functions.add(name);
				}
			}
			
			for(int i=0;i<functions.size();i++){
				String name = functions.get(i);
				compareOneFunction(conn1,conn2,database1,database2,name,functionIds1.get(name),functionIds2.get(name));
			}
		}
		catch(Exception e){
			throw e;
		}		
	}
	
	/**
	 * 比较单个存储函数
	 * @param conn1  数据库连接1
	 * @param conn2  数据库连接2
	 * @param database1  数据库1
	 * @param database2  数据库2
	 * @param functionName  存储函数名称
	 * @param functionId1  存储函数ID1
	 * @param functionId2  存储函数ID2
	 * @throws Exception
	 */
	private static void compareOneFunction(Connection conn1, Connection conn2, String database1, String database2,
			String functionName, String functionId1, String functionId2) throws Exception {
		try{
			
			/**
			 * 查询mysql某个库中指定的存储过程：SPECIFIC_NAME name ，ROUTINE_DEFINIION sql,SQL_MODE sql_mode,DATA_TYPE data_type:
			 * data_type 数据类型，data_length 数据长度，data_precision 小数点位数，data_scale 数字总位数（整数+小数）
			 * 字段属性：字段名称、sql语句,sql模型,返回数据类型,数据长度，小数点位数，数字总位数
			 */
			String[] info = new String[]{"名称","sql语句","sql模型","返回数据类型","数据长度","数点位数","数字总位数"};
			
			
			DBType type1 = getDBTypeFromConn(conn1);
			DBType type2 = getDBTypeFromConn(conn2);
			if(type1==DBType.Unknown){
				throw new Exception("Database 1 type is unknown, can not compare tables!");
			}
			if(type2==DBType.Unknown){
				throw new Exception("Database 2 type is unknown, can not compare tables!");
			}			
			
			List<String> columns1 = new ArrayList<String>();//存储过程字段名
			HashMap<String,String[]> columnInfos1 = new HashMap<String,String[]>();//存储过程属性
			if(type1==DBType.MySQL)
				Util4MySQL.getOneFunctionColumnInfos(conn1,database1,type1,functionName,functionId1,columns1,columnInfos1);
			
			List<String> columns2 = new ArrayList<String>();//存储过程字段名
			HashMap<String,String[]> columnInfos2 = new HashMap<String,String[]>();//存储过程属性
			if(type2==DBType.MySQL)
				Util4MySQL.getOneFunctionColumnInfos(conn2,database2,type1,functionName,functionId2,columns2,columnInfos2);
			
			//存储函数字段名应一致
			if(columns1.equals(columns2)){
				//对比存储函数sql是否一致
				for(int i=0;i<columns1.size();i++){
					String name = columns1.get(i);
					String[] s1 = columnInfos1.get(name);
					String[] s2 = columnInfos2.get(name);			
					for(int k=0;k<info.length;k++){
						if(StringUtil.isBlank(s1[k])){
							if(StringUtil.isNotBlank(s2[k])){
								logger.info(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
								System.out.println(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
							}
						}else{
							//当k=1时，比较存储过程的sql是否相同
							if(1 == k){
								String sql1 = StringUtil.removeBlankSpaceOrLine(s1[k]);
								String sql2 = StringUtil.removeBlankSpaceOrLine(s2[k]);
								if(!sql1.equals(sql2)){
									logger.info(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
									System.out.println(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
								}
							}else{
								if(!s1[k].equals(s2[k])){
									logger.info(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
									System.out.println(ERROR+"存储函数["+functionName+"]字段["+name+"]的值["+info[k]+"]不同："+s1[k]+"/"+s2[k]);
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e){
			throw e;
		}	
	}
	

}
