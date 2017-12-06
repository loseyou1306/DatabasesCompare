package com.database.compare.tools.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 跟MySQL相关的Util
 * @author Cat_L
 *
 */
public class Util4MySQL {
	public static Logger logger = LogUtil.getLog(Util4MySQL.class);

	/**
	 * 获取一个表的字段信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param dbType  数据库类型
	 * @param tableName  表名 
	 * @param tableId  表ID
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、字段类型、字段长度、是否非空
	 * @throws Exception
	 */
//	public static void getOneTableColumnInfos(Connection conn,String database,DBType dbType,String tableName,String tableId,List<String> columns,HashMap<String,String[]> columnInfos) throws Exception{
//		Statement stmt = null;
//		ResultSet rs = null;
//		try{
//			stmt = conn.createStatement();
//			rs = stmt.executeQuery("select COLUMN_NAME as name,concat(ifnull(CHARACTER_MAXIMUM_LENGTH,'-') , ' or ' ,ifnull(NUMERIC_PRECISION,'0') , '.' , ifnull(NUMERIC_SCALE,'0')) as len, data_type as atttype,IS_NULLABLE as attnotnull from information_schema.COLUMNS where lower(TABLE_SCHEMA) in ('"+database.toLowerCase()+"') and lower(TABLE_name) in ('"+tableName.toLowerCase()+"')");
//			while(rs.next()){
//				String name = rs.getString(1);
//				String type = rs.getString(3);
//				type = ColumnTypeUtil.getGenericType(dbType,type);
//				String len = rs.getString(2);
//				String notnull = rs.getString(4);
//				columns.add(name);
//				columnInfos.put(name, new String[]{name,type,len,notnull});
//			}
//		}
//		catch(Exception e){
//			throw new Exception("query ColumnInfos from table or view '"+tableName+"' in database '"+database+"' error: "+e.getMessage());
//		}
//		finally{
//			CompareDatabaseUtil.close(null,stmt,rs);			
//		}
//	}
	public static void getOneTableColumnInfos(Connection conn,String database,DBType dbType,String tableName,String tableId,List<String> columns,HashMap<String,String[]> columnInfos) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			//查询mysql某个库中某个表中的所有字段：column_name	列名，dafault_value 默认值，nullable 是否为空，
			//data_type 数据类型，data_length 数据长度，data_precision 小数点位数，data_scale 数字总位数（整数+小数）
			String sql = "SELECT lower(column_name) column_name,column_default dafault_value,substring(is_nullable, 1, 1) nullable,column_type data_type,character_maximum_length data_length,numeric_precision data_precision,numeric_scale data_scale "
					+ "FROM information_schema.COLUMNS "
					+ "WHERE table_schema = '"+database.toLowerCase()+"' AND table_name = '"+tableName.toLowerCase()+"'";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String column_name = rs.getString(1);
				String dafault_value = rs.getString(2);
				String nullable = rs.getString(3);
				String data_type = rs.getString(4);
				data_type = ColumnTypeUtil.getGenericType(dbType,data_type);
				String data_length = rs.getString(5);
				String data_precision = rs.getString(6);
				String data_scale = rs.getString(7);
				columns.add(column_name);
				columnInfos.put(column_name, new String[]{column_name,dafault_value,nullable,data_type,data_length,data_precision,data_scale});
			}
		}
		catch(Exception e){
			throw new Exception("query ColumnInfos from table or view '"+tableName+"' in database '"+database+"' error: "+e.getMessage());
			}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	/**
	 * 获取一个索引的字段信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param indexName  索引名 （表名.索引名）
	 * @param indexId  索引ID（索引名）
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、是否唯一、字段排序
	 * @throws Exception
	 */
	public static void getOneIndexColumnInfos(Connection conn,String database,String indexName,String indexId,List<String> columns,HashMap<String,String[]> columnInfos) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select COLUMN_name as attname,non_unique as attunique,seq_in_index as attindex from information_schema.statistics where lower(TABLE_name) in ('"+indexName.substring(0,indexName.indexOf(".")).toLowerCase()+"')  and lower(index_name) in ('"+indexId.toLowerCase()+"') and lower(table_schema) in ('"+database.toLowerCase()+"')  order by attindex");
			while(rs.next()){
				String name = rs.getString(1);
				String uniue = rs.getString(2);
				String attindex = rs.getString(3);
				columns.add(name);
				columnInfos.put(name, new String[]{name,uniue,attindex});
			}
		}
		catch(Exception e){
			throw new Exception("query Index ColumnInfos from index '"+indexName+"' in database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	/**
	 * 获取数据库包含的索引
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param indexs  返回结果1
	 * @param indexIds  返回结果2
	 * @throws Exception
	 */
	public static void getIndexs(Connection conn,String database,List<String> indexs,HashMap<String,String> indexIds) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select DISTINCT table_name,index_name from information_schema.statistics where lower(table_schema) in ('"+database.toLowerCase()+"') order by table_name,index_name");
			while(rs.next()){
				String name = rs.getString(1)+"."+rs.getString(2);
				String id = rs.getString(2);
				indexs.add(name);
				indexIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query index from database '"+database+"' error:"+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	
	
	
	/**
	 * 获取数据库包含的表
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param tables  返回结果1
	 * @param tableIds  返回结果2
	 * @throws Exception
	 */
	public static void getTables(Connection conn,String database,List<String> tables,HashMap<String,String> tableIds) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select TABLE_NAME a1,TABLE_NAME a2 from information_schema.tables where table_type in ('BASE TABLE') and lower(TABLE_SCHEMA) in ('"+database.toLowerCase()+"') order by a1");
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				tables.add(name);
				tableIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query table name from database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	/**
	 * 获取数据库包含的表
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param tables  返回结果1
	 * @param tableIds  返回结果2
	 * @throws Exception
	 */
	public static void getViews(Connection conn,String database,List<String> tables,HashMap<String,String> tableIds) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select TABLE_NAME a1,TABLE_NAME a2 from information_schema.tables where table_type in ('VIEW') and lower(TABLE_SCHEMA) in ('"+database.toLowerCase()+"') order by a1");
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				tables.add(name);
				tableIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query view from database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}

	/**
	 * 获取数据库包含的索引
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param tables  返回结果1
	 * @param tableIds  返回结果2
	 * @throws Exception
	 */
	public static void getTriggers(Connection conn, String database, List<String> tables,
			HashMap<String, String> tableIds) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			//查询指定数据库中的所有触发器
			rs = stmt.executeQuery("SELECT TRIGGER_NAME a1,TRIGGER_NAME a2 FROM information_schema.`TRIGGERS` WHERE lower(TRIGGER_SCHEMA) in ('"+database.toLowerCase()+"') order by a1");
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				tables.add(name);
				tableIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query trigger from database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
		
	}

	/**
	 * 获取一个触发器的属性信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param triggerName  触发器名称
	 * @param triggerId  触发器ID
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、是否唯一、字段排序
	 * @throws Exception
	 */
	public static void getOneTriggerColumnInfos(Connection conn, String database, String triggerName,
			String triggerId, List<String> columns, HashMap<String, String[]> columnInfos) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			/**
			 * 查询mysql某个库中指定的触发器：TRIGGER_NAME name 触发器名称，EVENT_OBJECT_TABLE trigger_table 触发表名
			 * ACTION_STATEMENT trigger_statement 触发的操作，ACTION_ORIENTATION trigger_orientation 触发决策，ACTION_TIMING timing 触发时刻
			 * 字段属性：字段名称、触发表名、触发操作、是否每条记录触发、触发时刻
			 */
			stmt = conn.createStatement();
			String sql = "SELECT TRIGGER_NAME name,EVENT_OBJECT_TABLE trigger_table,ACTION_STATEMENT trigger_statement,ACTION_ORIENTATION trigger_orientation,ACTION_TIMING timing "
					+ "FROM `information_schema`.`TRIGGERS` "
					+ "WHERE lower(TRIGGER_SCHEMA) IN ('"+database.toLowerCase()+"') "
							+ "AND lower(TRIGGER_NAME) IN ('"+triggerName.toLowerCase()+"')";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String name = rs.getString(1);
				String trigger_table = rs.getString(2);
				String trigger_statement = rs.getString(3);
				String trigger_orientation = rs.getString(4);
				String timing = rs.getString(5);
				columns.add(name);
				columnInfos.put(name, new String[]{name,trigger_table,trigger_statement,trigger_orientation,timing});
			}
		}
		catch(Exception e){
			throw new Exception("query trigger ColumnInfos from trigger '"+triggerName+"' in database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	/**
	 * 获取数据库包含的存储过程
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param tables  返回结果1
	 * @param tableIds  返回结果2
	 * @throws Exception
	 */
	public static void getProcedures(Connection conn, String database, List<String> tables,
			HashMap<String, String> tableIds) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			//查询指定数据库中的所有存储过程
			String sql = "SELECT name a1,name a2 FROM mysql.proc WHERE db in ('"+database.toLowerCase()+"') AND `type` = 'PROCEDURE' order by a1";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				tables.add(name);
				tableIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query procedure from database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
		
	}

	/**
	 * 获取一个存储过程的属性信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param procedureName  存储过程名称
	 * @param procedureId  存储过程ID
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、是否唯一、字段排序
	 * @throws Exception
	 */
	public static void getOneProcedureColumnInfos(Connection conn, String database, String procedureName,
			String procedureId, List<String> columns, HashMap<String, String[]> columnInfos) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			/**
			 * 查询mysql某个库中指定的存储过程：SPECIFIC_NAME name ，ROUTINE_DEFINIION sql,SQL_MODE sql_mode
			 * 字段属性：字段名称、sql语句,sql模型
			 */
			stmt = conn.createStatement();
			
			String sql = "SELECT SPECIFIC_NAME name ,ROUTINE_DEFINITION sql_str,SQL_MODE sql_mode "
					+ "FROM information_schema.Routines "
					+ "WHERE ROUTINE_SCHEMA IN ('"+database.toLowerCase()+"') "
					+ "AND ROUTINE_NAME IN ('"+procedureName.toLowerCase()+"') "
					+ "AND ROUTINE_TYPE IN ('PROCEDURE')";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String name = rs.getString(1);
				String sql_str = rs.getString(2);
				String sql_mode = rs.getString(3);
				columns.add(name);
				columnInfos.put(name, new String[]{name,sql_str,sql_mode});
			}
		}
		catch(Exception e){
			throw new Exception("query procedureInfos from procedure '"+procedureName+"' in database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
	/**
	 * 获取数据库包含的存储函数
	 * @param conn   数据库连接
	 * @param database   数据库名称
	 * @param tables  返回结果1
	 * @param tableIds  返回结果2
	 * @throws Exception
	 */
	public static void getFunctions(Connection conn, String database, List<String> tables,
			HashMap<String, String> tableIds) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			//查询指定数据库中的所有存储过程
			String sql = "SELECT name a1,name a2 FROM mysql.proc WHERE db in ('"+database.toLowerCase()+"') AND `type` = 'FUNCTION' order by a1";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				tables.add(name);
				tableIds.put(name, id);
			}
		}
		catch(Exception e){
			throw new Exception("query function from database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
		
	}

	/**
	 * 获取一个存储函数的属性信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param dbType  数据库类型
	 * @param functionName  存储函数名称
	 * @param functionId  存储函数ID
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、是否唯一、字段排序
	 * @throws Exception
	 */
	public static void getOneFunctionColumnInfos(Connection conn, String database, DBType dbType, String functionName,
			String functionId, List<String> columns, HashMap<String, String[]> columnInfos) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		try{
			/**
			 * 查询mysql某个库中指定的存储过程：SPECIFIC_NAME name ，ROUTINE_DEFINIION sql,SQL_MODE sql_mode,DATA_TYPE data_type:
			 * data_type 数据类型，data_length 数据长度，data_precision 小数点位数，data_scale 数字总位数（整数+小数）
			 * 字段属性：字段名称、sql语句,sql模型,返回数据类型,数据长度，小数点位数，数字总位数
			 */
			stmt = conn.createStatement();
			
			String sql = "SELECT SPECIFIC_NAME name ,ROUTINE_DEFINITION sql_str,SQL_MODE sql_mode,DATA_TYPE data_type,character_maximum_length data_length,numeric_precision data_precision,numeric_scale data_scale "
					+ "FROM information_schema.Routines "
					+ "WHERE ROUTINE_SCHEMA IN ('"+database.toLowerCase()+"') "
					+ "AND ROUTINE_NAME IN ('"+functionName.toLowerCase()+"') "
					+ "AND ROUTINE_TYPE IN ('FUNCTION')";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String name = rs.getString(1);
				String sql_str = rs.getString(2);
				String sql_mode = rs.getString(3);
				String data_type = rs.getString(4);
				data_type = ColumnTypeUtil.getGenericType(dbType,data_type);
				String data_length = rs.getString(5);
				String data_precision = rs.getString(6);
				String data_scale = rs.getString(7);
				columns.add(name);
				columnInfos.put(name, new String[]{name,sql_str,sql_mode,data_type,data_length,data_precision,data_scale});
			}
		}
		catch(Exception e){
			throw new Exception("query functionInfos from function '"+functionName+"' in database '"+database+"' error: "+e.getMessage());
		}
		finally{
			CompareDatabaseUtil.close(null,stmt,rs);			
		}
	}
	
}
