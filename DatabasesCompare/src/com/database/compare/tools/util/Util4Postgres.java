package com.database.compare.tools.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;


/**
 * 跟Postgres相关的Util
 * @author Cat_L
 *
 */
public class Util4Postgres {

	/**
	 * 获取一个表的字段信息
	 * @param conn  数据库连接
	 * @param database  数据库名
	 * @param dbType 数据库类型
	 * @param tableName  表名 
	 * @param tableId  oid
	 * @param columns   返回值1，字段名称
	 * @param columnInfos  返回值2，字段属性按照这个顺序放在数组中：字段名称、字段类型、字段长度、是否非空
	 * @throws Exception
	 */
	public static void getOneTableColumnInfos(Connection conn,String database,DBType dbType, String tableName,String tableId,List<String> columns,HashMap<String,String[]> columnInfos) throws Exception{
		Statement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.createStatement();
			// TODO
			/**
			 * 查询mysql某个库中某个表中的所有字段：column_name	列名，dafault_value 默认值，nullable 是否为空，
			 * data_type 数据类型，data_length 数据长度，data_precision 小数点位数，data_scale 数字总位数（整数+小数）
			 * 字段属性：字段名称、字段类型、字段长度、是否非空
			 * attname,atthasdef,attnotnull,atttypid,atttypmod,
			 */
			//String[] info = new String[]{"名称","字段类型","字段长度","是否非空"};
			String[] info = new String[]{"名称","默认值","是否非空","数据类型","数据长度","小数点位数","数字总位数"};
			String sql = "select b.attname,b.atttypid,(select typname from pg_type c where c.oid = b.atttypid) as atttypname,b.attlen,b.atttypmod,b.attnotnull,b.attisdropped "
					+ "from pg_attribute b "
					+ "where b.attisdropped = 'f' and b.attrelid="+tableId;
			
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				String name = rs.getString(1);
				String type = rs.getString(3);
				type = ColumnTypeUtil.getGenericType(dbType,type);
				String len = rs.getString(2);
				String notnull = rs.getString(4);
				columns.add(name);
				columnInfos.put(name, new String[]{name,type,len,notnull});
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
			//查询普通表 ，pg_class中relkind列属性：r = 普通表，i = 索引，S = 序列，v = 视图， c = 复合类型，s = 特殊，t = TOAST表
			String sql = "select a.oid,a.relname from pg_class a "
					+ "where a.relowner in (select b.oid from pg_authid b where b.rolname='"+database.toLowerCase()+"') "
							+ "and a.relkind='r' and a.relnamespace in (select oid from pg_namespace where nspname='public') "
							+ "order by a.relname";
			rs = stmt.executeQuery(sql);
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
	 * 获取数据库包含的视图
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
}
