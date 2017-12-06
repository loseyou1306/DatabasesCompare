package com.database.compare.tools.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 读取指定路径的配置文件 单利
 * 
 * @author Cat_L
 * @version 1.0
 */

public class PropertiesUtil {
	public static Logger logger = LogUtil.getLog(PropertiesUtil.class);
	/**
	 * 读取到配置信息后，缓存起来
	 */
	private Map<String, String> ctxPropertiesMap;

	/**
	 * 定义单例
	 */
	private static final PropertiesUtil INSTANCE = new PropertiesUtil();

	public static PropertiesUtil getInstance() {
		return INSTANCE;
	}


	/**
	 * 读取指定路径的文件
	 * 
	 * @param configFilePath
	 *            路径
	 * @throws Exception 抛出异常
	 */
	public void propertyConfigLoader(String configFilePath) throws Exception {
		// 首先加载配置文件是否有问题,要么抛错,要么返回true/false

		if (StringUtil.isBlank(configFilePath)) {
			return;
		}
		Properties config = null;
		if (StringUtil.isNotBlank(configFilePath.trim())) {
			InputStreamReader configFileStream = null;
			try {
				//根据路径读取文件或资源文件
				File file = new File(configFilePath);
				if (file.exists()) {
					configFileStream = new InputStreamReader(new FileInputStream(file),"UTF-8");
				} else {
					configFileStream = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(configFilePath),"UTF-8");
				}
				config = new Properties();
				// 文件编码
				config.load(configFileStream);
			} catch (IOException e) {
				throw new Exception("load file error: "+e.getMessage());
			}finally {
				if (configFileStream != null) {  
		               try {  
		            	   configFileStream.close(); // 关闭流  
		               } catch (IOException e) { 
		            	   throw new Exception("inputStream close IOException: " + e.getMessage());  
		               }  
		           }  
			}
		}
		if(null != config){
			ctxPropertiesMap = new HashMap<String, String>();
			for (Object key : config.keySet()) {
				String keyStr = key.toString();
				String value = config.getProperty(keyStr);
				ctxPropertiesMap.put(keyStr, value);
			}
		}
	}

	/**
	 * 读取配置文件的值
	 * 
	 * @param key
	 *            配置文件的key
	 * @param defaultValue
	 *            默认值
	 * @return 返回配置文件值
	 */
	public String getConfig(String key, String defaultValue) {
		String value = ctxPropertiesMap.get(key);
		if (StringUtil.isBlank(value)) {
			return defaultValue;
		}
		return value;
	}
	/**
	 * Java桌面程序中，可以通过(new File("")).getAbsolutePath()获取项目根目录(非Tomcat下)。
	 * @return
	 */
	public static String getPath(){
		File file=new File("");
	 	String abspath=file.getAbsolutePath();
	 	return abspath;
	} 
}
