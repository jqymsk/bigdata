package jiq.util.copy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyUtil {
	private static final Logger LOG = LoggerFactory.getLogger(PropertyUtil.class);

	private static Properties props = new Properties();
	private static PropertyUtil instance = null;

	private PropertyUtil() {
		try {
			String userDir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

			File proFile = new File(userDir + "system.properties");

			if (proFile.exists()) {
				props.load(new FileInputStream(userDir + "system.properties"));
			}
		} catch (IOException e) {
			LOG.info("The IOException occured {}.", e);
		}
	}

	/**
	 * 单例模式
	 * 
	 * @return
	 */
	public synchronized static PropertyUtil getInstance() {
		if (null == instance) {
			instance = new PropertyUtil();
		}

		return instance;
	}

	/**
	 * 获取参数值
	 * 
	 * @param properites的key值
	 * @param defValue默认值
	 * @return
	 */
	public String getValue(String key, String defValue) {
		String rtValue = null;

		if (null == key) {
			LOG.error("key is null");
		} else {
			rtValue = getPropertiesValue(key);
		}

		if (null == rtValue) {
			LOG.warn("PropertyUtil.getValues return null, key is " + key);
			rtValue = defValue;
		}

		return rtValue;
	}

	/**
	 * 通过key值获取value
	 * 
	 * @param key
	 * @return
	 */
	private String getPropertiesValue(String key) {
		String rtValue = props.getProperty(key);

		return rtValue;
	}

	/**
	 * 通过proFile、key获取value
	 * 
	 * @author jqymsk
	 * @param properites去后缀的文件名称
	 * @param properites的key值
	 * @return
	 */
	public static String getProp(String proFile, String key) {
		String value = "";

		try {
			value = ResourceBundle.getBundle(proFile).getString(key);
		} catch (MissingResourceException e) {
			LOG.error("PropertyUtil.getProp return null, key is " + key);
		}

		return value;
	}

}
