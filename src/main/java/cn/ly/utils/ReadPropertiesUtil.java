package cn.ly.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author luoyu
 * @create 2019/11/8 14:59
 */
public class ReadPropertiesUtil {

    public static Properties loadProperties(String path) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = ReadPropertiesUtil.class.getResourceAsStream(path);
        properties.load(inputStream);
        return properties;
    }

}
