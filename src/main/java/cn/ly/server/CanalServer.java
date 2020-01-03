package cn.ly.server;

import cn.ly.controller.CanalTask;
import cn.ly.utils.ReadPropertiesUtil;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author luoyu
 * @create 2019/11/8 15:10
 */
public class CanalServer {

    public static final String SERVER_IP_KEY = "serverIp";
    public static final String SERVER_PORT_KEY = "serverPort";
    public static final String DESTINATION_KEY = "destination";
    public static final String TOPIC_KEY = "topic";

    public static void main(String[] args) throws IOException {

        Properties properties = ReadPropertiesUtil.loadProperties("/config.properties");
        String ip = properties.getProperty(SERVER_IP_KEY);
        int port = Integer.parseInt(properties.getProperty(SERVER_PORT_KEY));
        String destination = properties.getProperty(DESTINATION_KEY);
        String topic = properties.getProperty(TOPIC_KEY);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        threadPool.execute(new CanalTask(ip,destination,port,topic));
        threadPool.shutdown();
    }
}
