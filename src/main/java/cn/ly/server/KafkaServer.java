package cn.ly.server;

import cn.ly.utils.ReadPropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author luoyu
 * @create 2019/11/8 17:12
 */
public class KafkaServer {
    private static KafkaServer sInstance;
    private KafkaProducer<String, String> mProducer;

    private KafkaServer() {
        try {
            // 读取配置文件
            Properties properties = ReadPropertiesUtil.loadProperties("/config.properties");
            mProducer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            throw new RuntimeException("读取配置文件失败， e： " + e.getMessage());
        }
    }

    /**
     * 创建单例
     *
     * @return
     */
    public static KafkaServer getInstance() {
        if (sInstance == null) {
            synchronized (KafkaServer.class) {
                if (sInstance == null) {
                    sInstance = new KafkaServer();
                }
            }
        }
        return sInstance;
    }


    /**
     * 发送消息
     *
     * @param topic
     * @param sendKey
     * @param data
     */
    public void sendMsg(String topic, String sendKey, String data) {
        mProducer.send(new ProducerRecord<String, String>(topic, sendKey, data));
    }
}
