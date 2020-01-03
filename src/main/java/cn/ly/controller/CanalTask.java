package cn.ly.controller;

import cn.ly.model.ColumModel;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.List;

/**
 * @Author luoyu
 * @create 2019/11/8 15:50
 */
public class CanalTask implements Runnable {
    private String ip, destination, topic;
    private int port;
    private boolean isRunning = true;
    private Logger logger;
    private static final int BATCH_SIZE = 1024;

    public CanalTask(String ip, String destination, int port, String topic) {
        this.ip = ip;
        this.destination = destination;
        this.port = port;
        this.topic = topic;
    }


    public void run() {
//        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,port), destination,"","");
        CanalConnector canalConnector = CanalConnectors.newClusterConnector("bm01:2181", destination, "", "");
        Assert.notNull(canalConnector, "connector is null");
        while (isRunning){
            try {
                canalConnector.connect();
                canalConnector.subscribe();
                while (isRunning) {
                    Message message = canalConnector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (size != 0) {
                        running(message.getEntries());
                    }
                    canalConnector.ack(batchId); // 提交确认
                }
            } catch (Exception e) {
                canalConnector.rollback();
                failError(e);
                isRunning = false;
                logger.error(this.getClass().getSimpleName() + " running task is ", e);
            } finally {
                isRunning = false;
                canalConnector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void failError(Exception e) {
    }

    private void running(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            //mysql的事务开始前 和事务结束后的内容  不要的
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entryType == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            //如果不是以上的事务，那么解析binlog
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                //获取关键字段 哪一个数据库有事务发生  那张表 、 增加  删除  修改
                CanalEntry.EventType eventType = rowChange.getEventType(); //操作的是insert 还是delete 还是update
                logger.info(eventType.name() + "-----------------------------");
                ColumModel model = new ColumModel();
                model.setDataBase(entry.getHeader().getSchemaName());//当前操作的mysql数据库
                model.setTableName(entry.getHeader().getTableName());//当前操作的是哪一张表

                //迭代所有获取到的binlog数据，然后根据当前mysql的INSERT  UPDATE  DELETE操作，进行解析
                JSONObject jsonObject = null;
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    //判断：当前是什么操作
                    if (eventType == CanalEntry.EventType.DELETE) {
                        jsonObject = structModel(entry, rowData.getBeforeColumnsList(), eventType.name());
                    } else if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                        jsonObject = structModel(entry, rowData.getAfterColumnsList(), eventType.name());
                    }
                }
                if (null != jsonObject) {
                    //把数据封装成json输出到kafka中
                    model.setContent(jsonObject.toJSONString());
                    String data = JSONObject.toJSONString(model);
                    logger.info(data);
//                    KafkaServer.getInstance().sendMsg(topic, UUID.randomUUID().toString(), data);
                }
            } catch (Exception e) {
                logger.error("parser msg is  error ", e);
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
        }
    }

    private JSONObject structModel(
            CanalEntry.Entry entry, List<CanalEntry.Column> afterColumnsList, String entryType) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : afterColumnsList) {
            jsonObject.fluentPut(column.getName(), column.getValue());
        }
        System.out.println("============="+ jsonObject.toJSONString());
        jsonObject.fluentPut("executeTime", entry.getHeader().getExecuteTime());
        jsonObject.fluentPut("eventType", entryType);
        return jsonObject;
    }
}
