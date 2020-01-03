package cn.ly.model;

import java.io.Serializable;

/**
 * @Author luoyu
 * @create 2019/11/8 16:31
 */
public class ColumModel implements Serializable {
    private String dataBase;
    private String tableName;
    private String content;

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
