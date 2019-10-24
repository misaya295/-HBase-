package com.cwk.weibo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;

public class Message {
    private String uid;
    private long timestamp;
    private String content;

    @Override
    public String toString() {

        Date date= new Date(timestamp);
        SimpleDateFormat simpleFormatter= new
                SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "Message{" +
                "用户id='" + uid + '\n' +
                ", 发布时间=" + simpleFormatter.format(date) +'\n'+
                ", 微博内容='" + content +
                '}';
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Message() {
    }

    public Message(String uid, long timestamp, String content) {
        this.uid = uid;
        this.timestamp = timestamp;
        this.content = content;
    }
}
