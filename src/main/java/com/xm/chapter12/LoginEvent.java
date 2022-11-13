package com.xm.chapter12;

/**
 * @author 夏明
 * @version 1.0
 */
public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
