package org.example.gs7.shili;

public class UserAction {
    public String userId;
    public String action;
    public long timestamp;

    public UserAction() {}

    public UserAction(String userId, String action, long timestamp) {
        this.userId = userId;
        this.action = action;
        this.timestamp = timestamp;
    }

    // 确保实现toString方法以便打印结果
    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
