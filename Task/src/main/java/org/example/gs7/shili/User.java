package org.example.gs7.shili;

public class User {
    public String userId;
    public String name;
public Long date;
    public User() {}

    public User(String userId, String name, Long date) {
        this.userId = userId;
        this.name = name;
        this.date = date;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", name='" + name + '\'' +
                ", date=" + date +
                '}';
    }
}
