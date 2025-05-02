package org.example;

public class OdsMallLog {
    String table;
    String data;

    public OdsMallLog(String table, String data) {
        this.table = table;
        this.data = data;
    }

    public OdsMallLog() {
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
