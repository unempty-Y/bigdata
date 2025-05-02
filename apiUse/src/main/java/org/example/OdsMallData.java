package org.example;

import com.google.gson.annotations.SerializedName;

public class OdsMallData {
    public OdsMallData() {
    }

    @SerializedName("database")
    private String database;
    @SerializedName("table")
    private String table;
    @SerializedName("type")
    private String type;
    @SerializedName("ts")
    private int ts;
    @SerializedName("xid")
    private int xid;
    @SerializedName("commit")
    private boolean commit;
    @SerializedName("data")
    private Object data;

    public OdsMallData(String database, String table, String type, int ts, int xid, boolean commit, Object data) {
        this.database = database;
        this.table = table;
        this.type = type;
        this.ts = ts;
        this.xid = xid;
        this.commit = commit;
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getType() {
        return type;
    }

    public int getTs() {
        return ts;
    }

    public int getXid() {
        return xid;
    }

    public boolean isCommit() {
        return commit;
    }

    public Object getData() {
        return data;
    }

    // Optional: toString for debugging
    @Override
    public String toString() {
        return "OdsMallData{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                ", xid=" + xid +
                ", commit=" + commit +
                ", data=" + data +
                '}';
    }

}