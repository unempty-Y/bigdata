package org.example;

public class ForceTableSP {
    private int forceid; // 主键
    private String abutmentno;
    private String bridgeid;
    private String piereno;
    private float forcevalue;
    private String time_stamp; // 假设我们将其作为字符串处理，以保持与数据库中的一致性
    private float temperature;
    private float humidness;
    private byte risk; // 使用 byte 类型对应于 TINYINT

    public ForceTableSP(int forceid, String abutmentno, String bridgeid, String piereno, float forcevalue, String time_stamp, float temperature, float humidness, byte risk) {
        this.forceid = forceid;
        this.abutmentno = abutmentno;
        this.bridgeid = bridgeid;
        this.piereno = piereno;
        this.forcevalue = forcevalue;
        this.time_stamp = time_stamp;
        this.temperature = temperature;
        this.humidness = humidness;
        this.risk = risk;
    }

    public int getForceid() {
        return forceid;
    }

    public void setForceid(int forceid) {
        this.forceid = forceid;
    }

    public String getAbutmentno() {
        return abutmentno;
    }

    public void setAbutmentno(String abutmentno) {
        this.abutmentno = abutmentno;
    }

    public String getBridgeid() {
        return bridgeid;
    }

    public void setBridgeid(String bridgeid) {
        this.bridgeid = bridgeid;
    }

    public String getPiereno() {
        return piereno;
    }

    public void setPiereno(String piereno) {
        this.piereno = piereno;
    }

    public float getForcevalue() {
        return forcevalue;
    }

    public void setForcevalue(float forcevalue) {
        this.forcevalue = forcevalue;
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public float getHumidness() {
        return humidness;
    }

    public void setHumidness(float humidness) {
        this.humidness = humidness;
    }

    public byte getRisk() {
        return risk;
    }

    public void setRisk(byte risk) {
        this.risk = risk;
    }
}
