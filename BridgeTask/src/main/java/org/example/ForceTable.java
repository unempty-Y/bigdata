package org.example;

public  class ForceTable {
    private String abutmentno;
    private String bridgeid;
    private String pierno;
    private float forcevalue;
    private String time_stamp;

    public ForceTable(String abutmentno, String bridgeid, String pierno, float forcevalue, String time_stamp) {
        this.abutmentno = abutmentno;
        this.bridgeid = bridgeid;
        this.pierno = pierno;
        this.forcevalue = forcevalue;
        this.time_stamp = time_stamp;
    }

    @Override
    public String toString() {
        return "ForceTable{" +
                "abutmentno='" + abutmentno + '\'' +
                ", bridgeid='" + bridgeid + '\'' +
                ", pierno='" + pierno + '\'' +
                ", forcevalue=" + forcevalue +
                ", time_stamp='" + time_stamp + '\'' +
                '}';
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

    public String getPierno() {
        return pierno;
    }

    public void setPierno(String pierno) {
        this.pierno = pierno;
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
}
