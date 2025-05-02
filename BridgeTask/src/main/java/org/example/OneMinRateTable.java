package org.example;

public  class OneMinRateTable {
    String bridgeid;
    String abutmentno;
    Double overloadrate;
    Double voidrate;
    String time_stamp;

    public OneMinRateTable(String bridgeid, String abutmentno, Double overloadrate, Double voidrate, String time_stamp) {
        this.bridgeid = bridgeid;
        this.abutmentno = abutmentno;
        this.overloadrate = overloadrate;
        this.voidrate = voidrate;
        this.time_stamp = time_stamp;
    }

    @Override
    public String toString() {
        return "OneMinRateTable{" +
                "bridgeid='" + bridgeid + '\'' +
                ", abutmentno='" + abutmentno + '\'' +
                ", overloadrate=" + overloadrate +
                ", voidrate=" + voidrate +
                ", time_stamp='" + time_stamp + '\'' +
                '}';
    }

    public String getBridgeid() {
        return bridgeid;
    }

    public String getAbutmentno() {
        return abutmentno;
    }

    public Double getOverloadrate() {
        return overloadrate;
    }

    public Double getVoidrate() {
        return voidrate;
    }

    public String getTime_stamp() {
        return time_stamp;
    }
}
