package org.example;

public  class RateTable {
    String bridgeid;
    String ratename;
    Double ratevalue;
    String time_stamp;

    @Override
    public String toString() {
        return "RateTable{" +
                "bridgeid='" + bridgeid + '\'' +
                ", ratename='" + ratename + '\'' +
                ", ratevalue=" + ratevalue +
                ", time_stamp='" + time_stamp + '\'' +
                '}';
    }

    public RateTable(String bridgeid, String ratename, Double ratevalue, String time_stamp) {
        this.bridgeid = bridgeid;
        this.ratename = ratename;
        this.ratevalue = ratevalue;
        this.time_stamp = time_stamp;
    }

    public String getBridgeid() {
        return bridgeid;
    }

    public String getRatename() {
        return ratename;
    }

    public Double getRatevalue() {
        return ratevalue;
    }

    public String getTime_stamp() {
        return time_stamp;
    }
}
