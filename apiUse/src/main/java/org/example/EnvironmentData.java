package org.example;

public class EnvironmentData {
    public EnvironmentData(){}
    private int EnvoId;
    private int BaseID;
    private int CO2;
    private int PM25;
    private int PM10;
    private double Temperature;
    private double Humidity;
    private int TVOC;
    private int CH2O;
    private int Smoke;
    private String InPutTime;

    @Override
    public String toString() {
        return "EnvironmentData{" +
                "EnvoId=" + EnvoId +
                ", BaseID=" + BaseID +
                ", CO2=" + CO2 +
                ", PM25=" + PM25 +
                ", PM10=" + PM10 +
                ", Temperature=" + Temperature +
                ", Humidity=" + Humidity +
                ", TVOC=" + TVOC +
                ", CH2O=" + CH2O +
                ", Smoke=" + Smoke +
                ", InPutTime='" + InPutTime + '\'' +
                '}';
    }

    public EnvironmentData(int envoId, int baseID, int CO2, int PM25, int PM10, double temperature, double humidity, int TVOC, int CH2O, int smoke, String inPutTime) {
        EnvoId = envoId;
        BaseID = baseID;
        this.CO2 = CO2;
        this.PM25 = PM25;
        this.PM10 = PM10;
        Temperature = temperature;
        Humidity = humidity;
        this.TVOC = TVOC;
        this.CH2O = CH2O;
        Smoke = smoke;
        InPutTime = inPutTime;
    }

    public int getEnvoId() {
        return EnvoId;
    }

    public void setEnvoId(int envoId) {
        EnvoId = envoId;
    }

    public int getBaseID() {
        return BaseID;
    }

    public void setBaseID(int baseID) {
        BaseID = baseID;
    }

    public int getCO2() {
        return CO2;
    }

    public void setCO2(int CO2) {
        this.CO2 = CO2;
    }

    public int getPM25() {
        return PM25;
    }

    public void setPM25(int PM25) {
        this.PM25 = PM25;
    }

    public int getPM10() {
        return PM10;
    }

    public void setPM10(int PM10) {
        this.PM10 = PM10;
    }

    public double getTemperature() {
        return Temperature;
    }

    public void setTemperature(double temperature) {
        Temperature = temperature;
    }

    public double getHumidity() {
        return Humidity;
    }

    public void setHumidity(double humidity) {
        Humidity = humidity;
    }

    public int getTVOC() {
        return TVOC;
    }

    public void setTVOC(int TVOC) {
        this.TVOC = TVOC;
    }

    public int getCH2O() {
        return CH2O;
    }

    public void setCH2O(int CH2O) {
        this.CH2O = CH2O;
    }

    public int getSmoke() {
        return Smoke;
    }

    public void setSmoke(int smoke) {
        Smoke = smoke;
    }

    public String getInPutTime() {
        return InPutTime;
    }

    public void setInPutTime(String inPutTime) {
        InPutTime = inPutTime;
    }
}
