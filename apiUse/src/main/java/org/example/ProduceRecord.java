package org.example;

public class ProduceRecord {
    public ProduceRecord(){}
    private int produceRecordID;
    private int produceMachineID;
    private String produceCodeNumber;
    private String produceStartWaitTime;
    private String produceCodeStartTime;
    private String produceCodeEndTime;
    private int produceCodeCycleTime;
    private String produceEndTime;
    private int produceTotalOut;
    private int produceInspect;

    @Override
    public String toString() {
        return "ProduceRecord{" +
                "produceRecordID=" + produceRecordID +
                ", produceMachineID=" + produceMachineID +
                ", produceCodeNumber='" + produceCodeNumber + '\'' +
                ", produceStartWaitTime='" + produceStartWaitTime + '\'' +
                ", produceCodeStartTime='" + produceCodeStartTime + '\'' +
                ", produceCodeEndTime='" + produceCodeEndTime + '\'' +
                ", produceCodeCycleTime=" + produceCodeCycleTime +
                ", produceEndTime='" + produceEndTime + '\'' +
                ", produceTotalOut=" + produceTotalOut +
                ", produceInspect=" + produceInspect +
                '}';
    }

    public ProduceRecord(int produceRecordID, int produceMachineID, String produceCodeNumber, String produceStartWaitTime, String produceCodeStartTime, String produceCodeEndTime, int produceCodeCycleTime, String produceEndTime, int produceTotalOut, int produceInspect) {
        this.produceRecordID = produceRecordID;
        this.produceMachineID = produceMachineID;
        this.produceCodeNumber = produceCodeNumber;
        this.produceStartWaitTime = produceStartWaitTime;
        this.produceCodeStartTime = produceCodeStartTime;
        this.produceCodeEndTime = produceCodeEndTime;
        this.produceCodeCycleTime = produceCodeCycleTime;
        this.produceEndTime = produceEndTime;
        this.produceTotalOut = produceTotalOut;
        this.produceInspect = produceInspect;
    }

    public int getProduceRecordID() {
        return produceRecordID;
    }

    public void setProduceRecordID(int produceRecordID) {
        this.produceRecordID = produceRecordID;
    }

    public int getProduceMachineID() {
        return produceMachineID;
    }

    public void setProduceMachineID(int produceMachineID) {
        this.produceMachineID = produceMachineID;
    }

    public String getProduceCodeNumber() {
        return produceCodeNumber;
    }

    public void setProduceCodeNumber(String produceCodeNumber) {
        this.produceCodeNumber = produceCodeNumber;
    }

    public String getProduceStartWaitTime() {
        return produceStartWaitTime;
    }

    public void setProduceStartWaitTime(String produceStartWaitTime) {
        this.produceStartWaitTime = produceStartWaitTime;
    }

    public String getProduceCodeStartTime() {
        return produceCodeStartTime;
    }

    public void setProduceCodeStartTime(String produceCodeStartTime) {
        this.produceCodeStartTime = produceCodeStartTime;
    }

    public String getProduceCodeEndTime() {
        return produceCodeEndTime;
    }

    public void setProduceCodeEndTime(String produceCodeEndTime) {
        this.produceCodeEndTime = produceCodeEndTime;
    }

    public int getProduceCodeCycleTime() {
        return produceCodeCycleTime;
    }

    public void setProduceCodeCycleTime(int produceCodeCycleTime) {
        this.produceCodeCycleTime = produceCodeCycleTime;
    }

    public String getProduceEndTime() {
        return produceEndTime;
    }

    public void setProduceEndTime(String produceEndTime) {
        this.produceEndTime = produceEndTime;
    }

    public int getProduceTotalOut() {
        return produceTotalOut;
    }

    public void setProduceTotalOut(int produceTotalOut) {
        this.produceTotalOut = produceTotalOut;
    }

    public int getProduceInspect() {
        return produceInspect;
    }

    public void setProduceInspect(int produceInspect) {
        this.produceInspect = produceInspect;
    }
}
