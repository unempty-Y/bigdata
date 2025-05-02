package org.example;

public class ChangeRecord {
    public ChangeRecord(){}
    private String changeId;
    private int changeMachineID;
    private int changeMachineRecordID;
    private String changeRecordState;
    private String changeStartTime;
    private String changeEndTime;
    private int changeHandleState;

    @Override
    public String toString() {
        return "ChangeRecord{" +
                "changeId='" + changeId + '\'' +
                ", changeMachineID=" + changeMachineID +
                ", changeMachineRecordID=" + changeMachineRecordID +
                ", changeRecordState='" + changeRecordState + '\'' +
                ", changeStartTime='" + changeStartTime + '\'' +
                ", changeEndTime='" + changeEndTime + '\'' +
                ", changeHandleState=" + changeHandleState +
                '}';
    }

    public String getChangeId() {
        return changeId;
    }

    public void setChangeId(String changeId) {
        this.changeId = changeId;
    }

    public int getChangeMachineID() {
        return changeMachineID;
    }

    public void setChangeMachineID(int changeMachineID) {
        this.changeMachineID = changeMachineID;
    }

    public int getChangeMachineRecordID() {
        return changeMachineRecordID;
    }

    public void setChangeMachineRecordID(int changeMachineRecordID) {
        this.changeMachineRecordID = changeMachineRecordID;
    }

    public String getChangeRecordState() {
        return changeRecordState;
    }

    public void setChangeRecordState(String changeRecordState) {
        this.changeRecordState = changeRecordState;
    }

    public String getChangeStartTime() {
        return changeStartTime;
    }

    public void setChangeStartTime(String changeStartTime) {
        this.changeStartTime = changeStartTime;
    }

    public String getChangeEndTime() {
        return changeEndTime;
    }

    public void setChangeEndTime(String changeEndTime) {
        this.changeEndTime = changeEndTime;
    }

    public int getChangeHandleState() {
        return changeHandleState;
    }

    public void setChangeHandleState(int changeHandleState) {
        this.changeHandleState = changeHandleState;
    }

    public ChangeRecord(String changeId, int changeMachineID, int changeMachineRecordID, String changeRecordState, String changeStartTime, String changeEndTime, int changeHandleState) {
        this.changeId = changeId;
        this.changeMachineID = changeMachineID;
        this.changeMachineRecordID = changeMachineRecordID;
        this.changeRecordState = changeRecordState;
        this.changeStartTime = changeStartTime;
        this.changeEndTime = changeEndTime;
        this.changeHandleState = changeHandleState;
    }
}
