package org.example.gs10;

public class ChangeStateOtherToRunAgg {
    public ChangeStateOtherToRunAgg() {
    }

    private int change_machine_id;
    private String last_machine_state;
    private int total_change_torun;
    private String in_time;

    public ChangeStateOtherToRunAgg(int change_machine_id, String last_machine_state, int total_change_torun, String in_time) {
        this.change_machine_id = change_machine_id;
        this.last_machine_state = last_machine_state;
        this.total_change_torun = total_change_torun;
        this.in_time = in_time;
    }

    @Override
    public String toString() {
        return "change_state_other_to_run_agg{" +
                "change_machine_id=" + change_machine_id +
                ", last_machine_state='" + last_machine_state + '\'' +
                ", total_change_torun=" + total_change_torun +
                ", in_time='" + in_time + '\'' +
                '}';
    }

    public int getChange_machine_id() {
        return change_machine_id;
    }

    public void setChange_machine_id(int change_machine_id) {
        this.change_machine_id = change_machine_id;
    }

    public String getLast_machine_state() {
        return last_machine_state;
    }

    public void setLast_machine_state(String last_machine_state) {
        this.last_machine_state = last_machine_state;
    }

    public int getTotal_change_torun() {
        return total_change_torun;
    }

    public void setTotal_change_torun(int total_change_torun) {
        this.total_change_torun = total_change_torun;
    }

    public String getIn_time() {
        return in_time;
    }

    public void setIn_time(String in_time) {
        this.in_time = in_time;
    }
}
