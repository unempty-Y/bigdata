package org.example;

public class Produce5MinAgg {
    public Produce5MinAgg(){}
    private String machine_id;
    private int total_produce;

    public Produce5MinAgg(String machine_id, int total_produce) {
        this.machine_id = machine_id;
        this.total_produce = total_produce;
    }

    // Getters and Setters
    public String getMachine_id() {
        return machine_id;
    }

    public void setMachine_id(String machine_id) {
        this.machine_id = machine_id;
    }

    public int getTotal_produce() {
        return total_produce;
    }

    public void setTotal_produce(int total_produce) {
        this.total_produce = total_produce;
    }
}
