package org.example.gs4;

public class totalProduce {
    private String id;
    private int total;

    public totalProduce() {
    }

    @Override
    public String toString() {
        return "totalProduce{" +
                "id='" + id + '\'' +
                ", sum=" + total +
                '}';
    }

    public totalProduce(String id, int total) {
        this.id = id;
        this.total = total;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getTotal() {
        return total;
    }
}
