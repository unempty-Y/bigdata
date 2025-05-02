package org.example;


import java.math.BigInteger;

public class OrderPositiveAgger {
    public OrderPositiveAgger() {
    }

    private BigInteger id;
    private String orderprice;
    private String orderdetailcount;

    public OrderPositiveAgger(BigInteger id, String orderprice, String orderdetailcount) {
        this.id = id;
        this.orderprice = orderprice;
        this.orderdetailcount = orderdetailcount;
    }

    // Getters and Setters
    public BigInteger getId() {
        return id;
    }

    public void setId(BigInteger id) {
        this.id = id;
    }

    public String getOrderprice() {
        return orderprice;
    }

    public void setOrderprice(String orderprice) {
        this.orderprice = orderprice;
    }

    public String getOrderdetailcount() {
        return orderdetailcount;
    }

    public void setOrderdetailcount(String orderdetailcount) {
        this.orderdetailcount = orderdetailcount;
    }
}