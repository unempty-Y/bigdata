package org.example;

import com.google.gson.annotations.SerializedName;

public class OrderDetail {
    public OrderDetail() {
    }
@SerializedName("order_detail_id")
    private int order_detail_id;
    private String order_sn;
    private int product_id;
    private String product_name;
    private int product_cnt;
    private double product_price;
    private double average_cost;
    private double weight;
    private double fee_money;
    private int w_id;
    private String create_time;
    private String modified_time;

    public OrderDetail(int order_detail_id, String order_sn, int product_id, String product_name, int product_cnt, double product_price, double average_cost, double weight, double fee_money, int w_id, String create_time, String modified_time) {
        this.order_detail_id = order_detail_id;
        this.order_sn = order_sn;
        this.product_id = product_id;
        this.product_name = product_name;
        this.product_cnt = product_cnt;
        this.product_price = product_price;
        this.average_cost = average_cost;
        this.weight = weight;
        this.fee_money = fee_money;
        this.w_id = w_id;
        this.create_time = create_time;
        this.modified_time = modified_time;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "order_detail_id=" + order_detail_id +
                ", order_sn='" + order_sn + '\'' +
                ", product_id=" + product_id +
                ", product_name='" + product_name + '\'' +
                ", product_cnt=" + product_cnt +
                ", product_price=" + product_price +
                ", average_cost=" + average_cost +
                ", weight=" + weight +
                ", fee_money=" + fee_money +
                ", w_id=" + w_id +
                ", create_time='" + create_time + '\'' +
                ", modified_time='" + modified_time + '\'' +
                '}';
    }

    public int getOrder_detail_id() {
        return order_detail_id;
    }

    public String getOrder_sn() {
        return order_sn;
    }

    public int getProduct_id() {
        return product_id;
    }

    public String getProduct_name() {
        return product_name;
    }

    public int getProduct_cnt() {
        return product_cnt;
    }

    public double getProduct_price() {
        return product_price;
    }

    public double getAverage_cost() {
        return average_cost;
    }

    public double getWeight() {
        return weight;
    }

    public double getFee_money() {
        return fee_money;
    }

    public int getW_id() {
        return w_id;
    }

    public String getCreate_time() {
        return create_time;
    }

    public String getModified_time() {
        return modified_time;
    }
}
