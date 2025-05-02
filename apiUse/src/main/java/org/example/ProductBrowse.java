package org.example;

public class ProductBrowse {
    private String log_id;
    private String product_id;
    private int customer_id;
    private int gen_order;
    private String order_sn;
    private String modified_time;

    public ProductBrowse() {
    }

    public ProductBrowse(String log_id, String product_id, int customer_id, int gen_order, String order_sn, String modified_time) {
        this.log_id = log_id;
        this.product_id = product_id;
        this.customer_id = customer_id;
        this.gen_order = gen_order;
        this.order_sn = order_sn;
        this.modified_time = modified_time;
    }


    @Override
    public String toString() {
        return "ProductBrowse{" +
                "log_id=" + log_id +
                ", product_id='" + product_id + '\'' +
                ", customer_id=" + customer_id +
                ", gen_order=" + gen_order +
                ", order_sn='" + order_sn + '\'' +
                ", modified_time='" + modified_time + '\'' +
                '}';
    }

    public String getLog_id() {
        return log_id;
    }

    public void setLog_id(String log_id) {
        this.log_id = log_id;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public int getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(int customer_id) {
        this.customer_id = customer_id;
    }

    public int getGen_order() {
        return gen_order;
    }

    public void setGen_order(int gen_order) {
        this.gen_order = gen_order;
    }

    public String getOrder_sn() {
        return order_sn;
    }

    public void setOrder_sn(String order_sn) {
        this.order_sn = order_sn;
    }

    public String getModified_time() {
        return modified_time;
    }

    public void setModified_time(String modified_time) {
        this.modified_time = modified_time;
    }
}
