package org.example;

public class OrderMaster {
    public OrderMaster() {
    }

    private int order_id;
    private String order_sn;
    private int customer_id;
    private String shipping_user;
    private String province;
    private String city;
    private String address;
    private int order_source;
    private int payment_method;
    private Double order_money;
    private Double district_money;
    private Double shipping_money;
    private Double payment_money;
    private String shipping_comp_name;
    private String shipping_sn;
    private String create_time;
    private String shipping_time;
    private String pay_time;
    private String receive_time;
    private String order_status;
    private int order_point;
    private String invoice_title;
    private String modified_time;

    public int getOrder_id() {
        return order_id;
    }

    public String getOrder_sn() {
        return order_sn;
    }

    public int getCustomer_id() {
        return customer_id;
    }

    public String getShipping_user() {
        return shipping_user;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public String getAddress() {
        return address;
    }

    public int getOrder_source() {
        return order_source;
    }

    public int getPayment_method() {
        return payment_method;
    }

    public Double getOrder_money() {
        return order_money;
    }

    public Double getDistrict_money() {
        return district_money;
    }

    public Double getShipping_money() {
        return shipping_money;
    }

    public Double getPayment_money() {
        return payment_money;
    }

    public String getShipping_comp_name() {
        return shipping_comp_name;
    }

    public String getShipping_sn() {
        return shipping_sn;
    }

    public String getCreate_time() {
        return create_time;
    }

    public String getShipping_time() {
        return shipping_time;
    }

    public String getPay_time() {
        return pay_time;
    }

    public String getReceive_time() {
        return receive_time;
    }

    public String getOrder_status() {
        return order_status;
    }

    public int getOrder_point() {
        return order_point;
    }

    public String getInvoice_title() {
        return invoice_title;
    }

    public String getModified_time() {
        return modified_time;
    }

    public OrderMaster(int order_id, String order_sn, int customer_id, String shipping_user, String province, String city, String address, int order_source, int payment_method, Double order_money, Double district_money, Double shipping_money, Double payment_money, String shipping_comp_name, String shipping_sn, String create_time, String shipping_time, String pay_time, String receive_time, String order_status, int order_point, String invoice_title, String modified_time) {
        this.order_id = order_id;
        this.order_sn = order_sn;
        this.customer_id = customer_id;
        this.shipping_user = shipping_user;
        this.province = province;
        this.city = city;
        this.address = address;
        this.order_source = order_source;
        this.payment_method = payment_method;
        this.order_money = order_money;
        this.district_money = district_money;
        this.shipping_money = shipping_money;
        this.payment_money = payment_money;
        this.shipping_comp_name = shipping_comp_name;
        this.shipping_sn = shipping_sn;
        this.create_time = create_time;
        this.shipping_time = shipping_time;
        this.pay_time = pay_time;
        this.receive_time = receive_time;
        this.order_status = order_status;
        this.order_point = order_point;
        this.invoice_title = invoice_title;
        this.modified_time = modified_time;
    }

    // Optional: toString for debugging
    @Override
    public String toString() {
        return "OrderMaster{" +
                "order_id=" + order_id +
                ", order_sn='" + order_sn + '\'' +
                ", customer_id=" + customer_id +
                ", shipping_user='" + shipping_user + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", address='" + address + '\'' +
                ", order_source=" + order_source +
                ", payment_method=" + payment_method +
                ", order_money=" + order_money +
                ", district_money=" + district_money +
                ", shipping_money=" + shipping_money +
                ", payment_money=" + payment_money +
                ", shipping_comp_name='" + shipping_comp_name + '\'' +
                ", shipping_sn='" + shipping_sn + '\'' +
                ", create_time=" + create_time +
                ", shipping_time=" + shipping_time +
                ", pay_time=" + pay_time +
                ", receive_time=" + receive_time +
                ", order_status='" + order_status + '\'' +
                ", order_point=" + order_point +
                ", invoice_title='" + invoice_title + '\'' +
                ", modified_time='" + modified_time + '\'' +
                '}';
    }
}