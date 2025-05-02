package org.example

case class OrderPositiveAgger(
                             id:BigInt,
                             orderprice:String,
                             orderdetailcount:String
                             )
case class Produce5minAgg(
                           machine_id: String,
                           total_produce: Int)

case class Ods_mall_data(
                          database: String,
                          table: String,
                          $type: String,
                          ts: Int,
                          xid: Int,
                          commit: Boolean,
                          data: Any
                        )

case class Order_detail(
                         order_detail_id: Int,
                         order_sn: String,
                         product_id: Int,
                         product_name: String,
                         product_cnt: Int,
                         product_price: Double,
                         average_cost: Double,
                         weight: Double,
                         fee_money: Double,
                         w_id: Int,
                         create_time: String,
                         modified_time: String
                       )

case class Order_master(
                         order_id: Int,
                         order_sn: String,
                         customer_id: Int,
                         shipping_user: String,
                         province: String,
                         city: String,
                         address: String,
                         order_source: Int,
                         payment_method: Int,
                         order_money: Double,
                         district_money: Double,
                         shipping_money: Double,
                         payment_money: Double,
                         shipping_comp_name: String,
                         shipping_sn: String,
                         create_time: String,
                         shipping_time: String,
                         pay_time: String,
                         receive_time: String,
                         order_status: String,
                         order_point: Int,
                         invoice_title: String,
                         modified_time: String
                       )

case class Product_browse(
                           log_id: Int,
                           order_sn: String,
                           product_id: Int,
                           customer_id: String,
                           gen_order: Int,
                           modified_time: Double
                         )

