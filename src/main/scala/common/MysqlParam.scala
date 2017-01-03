package common


case class MysqlParam(host:String,port:String,user:String,password:String)

object AdvJson extends Enumeration {
  type AdvJson = Value
  val ad_id, uid, ad_type, ad_source, advertiser_name, ad_product_name, industry, product_id, app_ver, position_id, position_type,
  target_size, daily_count_limit, news_configid, biz_configid, ad_configid, sort_weight,
  pay_model, channel, scenario, unit_price, log_type = Value.id
}




