package com.tosit.project.session

/**
  * Created by Wanghan on 2017/3/13.
  * Copyright © Wanghan SCU. All Rights Reserved
  */


/**
  * 构造方法
  *
  * @param category_id
  * @param click_times
  * @param order_times
  * @param pay_times
  */
class KeyPair(category_id:String,click_times: Int, order_times: Int, pay_times: Int) extends Ordered[KeyPair] with Serializable {
    val categoryId = category_id
    val click = click_times
    val order = order_times
    val pay = pay_times

    override def compare(that: KeyPair): Int = {
        if (this.click == that.click) {
            if (this.order == that.order) {
                return this.click - that.click
            }
            return this.order - that.order
        }
        return this.click - that.click
    }
}
