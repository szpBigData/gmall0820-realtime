package com.atguigu.gmall.realtime.app.udf;

/**
 * @author sunzhipeng
 * @create 2021-04-17 22:22
 * <p>
 * Author: Felix
 * Desc:自定义UDTF函数实现商品点击次数、订单次数、添加购物次数的统计
 */
/**
 * Author: Felix
 * Desc:自定义UDTF函数实现商品点击次数、订单次数、添加购物次数的统计
 */

import com.atguigu.gmall.realtime.common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<ct BIGINT,source STRING>"))
public class KeywordProductC2RUDTF extends TableFunction<Row> {
    public void eval(Long clickCt, Long cartCt, Long orderCt) {

        if (clickCt > 0L) {
            Row rowClick = new Row(2);
            rowClick.setField(0, clickCt);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }
        if (cartCt > 0L) {
            Row rowCart = new Row(2);
            rowCart.setField(0, cartCt);
            rowCart.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowCart);
        }
        if (orderCt > 0) {
            Row rowOrder = new Row(2);
            rowOrder.setField(0, orderCt);
            rowOrder.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowOrder);
        }

    }
}

