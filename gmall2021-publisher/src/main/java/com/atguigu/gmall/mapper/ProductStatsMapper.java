package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.ProductStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-05-09 21:11
 */
public interface ProductStatsMapper {
    @Select("select sum(order_count) from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    public BigDecimal  getGMV(int date);

    //统计某天不同SPU商品交易额排名
    @Select("select spu_id,spu_name,sum(order_amount) order_amount," +
            "sum(order_ct) order_ct from product_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name " +
            "having order_amount>0 order by order_amount desc limit #{limit} ")
    public List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);

    @Select("select category3_id,category3_name,sum(order_amount) order_amount \" +\n" +
            "        \"from product_stats_2021 \" +\n" +
            "        \"where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name \" +\n" +
            "        \"having order_amount>0  order by  order_amount desc limit #{limit}")
    public List<ProductStats> getProductStatsGroupByCategory3(@Param("date") int date,@Param("limit") int limit);

    //统计某天不同品牌商品交易额排名
    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
            "from product_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name " +
            "having order_amount>0  order by  order_amount  desc limit #{limit} ")
    public List<ProductStats> getProductStatsByTrademark(@Param("date")int date,  @Param("limit") int limit);

}
