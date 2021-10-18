package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;

/**
 * @author sunzhipeng
 * @create 2021-04-17 19:57
 * 查询Phoenix的工具类
 */
public class PhoenixUtil {
    public static Connection conn = null;

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<JSONObject> objectList = queryList("select * from  base_trademark", JSONObject.class);
        System.out.println(objectList);
    }


    public static void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (conn == null) {
            queryInit();
        }
        List<T> resultList = new ArrayList();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }
}

