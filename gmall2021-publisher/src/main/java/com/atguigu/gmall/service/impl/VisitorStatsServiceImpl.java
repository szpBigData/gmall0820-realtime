package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.VisitorStats;
import com.atguigu.gmall.mapper.VisitorStatsMapper;
import com.atguigu.gmall.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-05-09 22:46
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> selectVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> selectVisitorStatsByHour(int date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long selectPv(int date) {
        return visitorStatsMapper.selectPv(date);
    }

    @Override
    public Long selectUv(int date) {
        return visitorStatsMapper.selectUv(date);
    }
}
