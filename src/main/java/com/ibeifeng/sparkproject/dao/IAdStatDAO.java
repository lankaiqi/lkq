package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 */
public interface IAdStatDAO {

    void updateBatch(List<AdStat> adStatss);
}
