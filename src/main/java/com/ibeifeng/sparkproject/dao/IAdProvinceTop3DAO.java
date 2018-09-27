package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdProvinceTop3;

import java.util.List;

/**
 * 各省份top3热门广告DAO接口
 */
public interface IAdProvinceTop3DAO {

    void updateBatch(List<AdProvinceTop3> adProvinceTop3);
}
