package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdBlacklist;

import java.util.List;

/**
 * 广告黑名单DAO接口
 */
public interface IAdBlacklistDao {

    /**
     * 批量插入广告黑名单DAO
     * @param adBlacklistLists
     */
    void insertBatch(List<AdBlacklist> adBlacklistLists);

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    List<AdBlacklist> findAll();
}
