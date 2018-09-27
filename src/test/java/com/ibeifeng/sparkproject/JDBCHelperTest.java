package com.ibeifeng.sparkproject;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * jdbc辅助组件测试类
 */
public class JDBCHelperTest {
    public static void main(String[] args) throws Exception {
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
//        jdbcHelper.executeUpdate("insert into aa(a1,a2,a3) values (?,?,?)",
//                                    new Object[]{"王瑟","2","3432"});
       /* //测试查询
        final Map<String,Object> testUser = new HashMap<String, Object>();
        //
        jdbcHelper.executeQuery(
                "select a1,a2,a3 from aa where a2=?",
                new Object[]{2},
                new JDBCHelper.QueryCallback(){
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()){
                            //匿名内部类的使用,有一个重要点
                            //如果要访问外部类中的一些成员,比如方法内的局部变量
                            //那么,必须将局部变量,声明为static,才可以访问
                            //否则访问不了
                            testUser.put("name",rs.getString(1));
                            testUser.put("age",rs.getString(2));
                            testUser.put("tel",rs.getString(3));
                        }
                    }
                }
        );
        System.out.println("============================"+testUser.get("name")+"==========================");*/

    //批量执行sql
        String sql = "insert into aa(a1,a2,a3) values (?,?,?)";
        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"但是",23,188});
        paramsList.add(new Object[]{"规范",3,168});
        int[] ints = jdbcHelper.executBatch(sql, paramsList);

    }
}
