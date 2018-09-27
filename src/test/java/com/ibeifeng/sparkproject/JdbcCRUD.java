package com.ibeifeng.sparkproject;


import java.sql.*;

/**
 *  jdbc 增删改查
 */
public class JdbcCRUD {

    public static void main (String[] args){
        //insert();
        //update();
       // delete();
       // select();
        preparedStatement();
    }


    /**
     * 测试插入
     */
    private static void insert(){
        //定义数据库连接对象
        //引用jdbc相关的所有接口或者抽象类的时候,必须引用java.sql包下的
        //java.sql包下的,才代表java提供的jdbc接口,是一套规范
        //至于具体的实现,则由数据库驱动来提供,切记不要引用诸如com.mysql.sql包的类
        Connection conn =null;
        Statement stmt = null;
        //定义sql语句执行语句柄,Statement对象
        //Statement对象,其实就是底层会基于connection数据库连接
        //可以让我们方便的针对数据库中的表,执行增删改查的SQL语句
        //比如insert,update,delete和select语句
        try {
             //第一步,加载数据库的驱动,我们都是面向java.sql包下的接口编程
            //要想让jdbc真正操作数据库,必须先加载进来你要操作的数据库的驱动类
            //使用Class.forname()方式来加载数据库驱动类
            //Class.forname()是Java提供的一种基于反射的方式,直接根据类的全限定名(包+类)
            //从类所在地的磁盘文件(.class)中加载类的对应的内容,并创建对应的Class对象
            Class.forName("com.mysql.jdbc.Driver");
            //获取数据库的连接
            //使用DriverManager.getConnection()方法来获取针对数据库的连接
            conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test?userUnicode=true&characterEncoding=utf8", "root", "root");
            stmt= conn.createStatement();
            String sql = "insert into aa(a1,a2,a3) values ('李四','13','32')";
            int i = stmt.executeUpdate(sql);
            System.out.println("==============影响了======"+i+"==============行============");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (stmt != null){
                    stmt.close();
                }
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {  e.printStackTrace();  }
        }
    }
/**
 * 修改
 */
    private static void update(){
        Connection conn =null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test?userUnicode=true&characterEncoding=utf8", "root", "root");
            stmt= conn.createStatement();
            String sql = "update aa set a1='20' ";
            int i = stmt.executeUpdate(sql);
            System.out.println("==============影响了======"+i+"==============行============");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (stmt != null){
                    stmt.close();
                }
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {  e.printStackTrace();  }
        }
    }
    /**
     * 删除
     */
    private static void delete(){
        Connection conn =null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test?userUnicode=true&characterEncoding=utf8", "root", "root");
            stmt= conn.createStatement();
            String sql = "delete from aa where a1='20'";
            int i = stmt.executeUpdate(sql);
            System.out.println("==============影响了======"+i+"==============行============");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (stmt != null){
                    stmt.close();
                }
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {  e.printStackTrace();  }
        }
    }
    /**
     * 查询
     */
    private static void select(){
        Connection conn =null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test?userUnicode=true&characterEncoding=utf8", "root", "root");
            stmt= conn.createStatement();
            String sql = "select * from aa";
            rs = stmt.executeQuery(sql);
            while (rs.next()){
                String id = rs.getString(1);
                String age = rs.getString(2);
                String tel = rs.getString(3);
                System.out.println("id=" + id + ", age=" + age + ", tel=" + tel);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (stmt != null){
                    stmt.close();
                }
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {  e.printStackTrace();  }
        }
    }

    private static void preparedStatement() {
        Connection conn =null;
        PreparedStatement pstmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test?userUnicode=true&characterEncoding=utf8", "root", "root");
            String sql = "insert into aa(a1,a2,a3) values (?,?,?)";
            pstmt= conn.prepareStatement(sql);
            pstmt.setString(1,"张三");
            pstmt.setString(2,"30");
            pstmt.setString(3,"166");
            int i = pstmt.executeUpdate();
            System.out.println("==============影响了======"+i+"==============行============");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (pstmt != null){
                    pstmt.close();
                }
                if (conn != null){
                    conn.close();
                }
            } catch (SQLException e) {  e.printStackTrace();  }
        }
    }
}
