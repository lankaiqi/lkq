package textSpark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;

/**
 * Created by Administrator on 2018/9/22.
 */
public class SparkJabcSQL {
    public static void main(String[] args) throws Exception{
        String sql ="select category from sales where product=?";
        Connection conn =null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://lkq:10001/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice","","");
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,"a");
            rs = pstmt.executeQuery();
            while (rs.next()){
                int anInt = rs.getInt(1);
                System.out.println("============================"+anInt+"==========================");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
