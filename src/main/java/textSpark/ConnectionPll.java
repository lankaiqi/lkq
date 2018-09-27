package textSpark;



import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * Created by Administrator on 2018/9/18.
 */
public class ConnectionPll {
    private static LinkedList<Connection> connection;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取链接
     */
    public synchronized static Connection getConnection() {
        try {
            if (connection == null) {
                connection = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection("jdbc:mysql://lkq:3306/test", "root", "root");
                    ConnectionPll.connection.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection.poll();
    }

    ;public static void returnConnection(Connection conn){
        connection.push(conn);
    }
}
