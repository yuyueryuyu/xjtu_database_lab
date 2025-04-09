package cn.edu.xjtu.dblab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class App {
  public static void main(String[] args) {
        getConnect();
  }
  public static Connection getConnect() {
        String driver = "org.opengauss.Driver";
        String sourceURL = "jdbc:opengauss://10.0.2.15:26000/db_tpcc?user=joe&password=_Ggeliyu7104";
        Properties info = new Properties();
        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (Exception var9) {
            var9.printStackTrace();
            return null;
        }
        try {
            conn = DriverManager.getConnection(sourceURL);
            System.out.println("连接成功！");
            return conn;
        } catch (Exception var8) {
            var8.printStackTrace();
            return null;
        }
    }
}
