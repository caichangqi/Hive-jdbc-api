package com.glsx.biz.stc.container.hive;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveUtils
{
    private HiveUtils() {}

    static
    {
        try
        {
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection()
    {
        try
        {
            Connection conn =DriverManager.getConnection("jdbc:hive://master.hadoop:10000/myhive", "", "");
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
