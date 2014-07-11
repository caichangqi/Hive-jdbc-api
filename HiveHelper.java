package com.glsx.biz.stc.container.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.springframework.stereotype.Service;

@Service("hiveHelper")
public class HiveHelper {  
	  
    static HiveHelper _HBaseHelper = null;  
    Connection _Connection = null;  
    Statement _Statement = null;  
    PreparedStatement _PreparedStatement = null;  
    String _getExceptionInfoString = "";  
    String _getDBConnectionString = "";  
  
    private HiveHelper() {}          
  
    public static HiveHelper getInstanceBaseHelper() {  
        if (_HBaseHelper == null)  
            synchronized (HiveHelper.class) {  
                if(_HBaseHelper==null)  
                    _HBaseHelper = new HiveHelper();  
            }             
        return _HBaseHelper;  
    }  
  
    public Object ExcuteNonQuery(String sql) {  
        int n = 0;  
        try {  
            _Connection =(Connection) HiveUtils.getConnection();
            _Statement = _Connection.createStatement();  
            n = _Statement.executeUpdate(sql);  
            _Connection.commit();  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return n;  
    }  
  
    public Object ExcuteNonQuery(String sql, Object[] args) {  
        int n = 0;  
        try {  
            _Connection =(Connection) HiveUtils.getConnection();  
            _PreparedStatement = _Connection.prepareStatement(sql);  
            for (int i = 0; i < args.length; i++)  
                _PreparedStatement.setObject(i + 1, args[i]);  
            n = _PreparedStatement.executeUpdate();  
            _Connection.commit();  
        } catch (SQLException e) {  
            Dispose();
            e.printStackTrace(); 
        }  
        return n;  
    }  
  
    public ResultSet ExecuteQuery(String sql) {  
        ResultSet rsResultSet = null;  
        try {  
            _Connection =(Connection) HiveUtils.getConnection();
            _Statement = _Connection.createStatement();  
            rsResultSet = _Statement.executeQuery(sql);  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return rsResultSet;  
    }  
  
    public ResultSet ExceteQuery(String sql, Object[] args) {  
        ResultSet rsResultSet = null;  
        try {  
            _Connection =(Connection) HiveUtils.getConnection();
            _PreparedStatement = _Connection.prepareStatement(sql);  
            for (int i = 0; i < args.length; i++)  
                _PreparedStatement.setObject(i + 1, args[i]);  
            rsResultSet = _PreparedStatement.executeQuery();  
  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return rsResultSet;  
    }  
  
    public void Dispose() {  
        try {  
            if (_Connection != null)  
                _Connection.close();  
            if (_Statement != null)  
                _Statement.close();  
        } catch (Exception e) {  
            // TODO: handle exception  
            _getExceptionInfoString = e.getMessage();  
        }  
    }  
} 

