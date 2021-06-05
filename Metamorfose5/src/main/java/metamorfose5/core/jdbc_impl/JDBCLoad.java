/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.jdbc_impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class JDBCLoad {
    private JDBCMetamorfose parent;
    private java.sql.Connection connection;
    private String driverName = "org.postgresql.Driver";
    private String url_base = "jdbc:postgresql://";
    private String user;
    private String password;
    private String server;
    private String database;
    
    public JDBCLoad(JDBCMetamorfose parent) {
        this.parent = parent;
    }
    
    public void fromJDBC(String entityName, String server, String database, String user, String password, String table){
        this.user = user;
        this.password = password;
        this.server = server;
        this.database = database;
    
        HashMap<String,JSONArray> entitiesPool =  new HashMap<>();
        Connection connection = openConnection();
        
        try {
            String sql = "select * from " + table;
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            JSONArray jsonArray = new JSONArray();    
            // carrega em memória os dados do registro (entitiesPool)...
            while (rs.next()){
                JSONObject obj = new JSONObject();
                // Transformando o registro em objeto JSON
                for (int i=1; i<=rs.getMetaData().getColumnCount(); i++){
                    // Inserindo campo-valor no obj JSON
                    obj.put(rs.getMetaData().getColumnName(i), rs.getString(i));
                }
                //jsonArray.put(obj);
                if (obj.keySet().size() > 0) { jsonArray.put(obj); }
            }
            // Adiciona o array de objs JSON no entitiesPool
            parent.createEntity(entityName, jsonArray);
            connection.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }
    
    
    public void loadDriver(){
        try{
            Class.forName(driverName);
            System.out.println(this.getClass().getName()+".loadDriver(): ok.");
        } catch(Exception e){
            System.out.println(this.getClass().getName()+".loadDriver(): ERROR - " + e);
        }
    }
    
    public java.sql.Connection openConnection(){ 
        String url = url_base + server +"/"+ database;
        
        // tentando reaproveitar conexão aberta.
        try{
            if (this.connection != null){
                if (!this.connection.isClosed()){
                    System.out.println(this.getClass().getName()+".openConnection(): connection reopen.");
                    return this.connection;
                }
            }
        } catch (SQLException e){
            
        }
        
        // tentando estabelecer conexão com banco.
        try {
            this.connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            System.out.println(this.getClass().getName()+".openConnection(): ERROR - " + ex);
        }
        System.out.println(this.getClass().getName()+".openConnection(): opened.");
        return this.connection;
    }
}

