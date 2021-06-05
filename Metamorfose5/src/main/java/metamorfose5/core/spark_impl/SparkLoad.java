/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.spark_impl;

import java.util.Properties;
import static metamorfose5.core.spark_impl.SparkMetamorfose.getSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class SparkLoad {
    
    private static SparkSession spark;
    private SparkMetamorfose parent;

    public SparkLoad(SparkSession sparkSession, SparkMetamorfose parent) {
        this.spark = sparkSession;
        this.parent = parent;
    }
    
     public void fromJDBC(String entityName, String database, String user, String password, String table) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "123456");
        connectionProperties.put("driver", "org.postgresql.Driver");
        
        if (user != null && user.length()>0) connectionProperties.put("user", user);
        if (password != null && password.length()>0) connectionProperties.put("password", password);

        Dataset<Row> jdbcDataset1 = spark.read().jdbc("jdbc:postgresql:" + database, "public." + table, connectionProperties);
        this.parent.createEntity(entityName, jdbcDataset1);        
    }

    // Recupera os dados do CSV e retorna um Dataset<Row>
    public void fromCSV(String entityName, String csv_path) {
        Dataset<Row> datasetCSV = getSparkSession().read().option("header", "true").csv(csv_path);
        this.parent.createEntity(entityName, datasetCSV);        
    }    
    
    // Recupera os dados do CSV e retorna um Dataset<Row>
    public void fromJSON(String entityName, String json_path) {
        Dataset<Row> datasetJSON = getSparkSession().read().json(json_path);
        this.parent.createEntity(entityName, datasetJSON);        
    }
}
