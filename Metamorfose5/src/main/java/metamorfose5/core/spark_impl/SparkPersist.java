/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.spark_impl;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class SparkPersist {
    private static SparkSession spark;
    private SparkMetamorfose parent;

    public SparkPersist(SparkSession sparkSession, SparkMetamorfose parent) {
        this.spark = sparkSession;
        this.parent = parent;
    }
    
    // Salva os registros do modelo em RDB.
    public void toJDBC(String entityName, String user, String passwd, String database, String table, boolean appendMode) {

        Dataset<Row> rows = parent.getEntity(entityName).getData();
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", passwd);
        connectionProperties.put("driver", "org.postgresql.Driver");

        long inicio = System.currentTimeMillis();

        if (appendMode) {
            rows.write()
                    .mode(SaveMode.Append)
                    .option("encoding", "utf-8")
                    .jdbc("jdbc:postgresql:" + database + "?characterEncoding=utf-8", "public." + table, connectionProperties);
        } else {
            rows.write()
                    .mode(SaveMode.Overwrite)
                    .option("encoding", "utf-8")
                    .jdbc("jdbc:postgresql:" + database + "?characterEncoding=utf-8", "public." + table, connectionProperties);
        }

//        rows.write()
//                .mode(SaveMode.Overwrite)    
//                .option("encoding", "utf-8")
//                //.jdbc("jdbc:postgresql:pedidos", "public.comments", connectionProperties);
//                .jdbc("jdbc:postgresql:"+database+"?characterEncoding=utf-8", "public."+table, connectionProperties);
        long fim = System.currentTimeMillis();

        System.out.println("Framework.saveDatasetToJDBC: " + (fim - inicio) / 1000 + " segundos.");
    }
    
    public void toCSV(String entityName, String filename) {
        Dataset<Row> rows = parent.getEntity(entityName).getData();
        rows.write().csv(filename);
    }

    public void toJSON(String entityName, String filename) {
        Dataset<Row> rows = parent.getEntity(entityName).getData();
        rows.write().json(filename);
    }
}
