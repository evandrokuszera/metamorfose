/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.spark_impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class SparkFilter {
    private static SparkSession spark;
    private SparkMetamorfose parent;

    public SparkFilter(SparkSession sparkSession, SparkMetamorfose parent) {
        this.spark = sparkSession;
        this.parent = parent;
    }
    
    public void sql(String entityName, String statement){
        Dataset<Row> rows = spark.sql(statement);
        this.parent.createEntity(entityName, rows);
    }
}
