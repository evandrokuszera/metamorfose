/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testes;

import metamorfose.main.Framework;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Evandro
 */
public class ExJOIN_RDB_JSON_CSV {
    
    public static void main(String[] args) {
        
        Dataset<Row> csv = Framework.getRecordsFromCSV("D:\\notaql-dados\\join\\C.csv");
        Dataset<Row> json = Framework.getSparkSession().read().json("D:\\notaql-dados\\join\\J.json");
        Dataset<Row> rdb = Framework.getRecordsFromJDBC("teste", "r");
        
//        csv.show();
//        json.show();
//        rdb.show();
//        
        Dataset<Row> join1 = csv.join( rdb, csv.col("C1").equalTo(rdb.col("r1")), "left_outer" );
        join1.show(false);
        
//        Dataset<Row> join2 = join1.join( json, join1.col("C1").equalTo(json.col("id")) );
//        join2.show();
        
        Dataset<Row> join2 = join1.join( json, join1.col("C1").equalTo(json.col("address.number")) );
        join2.show();
        
        //json.filter(json.col("address.number").equalTo(5)).show();
        
        
        
        
    }
    
}
