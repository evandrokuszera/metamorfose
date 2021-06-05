/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.gui;

import javax.swing.JOptionPane;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class Simcaq {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("teste")
                .config("spark.master", "local")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "5g")               
                .getOrCreate();
        
        
//        JOptionPane.showConfirmDialog(null ,"pressione Ok para finalizar Spark");
//        
//        spark.close();
        
        
//        String csv = "D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_100k.CSV";
//        Dataset<Row> dataset = spark.read().csv(csv);
//        
//        dataset.printSchema();
//        
//        dataset.createOrReplaceTempView("matriculas");        
//        //Dataset<Row> namesDF = spark.sql("SELECT COUNT(_c11) FROM matriculas WHERE _c11 = 'M'");
//        Dataset<Row> namesDF = spark.sql("SELECT _c0, _c11 FROM matriculas WHERE _c11 = 'M'");
//        //namesDF.show();
//        String s = namesDF.showString(10, 0);
//        System.out.println(s);
        

        Dataset<Row> matriculasCO = spark.read().csv("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_CO.CSV");
        Dataset<Row> matriculasSUL = spark.read().csv("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k.CSV");
        
        matriculasCO.createOrReplaceTempView("matCO");
        matriculasSUL.createOrReplaceTempView("matSUL");
        
//        Dataset<Row> result = spark.sql("select COUNT (matCO._c0) from matCO");
//        result.show();
//        
//        System.out.println("Total de matriculas: " + matriculasCO.union(matriculasSUL).count());
    
        long inicio = System.currentTimeMillis();
        
        //System.out.println("Total de matriculas (M): " + matriculasCO.filter("_c11 = 'M'").union(matriculasSUL.filter("_c11 = 'M'")).count());
        
        Dataset<Row> result = spark.sql("select COUNT (matCO._c0) from matCO where _c11 = 'M' UNION select COUNT (matSUL._c0) from matSUL where _c11 = 'M'");
        result.show();
        
        long fim = System.currentTimeMillis();
        System.out.println("Tempo de execução: " + (fim-inicio)/1000);
        
        
        
        
        
//        matriculasCO.printSchema();
//        matriculasSUL.printSchema();

        
    }
}
