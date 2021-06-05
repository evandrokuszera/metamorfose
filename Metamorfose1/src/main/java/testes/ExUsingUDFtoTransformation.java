/*
 * Exemplo de uso de UDFs no Spark
 * Usa o conjunto de dados do projet Simcaq.
 * Exemplo de transformação do campo TP_SEXO (F ou M) para (1 ou 0)
 * Mostra três formas de registrar a UDF e chamá-la sobre Datasets.
 * 
 */
package testes;

import metamorfose.main.Framework;
import metamorfose.transformations.udf.UserDefinedFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Evandro
 */
public class ExUsingUDFtoTransformation {

    public static void main(String[] args) {        
        
        //usando_UDF_declarada_na_mesma_classe();
        //usando_UDF_declarada_em_classe_interna_via_registerJava();
        //usando_UDF_declarada_em_classe_externa_que_tem_varias_outras_udfs();
        chamando_UDF_em_consulta_SQL();
        
    }   
    

    // EXEMPLO 1
    public static void usando_UDF_declarada_na_mesma_classe(){
        
        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV");

        UDF1 sexoUDF = new UDF1<String, Integer>() {
            @Override
            public Integer call(String arg0) throws Exception {
                int result = 0;
                switch (arg0) {
                    case "M":
                        result = 1;
                        break;
                    case "F":
                        result = 0;
                        break;
                }
                return result;
            }
        };                
        
        // Registrando objeto UDF declarado mesma classe que será utilizada
        Framework.getSparkSession().udf().register("sexoUDF", sexoUDF, DataTypes.IntegerType);
        
        // Aplicando a UDF sexoUDF sobre a columna TP_SEXO. Caso TP_SEXO=M, então 1. Caso TP_SEXO=F, então 0.
        Dataset<Row> transformedRecords = records.withColumn("TP_SEXO", functions.callUDF("sexoUDF", records.col("TP_SEXO")));
        
        transformedRecords.show(false);
        
    }
    
    // EXEMPLO 2
    public static void usando_UDF_declarada_em_classe_interna_via_registerJava(){
        
        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV");             
        
        // Registrando UDF que reside em outra classe interna via 'registerJava'.
        Framework.getSparkSession().udf().registerJava("sexoUDF", SexoUDF.class.getName(), DataTypes.IntegerType);
                
        // Aplicando a UDF sexoUDF sobre a columna TP_SEXO. Caso TP_SEXO=M, então 1. Caso TP_SEXO=F, então 0.
        Dataset<Row> transformedRecords = records.withColumn("TP_SEXO", functions.callUDF("sexoUDF", records.col("TP_SEXO")));
        
        transformedRecords.show(false);
        
    }
    
    
    // EXEMPLO 3
    public static void usando_UDF_declarada_em_classe_externa_que_tem_varias_outras_udfs(){
        
        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV");             
        
        // Regisrando UDF que reside em outra classe que possui várias outras UDF. Cada UDF é uma instância de objeto que implementa a interface UDF1 ou UDFx
        Framework.getSparkSession().udf().register("sexoUDF", UserDefinedFunctions.sexoTransformation, DataTypes.IntegerType);
        
        // Aplicando a UDF sexoUDF sobre a columna TP_SEXO. Caso TP_SEXO=M, então 1. Caso TP_SEXO=F, então 0.
        Dataset<Row> transformedRecords = records.withColumn("TP_SEXO", functions.callUDF("sexoUDF", records.col("TP_SEXO")));
        
        transformedRecords.show(false);
        
    }
    
    // EXEMPLO 4
    public static void chamando_UDF_em_consulta_SQL(){
        
        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV");             
        
        // Regisrando UDF que reside em outra classe que possui várias outras UDF. Cada UDF é uma instância de objeto que implementa a interface UDF1 ou UDFx
        Framework.getSparkSession().udf().register("sexoUDF", UserDefinedFunctions.sexoTransformation, DataTypes.IntegerType);
        
        records.createOrReplaceTempView("mat");
        Dataset<Row> transformedRecords = Framework.getSparkSession().sql("select sexoUDF(TP_SEXO) as sexo from mat");
        
        transformedRecords.show(false);
        
    }      
          
    
    // CLASSE INTERNA USADA PELO MÉTODO 'registerJava'
    public static class SexoUDF implements UDF1<String, Integer> {

        @Override
        public Integer call(String arg0) {
            int result = 0;
            switch (arg0) {
                case "M":
                    result = 1;
                    break;
                case "F":
                    result = 0;
                    break;
            }

            return result;
        }
    }

}
