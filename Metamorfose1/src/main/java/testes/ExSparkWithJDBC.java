/*
 * Objetivo: Essa classe demonstra duas operação com bancos relacionais e Spark:
 * a) Obter um Dataset do banco relacional.
 * b) Salvar um Dataset no banco relacional.
 * c) Opcionamente, é possível realizar transformações sobre os dados.
 */
package testes;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Evandro
 */
public class ExSparkWithJDBC {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("teste")
                .config("spark.master", "local")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "5g")               
                .getOrCreate();
        
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "123456");
        

        LerDadosRDB(spark, connectionProperties);

        //SalvarDadosFromCSV_to_RDB(spark, connectionProperties);
        
    }
    
    public static void LerDadosRDB(SparkSession spark, Properties connectionProperties){
        Dataset<Row> jdbcDataset1 = spark.read().jdbc("jdbc:postgresql:pedidos", "public.pedidos", connectionProperties);
        jdbcDataset1.show();
    }
    
    
    public static void SalvarDadosFromCSV_to_RDB(SparkSession spark, Properties connectionProperties){
        // Carrega os dados .CSV para Dataset Spark
        Dataset<Row> jdbcDataset2 = spark.read().csv("D:\\notaql-dados\\comments.CSV");
        
        // Teste de transformação... convertendo o campo 'name' para caracteres maiúsculos.
        JavaRDD<Row> dadosTrans = jdbcDataset2.javaRDD().map((record) -> {
            String name = record.getString(0); 
            return RowFactory.create(name.toUpperCase(), record.getString(1)); 
        });
        
        // Definindo os tipos de dados dos campos do .CSV
        StructField field1 = DataTypes.createStructField("name", DataTypes.StringType, false);
        StructField field2 = DataTypes.createStructField("comments", DataTypes.StringType, true);
        // Criando esquema de dados com base nos 2 campos
        // Esse esquema deve corresponder os esquema da tabela do RDB
        StructType schema = DataTypes.createStructType(Arrays.asList(field1, field2));
        
        // Aplicanddo esquema ao Dataset
        //Dataset<Row> jdbcDataset2WithSchema = spark.createDataFrame(jdbcDataset2.javaRDD(), schema);
        Dataset<Row> jdbcDataset2WithSchema = spark.createDataFrame(dadosTrans, schema);
        
        // Persistindo os dados no RDB.
        // A opção SaveMode é importante nesse caso, o padrão é lançar uma exception caso a tabela já exista no RDB.
        jdbcDataset2WithSchema.write()
                .mode(SaveMode.Append)    
                .option("encoding", "utf-8")
                //.jdbc("jdbc:postgresql:pedidos", "public.comments", connectionProperties);
                .jdbc("jdbc:postgresql:pedidos?characterEncoding=utf-8", "public.comments", connectionProperties);
        
        // jdbc_url = "jdbc:postgresql://127.0.0.1/mydb?characterEncoding=utf-8"
    }
    
    
    // Essa classe servirá como esquema dos dados importados de um arquivo .CSV.
    // Ainda não consegui usar ela nos meus exemplos...
    public static class Comments implements Serializable {
        private String name;
        private String comments;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getComments() {
            return comments;
        }

        public void setComments(String comments) {
            this.comments = comments;
        }
    }
    
}
