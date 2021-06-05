/*
 * Objetivo principal: ler dados de arquivo .CSV e carregar em Dataset Spark com modelo definido em JavaBean.
 * 
 * As seguintes etapas foram realizadas:
 * a) definição do JavaBean para representar o Schema do Dataset
 * b) transformação dos dados do CSV para objeto JavaBean
 * c) usar Dataset de objetos JavaBean para executar comandos SQL.
 */

package testes;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class ExCarregarCSVtoDatasetWithJavaBean {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("teste")
                .config("spark.master", "local")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "5g")               
                .getOrCreate();
        
        Dataset<Row> csvRecords = getRecordsFromCSV(spark, "D:\\notaql-dados\\matriculas.CSV");
        
        long inicioTrans = System.currentTimeMillis();
        Dataset<Row> rowsTransformed = executeTransformationsUsingJavaBean(spark, csvRecords);
        long fimTrans = System.currentTimeMillis();
        
        rowsTransformed.createOrReplaceTempView("matriculas");
        Dataset<Row> results = spark.sql("SELECT * FROM matriculas WHERE sexo = 1 and nasc_ano = 2010").limit(10);
        results.show();        

        spark.close();  
        
        System.out.println("Transformations: " + (fimTrans-inicioTrans)/1000 + "s.");
    }
    
    // Recupera os dados do CSV e retorna um Dataset<Row>
    private static Dataset<Row> getRecordsFromCSV(SparkSession spark, String csv_path){
        
        Dataset<Row> datasetCSV = spark.read().option("header", "true").csv(csv_path);        
        return datasetCSV;
        
    }   
    
    // Processa Dataset com registros CSV.
    // Este método é o principal ponto de extensão. Aqui deve ser executada duas funcionalidades:
    // a) Mapeamento: entre campos do registro CSV e campos do modelo destino (JavaBean).
    // b) Transformações: transformações de dados, p. ex. conversão de tipos de dados, agrupamentos, agregações, etc.
    private static Dataset<Row> executeTransformationsUsingJavaBean(SparkSession spark, Dataset<Row> rows){        
        
        JavaRDD<Row> RDDrows = rows.toJavaRDD();
        
        JavaRDD<Modelo> results = RDDrows.map( record ->  {
            
            Modelo modelo = new Modelo();
            
            // Mapeamento dos campos CSV para classe Modelo...
            modelo.setAno_censo( Integer.parseInt(record.getAs("ANO"))           );
            modelo.setId(        Integer.parseInt(record.getAs("COD_MATRICULA")) );            
            modelo.setNasc_dia(  Integer.parseInt(record.getAs("NU_DIA"))        );
            modelo.setNasc_mes(  Integer.parseInt(record.getAs("NU_MES"))        );
            modelo.setNasc_ano(  Integer.parseInt(record.getAs("NU_ANO"))        );

            // Exemplo de transformação...
            switch ( record.getAs("TP_SEXO").toString() ){
                case "F":
                    modelo.setSexo(1);
                    break;
                case "M":
                    modelo.setSexo(0);
                    break;
            }
            modelo.setSigla_uf( record.getAs("UF") );
            
            return modelo;
        });       
        
        return spark.createDataFrame(results, Modelo.class);
    }    
        
    // Classe interna representado modelo de dados destino.
    // É interessante poder gerar essa classes a apartir da definição das fontes de Origem e Destino.
    public static class Modelo{
        private int ano_censo;
        private int id;
        private int nasc_dia;
        private int nasc_mes;
        private int nasc_ano;
        private int sexo;
        private String sigla_uf;

        public int getAno_censo() {
            return ano_censo;
        }

        public void setAno_censo(int ano_censo) {
            this.ano_censo = ano_censo;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getNasc_dia() {
            return nasc_dia;
        }

        public void setNasc_dia(int nasc_dia) {
            this.nasc_dia = nasc_dia;
        }

        public int getNasc_mes() {
            return nasc_mes;
        }

        public void setNasc_mes(int nasc_mes) {
            this.nasc_mes = nasc_mes;
        }

        public int getNasc_ano() {
            return nasc_ano;
        }

        public void setNasc_ano(int nasc_ano) {
            this.nasc_ano = nasc_ano;
        }

        public int getSexo() {
            return sexo;
        }

        public void setSexo(int sexo) {
            this.sexo = sexo;
        }

        public String getSigla_uf() {
            return sigla_uf;
        }

        public void setSigla_uf(String sigla_uf) {
            this.sigla_uf = sigla_uf;
        }
    }
    
    
}
