/*
 * Objetivo: testar o Framework para transformação dos dados do projeto Simcaq.
 * Os testes basicamente executam:
 * a) carregamento do arquivo CSV.
 * b) transformação dos dados de acordo com protocolo de mapeamento
 * c) execução de uma consulta sobre o dataset transformado para medir o tempo gasto na carga, transformação e retorno dos dados.
 * 
 */
package testes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import metamorfose.converters.Converter;
import metamorfose.main.Framework;
import metamorfose.map.EntityMap;
import metamorfose.model.Entity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Evandro
 */
public class TesteFramework {
    
    public static void main(String[] args) {
        
        //teste1_Load_Trans_Query(); 
        //teste2_Load_Query();
        //teste3_Load_Query();
        teste4_Load_Union_Trans_SaveJDBC();
        
        
    }    
    
    // Objetivo: este executa três passos:
    // a) carregar CSV para Dataset
    // b) transforma Dataset conforme esquema definido
    // c) consulta Dataset: "select sexo, count(*) from matriculas group by sexo"
    // Resultados: 
    //   - 1m07s
    public static void teste1_Load_Trans_Query() {
        
        // seta o mapeamento do esquema de origem para destino.
        //Framework.setMapping( getMappingFromCSV(0) );
        Framework.addEntityMapQueue( getMappingFromCSV(0) ); // ************************
        
        // carrega...
        Dataset<Row> csvRecords = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1000k.CSV");
        
        // transforma...
        long inicioTrans = System.currentTimeMillis();
        Dataset<Row> rowsTransformed = Framework.executeTransformationsQueue(csvRecords); // ********
        long fimTrans = System.currentTimeMillis();
        
        // consulta...
        rowsTransformed.createOrReplaceTempView("matriculas");
        Dataset<Row> results = Framework.getSparkSession().sql("select sexo, count(*) from matriculas group by sexo");
        results.show();
        
    }
    
    // Objetivo: este executa DOIS passos:
    // a) carregar CSV para Dataset    
    // b) consulta Dataset: "SELECT TP_SEXO, COUNT(*) FROM matriculas GROUP BY TP_SEXO"
    // Resultados (obs. não tem transformação de dados neste teste): 
    //   - 25s!
    //   - 38s
    public static void teste2_Load_Query() {
        
        //Framework.setMapping( getMappingFromCSV() );
                
        Dataset<Row> csvRecords = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k.CSV");
                
        long inicioTrans = System.currentTimeMillis();  
        csvRecords.createOrReplaceTempView("matriculas");
        Dataset<Row> results = Framework.getSparkSession().sql("SELECT TP_SEXO, COUNT(*) FROM matriculas GROUP BY TP_SEXO");
        results.show();
        long fimTrans = System.currentTimeMillis();
        
        System.out.println("Tempo de execução: " + (fimTrans-inicioTrans)/1000);        
    }
    
    // Objetivo: este executa DOIS passos:
    // a) carregar 2 CSVs para Dataset    
    // b) executar união dos CSVs
    // b) consultar Dataset: "SELECT TP_SEXO, COUNT(*) FROM matriculas GROUP BY TP_SEXO"
    // Resultados (8 milhões de registro:
    //   - 45s
    //   - 47s        
    public static void teste3_Load_Query() {
        
        //Framework.setMapping( getMappingFromCSV() );
                
        Dataset<Row> csvRecords1 = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k.CSV");
        Dataset<Row> csvRecords2 = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_CO.CSV");
        Dataset<Row> union = csvRecords1.union(csvRecords2);
                        
        long inicioTrans = System.currentTimeMillis();  
        union.createOrReplaceTempView("matriculas");
        Dataset<Row> results = Framework.getSparkSession().sql("SELECT TP_SEXO, COUNT(*) FROM matriculas GROUP BY TP_SEXO");
        results.show();
        long fimTrans = System.currentTimeMillis();
        
        System.out.println("Tempo de execução: " + (fimTrans-inicioTrans)/1000);        
    }
    
    
    // Objetivo: 
    // a) carregar 2 CSVs para Dataset    
    // b) executar união dos CSVs
    // c) transformar os dados de acordo com os mapeamentos.
    // d) persistir os dados em tabela postgres
    // Tempo (8 milhões de registros transformados e persistidos no postgres):
    //   - 18m32s
    //   - 18m20s
    public static void teste4_Load_Union_Trans_SaveJDBC(){
        
        //Framework.setMapping( getMappingFromCSV(0) );
        Framework.addEntityMapQueue( getMappingFromCSV(0) );
        
        Dataset<Row> csvRecords1 = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k.CSV");
        Dataset<Row> csvRecords2 = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_CO.CSV");
        Dataset<Row> union = csvRecords1.union(csvRecords2);
        
        Dataset<Row> results = Framework.executeTransformationsQueue(union);
        
        Framework.saveDatasetToJDBC(results,"simcaq","mats8k", false); 
        
    }
    
    
    
    
    
    // Esse método carrega os mapeamentos do projeto Simcaq do arquivo MapMatriculas2013.CSV.
    // Retorna um objeto EntityMap com os mapeamentos entre campos da origem para destino.
    // Obs.: uma transformação definida pelo usuário é utilizada nesse exemplo: SexoTransformation!
    //
    // Parâmetro: == 0 recupera todos os mapeamentos com status S do arquivo .CSV.
    // Parâmetro:  > 0 recupera apenas o número informado de mapeamentos do arquivo .CSV.
    public static EntityMap getMappingFromCSV(int numMapeamentos){
        int mappingCount = 0;
        EntityMap entityMap = new EntityMap("SIMCAQ", new Entity("matriculas"), new Entity("CSV"), new Converter());
       
        try {
            FileReader arq = new FileReader("D:\\notaql-dados\\MapMatriculas2013.CSV");
            BufferedReader br = new BufferedReader(arq);
            
            String linha = br.readLine();
            linha = br.readLine(); // ignorando a primeira linha do .CSV
            while (linha != null){
                if (mappingCount == numMapeamentos && numMapeamentos != 0)
                    break;
                
                String[] attributes = linha.split(";");
                
                if (attributes[0].equals("S")){
                    
                    if (attributes[1].equals("sexo")){
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", "com.kuszera.simcaq.transformations.SexoTransformation", "");
                    } else {
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", "", "");
                    }
                    
                    mappingCount++;
                                        
                    //System.out.println( entityMap.getTargetEntity().getFields().get(entityMap.getTargetEntity().getFields().size()-1).getName() );
                    
                }                
                linha = br.readLine();                
            }
            
            br.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        //System.out.println(entityMap.toString());
                
        return entityMap;
    }
    
}
