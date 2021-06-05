/*

 * Esta classe executa as transformações de dados usando função JavaScript.
 * É preciso analisar o desempenho dessa solução.
 * Analisei o desempenho: ficou semelhante ao usar funções Java compiladas dentro do framework !!!!
 */
package testes;

import metamorfose.main.Framework;
import metamorfose.map.EntityMap;
import metamorfose.map.EntityMapJsonUtility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Evandro
 */
public class TesteSimcaqJavaScriptTransformations {

    public static void main(String[] args) {
        
//        Mantive esse trecho de código para entender o objetivo desse experimento.
//        Função Javascript SexoTransformation é chamada dinamicamente seguindo as regras de mapeamento.
//        String sexoTrans = "function SexoTransformation(arg) {  "
//                + "	if (arg == 'F')                   "
//                + "         return 1;                   "
//                + "     else if (arg == 'M')            "
//                + "         return 2;                   "
//                + "}";
//        
//        EntityMap entityMap = new EntityMap("CSV_to_DATASET", new Entity("DATASET"), new Entity("CSV"), new Converter());
//        entityMap.mapFields("ano", "INT", "ANO_CENSO", "STRING", "","");
//        entityMap.mapFields("sexo", "INT", "TP_SEXO", "STRING", "", "SexoTransformation");
        
                
        EntityMap entityMap = EntityMapJsonUtility.loadFromJSON("D:\\notaql-dados\\save_mappings\\simcaq\\matriculas2013.json");
        //EntityMap entityMap = EntityMapJsonUtility.loadFromJSON("D:\\notaql-dados\\save_mappings\\simcaq\\matriculas2013_javascript.json");
        
        // copiando os nomes das funções de transformação para o campo UDF.
        // Nesse experimento estou armazendo o nome da função Javascript no campo UDF, de forma temporária para validar o experimento.
        // Será necessário repensar a estrutura dos mapeamentos.
//        for (FieldMap fm : entityMap.getFieldMappings()){
//            fm.setUdf(fm.getTransformationName());
//        }
                
        Framework.addEntityMapQueue(entityMap);        

        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k_1.CSV");
        Dataset<Row> records2 = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_3276k_2.CSV");
        Dataset<Row> union = records.union(records2);
                        
        Dataset<Row> convertedRows = Framework.executeTransformationsQueue(union);        
        
        Framework.saveDatasetToJDBC(convertedRows, "simcaq", "mats_script", false);
    }
    
}
