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
public class TesteJavaScriptTransformations {

    public static void main(String[] args) {        

//        EntityMap entityMap = new EntityMap("CSV_to_DATASET", new Entity("DATASET"), new Entity("CSV"), new Converter());       
//        entityMap.mapFields("id_item", "INT", "id_item", "STRING", "", TransformationType.CASTING, "");
//        entityMap.mapFields("pedido_id", "INT", "pedido_id", "STRING", "", TransformationType.CASTING, "");
//        entityMap.mapFields("desc", "STRING", "desc", "STRING", "upper", TransformationType.JAVASCRIPT, "function upper(value) {return value[0].toUpperCase();}");
//        entityMap.mapFields("qtde", "INT", "qtde", "STRING", "", TransformationType.CASTING, "");
//        entityMap.mapFields("valor", "DOUBLE", "valor", "STRING", "desconto", TransformationType.JAVASCRIPT, "function desconto(value) {return value[0]/100 * value[0];}");

//        EntityMapJsonUtility.saveToJSON(entityMap, "d:\\teste.json");
        
        EntityMap entityMap = EntityMapJsonUtility.loadFromJSON("d:\\teste.json");        
                
        Framework.addEntityMapQueue(entityMap);        

        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\itens.CSV");
                              
        Dataset<Row> convertedRows = Framework.executeTransformationsQueue(records);     
        
        convertedRows.show(false);
        
//        Framework.saveDatasetToJDBC(convertedRows, "simcaq", "mats_script", false);
    }
    
}
