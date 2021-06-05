/*
 * Objetivo: transformar relacionamento 1:N para JSON com vetor documentos embutidos. 
 * Essa classe é um primeiro exemplo para transformar RDB em JSON usando Spark.
 *
 * Passos executados:
 * a) Dois recursos são lidos e armazenados em Datasets. Um recurso pode ser uma tabela RDB, CSV ou um JSON. 
 * b) Através do Spark, os dois recursos são unidos (JOIN) por uma chave em comum, formando uma grande tabela.
 * c) Função MapToPair é aplicada sobre o recurso unido, de forma a selecionar uma chave única para cada linha.
 * d) Função ReduceByKey é aplicada para agrupar as linhas por chave e criar o array de documentos embutidos.
 * Obs.:
 * - Tomar cuidado com nomes de colunas duplicados no recurso unido.
 * - A classe JSONObject é utilizada para estruturar os dados, mas os dados são mantidos como Strings.
 * - 
 */
package testes;

import metamorfose.main.Framework;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;


/**
 *
 * @author Evandro
 */
public class ExRelacionamento_1_N_toJSON_DocumentEmbedd {
    
    public static void main(String[] args) {
        Dataset<Row> pedidos = Framework.getSparkSession().read().json("D:\\notaql-dados\\pedidos.json");
        //pedidos.printSchema();
        //pedidos.show(false);

        Dataset<Row> itens = Framework.getSparkSession().read().json("D:\\notaql-dados\\itens.json");
        //itens.printSchema();
        //itens.show(false);     

        Dataset<Row> join = pedidos.join(itens, pedidos.col("id_pedido").equalTo(itens.col("pedido_id")));        
        join.show(false);
        
        OneToManyToDocumentEmbedd(join, pedidos.columns(), itens.columns(), "id_pedido", "itens").saveAsTextFile("D:\\notaql-dados\\pedidoItens.json");
                //.foreach(record -> System.out.println(record));
    }
    
       
    public static JavaRDD<String> OneToManyToDocumentEmbedd(Dataset<Row> join, String[] oneSideColumns, String[] manySideColumns, String groupKey, String embedVectorName) {
         String[] joinColumns = join.columns();

        // MAPEAMENTO DOS REGISTROS DATASET PARA PARES CHAVE-VALOR.
        // OBS.: 
        //    - O PARÂMETRO CHAVE É USADO PARA IDENTIFICAR O REGISTRO UNICAMENTE.
        //    - PARA FACILITAR A IMPLEMENTAÇÃO OS REGISTROS SÃO ARMAZENADOS EM OBJETOS JSON.
        JavaPairRDD<String, String> pairRdd = join.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row csvRecord) throws Exception {
                JSONObject obj = new JSONObject();
                
                for (String col : joinColumns){
                    obj.put(col, csvRecord.getAs(col).toString());
                }                
                
                return new Tuple2<String,String>(obj.get( groupKey ).toString(), obj.toString());
            }

        });  
        
        //pairRdd.foreach(f -> System.out.println(f._2));
      
        // DEFINIÇÃO DA FUNÇÃO REDUCE (REDUCEBYKEY) PARA TRANSFORMAR RELACIONAMENTO 1:N EM JSON COM ARRAY DE DOCUMENTOS EMBUTIDOS.
        // OBS.:
        //   - OS REGISTROS SÃO AGRUPADOS PELO PARÊMTRO CHAVE.
        //   - UTILIZA OS NOMES DAS COLUNAS DAS ENTIDADES DO 'LADO UM' E 'LADO MUITOS' PARA CRIAR O JSON.
        //   - ENTIDADE DO 'LADO UM' COMPÕEM O PRIMEIRO NÍVEL DE CAMPOS DO OBJETO JSON.
        //   - ENTIDADE DO 'LADO MUITOS' COMPÕEM O ARRAY DE OBJETOS EMBUTIDOS DO OBJETO JSON.
        Function2<String, String, String> reduceUmParaMuitos = (o1, o2) -> {
            
            JSONObject oneSideObj = new JSONObject(o1);
            JSONObject manySideObj = new JSONObject(o2);            
            
            // If first call to reduce function, then the array field name doesn´t exist
            if ( ! oneSideObj.has(embedVectorName )){

                // Retriving entity fields for OneSideEntity and put into JSONObject
                JSONObject oneSideDoc = extractDataField(oneSideObj, oneSideColumns);
                
                // Creating document embed. The entity data of OneSideEntity will be stored in this document. 
                JSONObject innerDoc = extractDataField(oneSideObj, manySideColumns);                
              
                // Creating a vector of Embedded Documents and put the first Document Embed in the vector.
                JSONArray DocEmbedVector = new JSONArray();
                DocEmbedVector.put(innerDoc);
                
                // Add the vector of Embedded Documents into OneSideEntity.
                oneSideDoc.put(embedVectorName, DocEmbedVector);
                
                oneSideObj = oneSideDoc;
            }            
            
            // Creating document embedd. The entity data of ManySideEntity will be stored in this document. 
            JSONObject innerDoc = extractDataField(manySideObj, manySideColumns);
            
            // Add Embedded Documents in early vector created.
            oneSideObj.getJSONArray(embedVectorName).put(innerDoc);   
            
            System.out.println(oneSideObj);

            return oneSideObj.toString();
        };
        
        JavaPairRDD<String,String> reduce = pairRdd.reduceByKey( reduceUmParaMuitos );
        
        // Creating a newRDD without key of pair key-value.
        JavaRDD<String> newRDD = reduce.map(o -> o._2);
                
        return newRDD;
    }
    
    // Objective: Extract a subset of fields of a JSONObject and create a new JSONObject with these fields.
    // Parameters:
    //   - sourceObject: JSONObject with source fields.
    //   - columns: a vector with the name fields that will be extrated from sourceObject.
    // Return:
    //   - new JSONObject with the fields extracted from sourceObject.
    public static JSONObject extractDataField(JSONObject sourceObject, String[] columns){
        JSONObject newJSONObject = new JSONObject();
        
        for (String col : columns){
            newJSONObject.put(col, sourceObject.get(col).toString());
        }
        
        return newJSONObject;
    }
        
}
