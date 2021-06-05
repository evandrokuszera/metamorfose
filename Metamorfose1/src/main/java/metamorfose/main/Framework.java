/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import metamorfose.map.EntityMap;
import metamorfose.map.FieldMap;
import metamorfose.transformations.LoadTransformation;
import metamorfose.transformations.Transformation;
import metamorfose.transformations.javascript.JavascriptTransformationEngine;
import metamorfose.transformations.javascript.TransformationType;
import metamorfose.util.SparkSchemaCreator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Evandro
 */
public class Framework {
    
    private static SparkSession spark;
    private static ArrayList<EntityMap> entityMappingQueue = new ArrayList<>();
    private static ArrayList<JavascriptTransformationEngine> javascriptEngineQueue = new ArrayList<>();
    
    public static void addEntityMapQueue(EntityMap entityMap){
        entityMappingQueue.add(entityMap);
        addJavascriptEngineQueue(new JavascriptTransformationEngine());
    }
    
    public static void addJavascriptEngineQueue(JavascriptTransformationEngine engine){
        javascriptEngineQueue.add(engine);
    }
        
    // Recupera uma sessão Spark.
    public static SparkSession getSparkSession(){
        if (spark == null){
                
            spark = SparkSession
                .builder()
                .appName("nameApp")
                .config("spark.master", "local")
                .config("spark.driver.memory", "6g")
                .config("spark.executor.memory", "5g")  
                //.config("spark.executor.cores", "8")
                //.config("spark.default.parallelism", "30") Esse é o padrão do NotaQL. Não informar deixa a escolha do número de tarefas para Spark.
                .getOrCreate();  
            
        }
        return spark;        
    }        
     
    
    // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    
    // versão 3: transformações usando uma fila de EntityMap e JAVASCRIPT para transformações.
    public static Dataset<Row> executeTransformationsQueue(Dataset<Row> rows){        
        
        JavaRDD<Row> RDDrows = rows.toJavaRDD();
        
        // Recuperando o primeiro mapeamento da fila.
        // Como Apache Spark é lazy, tive alguns problemas ao criar pipelines de mapeamentos/transformações.
        // Por exemplo, aplicar um mapeamento sobre dt1 e depois aplicar outro mapeamento sobre o mesmo dataset dt1.
        // Usando essa fila o problema foi resolvido.
        EntityMap currentMapping = entityMappingQueue.get(0);
        // Removendo o mapeamento da fila
        entityMappingQueue.remove(0);
        
        // Caso as transformações usem scripts, 
        //  então esses serão passados para o engine de execução de Javascripts.
        // Motivo: carregar todos os scripts de uma vez tem melhor desempenho do que carregar a cada transformação.
        final JavascriptTransformationEngine currentJavascriptEngine = javascriptEngineQueue.get(0);
        javascriptEngineQueue.remove(0);
        if (currentMapping.getScripts() != null){
            currentJavascriptEngine.setScripts( currentMapping.getScripts() ); 
        }        
        
                      
        JavaRDD<Row> results = RDDrows.map(record ->  {
            List<Object> values = new ArrayList<>();             
            
            // Aplicando regras de mapeamento para cada campo do 'record'
            for (FieldMap fieldMap : currentMapping.getFieldMappings()){
                
                //Object recordColumnValue = record.getAs(fieldMap.getSourceFieldName());
                //Object[] recordColumnValue;
                List recordColumnsValues = new ArrayList();
                
                
                // Experimento - Recuperando dado de origem
                // Verifica a existência de tags no campo de origem.
                // Recupera os dados de origem de acordo com a tag ($VALUE e $LIST)
                if (fieldMap.getSourceFieldName().toUpperCase().contains("$VALUE(NULL)")){
                    recordColumnsValues.add(null);
                
                } else if (fieldMap.getSourceFieldName().contains("$VALUE")){
                    
                    String customizedField = fieldMap.getSourceFieldName().trim();
                    customizedField = customizedField.replace("$VALUE(", "");                    
                    customizedField = customizedField.replace(")", "");
                    //recordColumnValue = customizedField; 
                    recordColumnsValues.add(customizedField);
                    
                } else if (fieldMap.getSourceFieldName().contains("$LIST")){
                    
                    String customizedField = fieldMap.getSourceFieldName().trim();
                    customizedField = customizedField.replace("$LIST(", "");                    
                    customizedField = customizedField.replace(")", "");
                    
                    String fieldElements[] = customizedField.split(",");
                    for (String fieldName : fieldElements){
                        recordColumnsValues.add(record.getAs(fieldName.trim()));
                    }     
                    
                } else {
                    //recordColumnValue = record.getAs(fieldMap.getSourceFieldName());
                    recordColumnsValues.add( record.getAs(fieldMap.getSourceFieldName()) );
                }
                // fim do experimento - Recuperando dado de origem
                
                
                // Transformando dado de origem em dado de destino.
                if (fieldMap.getTransformationType() == TransformationType.JAVASCRIPT){
                // experimento Javascript   
                
                    //values.add(JavascriptTransformationEngine.executeTransformation(fieldMap.getTransformationName(), recordColumnsValues.toArray()) );                
                    values.add( currentJavascriptEngine.executeTransformation(fieldMap.getTransformationName(), recordColumnsValues.toArray()) );                
                
                // fim experimento Javascript
                }
                // Dois tipos de transformação:
                //else if (fieldMap.getTransformationName().length() != 0){   // a) transformação definida pelo usuário
                else if (fieldMap.getTransformationType() == TransformationType.JAVA) {
                    
                    Transformation transfomer = LoadTransformation.getTransformationByClassName( fieldMap.getTransformationName() );
                    if (transfomer != null){ 
                        values.add(transfomer.executeTransformation( recordColumnsValues.toArray() ));
                    } else {
                        System.out.println(Framework.class.getName()+": ERRO AO CARREGAR TRANSFORMATION (POSSIVELMENTE NÃO FOI LOCALIZADA) - "+fieldMap.getTransformationName());
                    }
                
                } else {                                // b) transformação como conversão de tipos de valores (CASTING)          
                    
                        values.add( currentMapping.getConverter().convertValues(fieldMap.getTargetField(), recordColumnsValues.get(0)) );
                    
                }
                
            }
            
            // retorna registro transformado.
            return RowFactory.create(values.toArray());
        });   
        
        //System.out.println( results.collect().toString() );
                       
        // Aplicando o schema ao dataset retornado.       
        StructType schema = SparkSchemaCreator.createSchemaFromEntity(currentMapping.getTargetEntity());

        //System.out.println( results.collect().toString() );
        
        return getSparkSession().createDataFrame(results, schema);        
    }
    
    
    
    // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    // versão 2: transformações usando uma fila de EntityMap. SEM JAVASCRIPT.
    public static Dataset<Row> executeTransformationsQueue2(Dataset<Row> rows){        
        
        JavaRDD<Row> RDDrows = rows.toJavaRDD();
        
        // Recuperando o primeiro mapeamento da fila.
        // Como Apache Spark é lazy, tive alguns problemas ao criar pipelines de mapeamentos/transformações.
        // Por exemplo, aplicar um mapeamento sobre dt1 e depois aplicar outro mapeamento sobre o mesmo dataset dt1.
        // Usando essa fila o problema foi resolvido.
        EntityMap currentMapping = entityMappingQueue.get(0);
        // Removendo o mapeamento da fila
        entityMappingQueue.remove(0);
                      
        JavaRDD<Row> results = RDDrows.map(record ->  {
            List<Object> values = new ArrayList<>();             
                                    
            for (FieldMap fieldMap : currentMapping.getFieldMappings()){
                
                //Object recordColumnValue = record.getAs(fieldMap.getSourceFieldName());
                //Object[] recordColumnValue;
                List recordColumnsValues = new ArrayList();
                
                
                // Experimento
                if (fieldMap.getSourceFieldName().contains("$VALUE(null)")){
                    recordColumnsValues.add(null);
                
                } else if (fieldMap.getSourceFieldName().contains("$VALUE")){
                    
                    String customizedField = fieldMap.getSourceFieldName().trim();
                    customizedField = customizedField.replace("$VALUE(", "");                    
                    customizedField = customizedField.replace(")", "");
                    //recordColumnValue = customizedField; 
                    recordColumnsValues.add(customizedField);
                    
                } else if (fieldMap.getSourceFieldName().contains("$LIST")){
                    
                    String customizedField = fieldMap.getSourceFieldName().trim();
                    customizedField = customizedField.replace("$LIST(", "");                    
                    customizedField = customizedField.replace(")", "");
                    
                    String fieldElements[] = customizedField.split(",");
                    for (String fieldName : fieldElements){
                        recordColumnsValues.add(record.getAs(fieldName.trim()));
                    }     
                    
                } else {
                    //recordColumnValue = record.getAs(fieldMap.getSourceFieldName());
                    recordColumnsValues.add( record.getAs(fieldMap.getSourceFieldName()) );
                }
                // fim do experimento.
                
                
                

                // Dois tipos de transformação:
                if (fieldMap.getTransformationName().length() != 0){   // a) transformação definida pelo usuário
                    
                    Transformation transfomer = LoadTransformation.getTransformationByClassName( fieldMap.getTransformationName() );
                    if (transfomer != null){                        
                        //values.add(transfomer.executeTransformation( recordColumnValue ));
                        values.add(transfomer.executeTransformation( recordColumnsValues.toArray() ));
                    } else {
                        System.out.println(Framework.class.getName()+": ERRO AO CARREGAR TRANSFORMATION (POSSIVELMENTE NÃO FOI LOCALIZADA) - "+fieldMap.getTransformationName());
                    }
                
                } else {                                // b) transformação como conversão de tipos de valores (CASTING)        
                    
                        //values.add( currentMapping.getConverter().convertValues(fieldMap.getTargetField(), recordColumnValue) );      
                        values.add( currentMapping.getConverter().convertValues(fieldMap.getTargetField(), recordColumnsValues.get(0)) );
                    
                }
                
            }
            
            return RowFactory.create(values.toArray());
        });   
        
        //System.out.println( results.collect().toString() );
                       
        // Aplicando o schema ao dataset retornado.       
        StructType schema = SparkSchemaCreator.createSchemaFromEntity(currentMapping.getTargetEntity());

        //System.out.println( results.collect().toString() );
        
        return getSparkSession().createDataFrame(results, schema);        
    }
       
    
    // Salva os registros do modelo em RDB.
    public static void saveDatasetToJDBC(Dataset<Row> rows, String database, String table, boolean appendMode){
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "123456");
        
        long inicio = System.currentTimeMillis();
        
        if (appendMode){
            rows.write()
                .mode(SaveMode.Append)    
                .option("encoding", "utf-8")
                //.jdbc("jdbc:postgresql:pedidos", "public.comments", connectionProperties);
                .jdbc("jdbc:postgresql:"+database+"?characterEncoding=utf-8", "public."+table, connectionProperties);
        } else {
            rows.write()
                .mode(SaveMode.Overwrite)    
                .option("encoding", "utf-8")
                //.jdbc("jdbc:postgresql:pedidos", "public.comments", connectionProperties);
                .jdbc("jdbc:postgresql:"+database+"?characterEncoding=utf-8", "public."+table, connectionProperties);
        }
        
//        rows.write()
//                .mode(SaveMode.Overwrite)    
//                .option("encoding", "utf-8")
//                //.jdbc("jdbc:postgresql:pedidos", "public.comments", connectionProperties);
//                .jdbc("jdbc:postgresql:"+database+"?characterEncoding=utf-8", "public."+table, connectionProperties);
        
        long fim = System.currentTimeMillis();
        
        System.out.println("Framework.saveDatasetToJDBC: " + (fim-inicio)/1000 + " segundos.");
    } 
    
    public static Dataset<Row> getRecordsFromJDBC(String database, String table){
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "123456");
        
        Dataset<Row> jdbcDataset1 = spark.read().jdbc("jdbc:postgresql:"+database, "public."+table, connectionProperties);
        return jdbcDataset1;        
    }
    
    // Recupera os dados do CSV e retorna um Dataset<Row>
    public static Dataset<Row> getRecordsFromCSV(String csv_path){
        
        Dataset<Row> datasetCSV = getSparkSession().read().option("header", "true").csv(csv_path);        
        return datasetCSV;
        
    }
    
    public static void saveRecordsToCSV(Dataset<Row> rows, String filename){        
        rows.write().csv(filename);  
    }
    
    public static void saveRecordsToJSON(Dataset<Row> rows, String filename){
        rows.write().json(filename);
    }
    
}
