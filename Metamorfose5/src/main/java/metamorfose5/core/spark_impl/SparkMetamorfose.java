/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.spark_impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import metamorfose5.util.MetamorfoseUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;
import metamorfose5.map.executor.MappingExecutor;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.Field;
import metamorfose5.core.Metamorfose;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 *
 * @author Evandro
 */
public class SparkMetamorfose implements Serializable, Metamorfose {
    private static ArrayList<MappingExecutor> mappingExecutorQueue = new ArrayList<>();
    
    private static boolean PRINT_MAPREDUCE_OBJS = false;
    private List<EntityMF<Dataset<Row>>> entities = new ArrayList<>();
    public static SparkSession spark;
    

    // Recupera uma sessão Spark.
    public static SparkSession getSparkSession() {
        if (spark == null) {

            spark = SparkSession
                    .builder()
                    .appName("MetamorfoseApp")
                    .config("spark.master", "local")
                    .config("spark.driver.memory", "4g")
                    //.config("spark.testing.memory", "2147480000") teste para erro GC outOfMemory... não funcionou.
                    .config("spark.executor.memory", "4g")
                    //.config("spark.executor.cores", "8")
                    .config("spark.default.parallelism", "8") //30 Esse é o padrão do NotaQL. Não informar deixa a escolha do número de tarefas para Spark.
                    .config("spark.sql.shuffle.partitions", "8")
                    .config("spark.storage.memoryFraction", "0")
                    .getOrCreate();

        }
        return spark;
    }

    // Creating a new entity
    public void createEntity(String entityName, Dataset<Row> records) {
        // First, I am going remove if entityName has already existed.
        if (this.getEntity(entityName) != null) {
            this.entities.remove(this.getEntity(entityName));
        }

        // Second, creating a new entity.
        EntityMF<Dataset<Row>> newEntity = new EntityMF<>(entityName);
        newEntity.setData(records);
        this.entities.add(newEntity);
        newEntity.getData().createOrReplaceTempView(entityName);
    }

    // Removing an existing entity
    public void removeEntity(String entityName) {

        EntityMF<Dataset<Row>> tempEntity = this.getEntity(entityName);

        if (tempEntity != null) {
            tempEntity.getData().unpersist();
            this.entities.remove(tempEntity);
            System.out.println("    METAMORFOSE.REMOVE_ENTITY(): " + entityName);
        }
    }

    // Joining two entities and create a new entity with joinEntityName
    public void joinEntities(String joinEntityName, String entityOne, String entityMany, String keyOne, String keyMany) {

        createEntity(joinEntityName,
                    getEntity(entityOne).getData()
                .join(
                    getEntity(entityMany).getData(),
                    getEntity(entityOne).getData().col(keyOne).equalTo(getEntity(entityMany).getData().col(keyMany)))
        );

    }
    
    // Joining two entities and create a new entity with joinEntityName
    public void rightJoinEntities(String joinEntityName, String entityOne, String entityMany, String keyOne, String keyMany) {

        createEntity(joinEntityName,
                    getEntity(entityOne).getData()
                .join(
                    getEntity(entityMany).getData(),
                    getEntity(entityOne).getData().col(keyOne).equalTo(getEntity(entityMany).getData().col(keyMany)),
                "right_outer") //"right_outer")
        );

    }

    @Override
    public EntityMF<Dataset<Row>> getEntity(String entityName) {
        for (EntityMF e : this.entities) {
            if (e.getName().equals(entityName)) {
                return e;
            }
        }
        return null;
    }
    
    public void printEntities(){
        System.out.println("Metamorfose Entities: ");
        for (EntityMF<Dataset<Row>> entity : this.entities){
            System.out.println(entity);
        }
    }

    @Override
    public List<EntityMF<Dataset<Row>>> getEntities() {
        return this.entities;
    }

    @Override
    public SparkPersist persist() {
        return new SparkPersist(getSparkSession(), this);
    }

    @Override
    public SparkLoad load() {
        return new SparkLoad(getSparkSession(), this);
    }

    @Override
    public SparkFilter filter() {
        return new SparkFilter(getSparkSession(), this);
    }

    
    // ******************************************************************************************
    // ******************************************************************************************
    // TRANSFORMAÇÃO USANDO MODELO JSON COMO MODELO INTERMEDIÁRIO
    // ******************************************************************************************  
    // ******************************************************************************************
    // Recebe um objeto JSON e mapeamentos (EntityMap) e retorna um objeto Row (Spark) com os campos nível 1 do objeto JSON.
    public static Row creatingRowObject(JSONObject jsonObj, MappingExecutor executor) {
        List<Object> outputValues = new ArrayList<>();

        for (FieldMapping fMapping : executor.getMapping().getFieldMappings()){
            for (Field field : fMapping.getTargetFields()){
                if (jsonObj.get(field.getName()) instanceof JSONObject) {
                    outputValues.add(jsonObj.getJSONObject(field.getName()).toString());
                } else if (jsonObj.get(field.getName()) instanceof JSONArray) {
                    outputValues.add(jsonObj.getJSONArray(field.getName()).toString());
                } else {
                    //outputValues.add(jsonObj.getString(field.getName())); // se o campo não for String lança exception!
                    outputValues.add(jsonObj.get(field.getName()).toString());
                }
            }
        }
        return RowFactory.create(outputValues.toArray());
    }
    
    public static StructType createDatasetSchema(MappingExecutor executor){
        List<StructField> fields = new ArrayList<>();

        for (FieldMapping fieldMap : executor.getMapping().getFieldMappings()) {
            StructField field = DataTypes.createStructField(
                                            fieldMap.getTargetFields().get(0).getName(),
                                            DataTypes.StringType,
                                            true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);        
        return schema;
    }
    
    
    public void mapTransform(String entityName, MappingExecutor executor) {
        this.mapTransformTo(entityName, executor, entityName);
    }

    // Using jsonModel
    public void mapTransformTo(String entityName, MappingExecutor executor, String newEntityName) {
        mappingExecutorQueue.add(executor);

        // Recuperando o primeiro mapeamento da fila.
        // Como Apache Spark é lazy, tive alguns problemas ao criar pipelines de mapeamentos/transformações.
        // Por exemplo, aplicar um mapeamento sobre dataset e depois aplicar outro mapeamento sobre o mesmo dataset dataset.
        // Usando essa fila o problema foi resolvido.
        MappingExecutor currentMappingExecutor = mappingExecutorQueue.get(0);
        // Removendo o mapeamento da fila
        mappingExecutorQueue.remove(0);
        
        // STEP 1: Map Function - Processing each record of entity dataset
        JavaRDD<Row> transformedData = this.getEntity(entityName).getData().toJavaRDD().map((record) -> { 
            // convertendo o record para um objeto JSON.  //***************************************************************************************************
            JSONObject inputJSONObj = MetamorfoseUtil.creatingJSONObject(record);
            JSONObject outputJSONObj = new JSONObject();

            if (PRINT_MAPREDUCE_OBJS) System.out.println("MAP FUNCTION:");
            if (PRINT_MAPREDUCE_OBJS) System.out.println("OBJ: " + inputJSONObj);
            
            return creatingRowObject(executor.execute(inputJSONObj), currentMappingExecutor);
        }); // Fim MAP

        // STEP 2: CREATING A NEW ENTITY OR EDITING AN EXISTING ENTITY   
        // 2.1: criando schema com base no currentMapping. Os campos destino (targetFields) são usados para criar esse esquema.
        StructType schema = createDatasetSchema(currentMappingExecutor);
        // 2.2: criando nova entidade (Dataset) com base em transformedData e schema gerados.
        createEntity(newEntityName, getSparkSession().createDataFrame(transformedData, schema));
    }

    
    
//    // Using jsonModel
    public void mapreduceTransformTo(String entityName, String groupKey, MappingExecutor executor, String newEntityName) {        
        mappingExecutorQueue.add(executor);

        // Recuperando o primeiro mapeamento da fila.
        // Como Apache Spark é lazy, tive alguns problemas ao criar pipelines de mapeamentos/transformações.
        // Por exemplo, aplicar um mapeamento sobre dataset e depois aplicar outro mapeamento sobre o mesmo dataset dataset.
        // Usando essa fila o problema foi resolvido.
        MappingExecutor currentMappingExecutor = mappingExecutorQueue.get(0);
        // Removendo o mapeamento da fila
        mappingExecutorQueue.remove(0);

        // STEP 1: MAP each record using keyGroup
        JavaPairRDD<String, String> pairRdd = this.getEntity(entityName).getData().toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {                
                JSONObject jsonObj = MetamorfoseUtil.creatingJSONObject(row); //***************************************************************************************************                                
                // Marca cada objeto com "metamorfose:true" para indicar que ainda não foi processado pela função reduce.
                jsonObj.put("metamorfose", "true");
                // criando uma Tuple com a chave do registro e um JSON com o restante dos campos.
                return new Tuple2<String, String>(jsonObj.get(groupKey).toString(), jsonObj.toString()); 
            }
        });
        
        // STEP 2: REDUCE a group of record and executing transformations acoording EntityMap
        Function2<String, String, String> reduceUmParaMuitos = (o1, o2) -> {
            JSONObject obj1JSON = new JSONObject(o1);
            JSONObject obj2JSON = new JSONObject(o2);
            JSONObject outputJSON = new JSONObject();

            if (PRINT_MAPREDUCE_OBJS) System.out.println("MAPREDUCE FUNCTION:");
            if (PRINT_MAPREDUCE_OBJS) System.out.println("OBJECT1: " + o1);
            if (PRINT_MAPREDUCE_OBJS) System.out.println("OBJECT2: " + o2);
            
            outputJSON = executor.execute(obj1JSON, obj2JSON);
            
            // Return a transform JSON object.
            if (PRINT_MAPREDUCE_OBJS) System.out.println("REDUCE_OBJECT: " + outputJSON.toString());
            return outputJSON.toString();
        }; // Fim Reduce Function

        pairRdd = pairRdd.reduceByKey(reduceUmParaMuitos);        

        // STEP 3: TRANSFORMING JAVAPAIRRDD TO JAVARDD (removing key inserted in MAP function). Some records not processed in Reduce Function will be process here...
        JavaRDD<Row> transformedData = pairRdd.map((o) -> { //***************************************************************************************************            
            JSONObject jsonObj = new JSONObject(o._2);
            try {
                String result = (String) jsonObj.get("metamorfose");
                if (PRINT_MAPREDUCE_OBJS) System.out.println("REGISTRO NÃO PROCESSADO PELA FUNÇÃO REDUCE:");
                if (PRINT_MAPREDUCE_OBJS) System.out.println(jsonObj);
                
                jsonObj = executor.execute(jsonObj, null);
            } catch (JSONException ex) { }            
            
            // Transformando objeto JSON em objeto Row (Spark).
            return creatingRowObject(jsonObj, currentMappingExecutor); 
        });

        pairRdd.unpersist();
        
        // STEP 4: CREATING A NEW ENTITY OR EDITING AN EXISTING ENTITY   
        // 4.1: criando schema com base no currentMapping. Os campos destino (targetFields) são usados para criar esse esquema.
        StructType schema = createDatasetSchema(currentMappingExecutor);
        // 4.2: criando nova entidade (Dataset) com base em transformedData e schema gerados.
        createEntity(newEntityName, getSparkSession().createDataFrame(transformedData, schema));        
    } // fim MapReduceTransformation
        
}
