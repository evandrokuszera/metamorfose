/*
 * Objetivo: esta classe executa o processo de conversão de rdb para nosql. 
 * O usuário deve fornecer os parâmetros e executar o código.
 * Passos que ela executa:
 *  - Lê os parâmetros fornecidos pelo usuário.
 *  - Carregar um objeto ConversionProcess (esse objeto deve ser criado pelo QBMetrics).
 *  - Executar a leitura do RDB, transformação e persistência de dados em JSON.
 *  - Persistência no MongoDB (sem suporte a outros NoSQLs ainda, não implementei).
 */
package metamorfose5.rdbtonosql.command.run;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import metamorfose5.rdbtonosql.command.impl.generators.ColCommandGenerator;
import dag.nosql_schema.ConversionProcess;
import dag.nosql_schema.NoSQLSchema;
import dag.persistence.ConversionProcessJson;
import dag.persistence.JSONPersistence;
import java.util.ArrayList;
import metamorfose5.rdbtonosql.command.CommandGenerator;
import metamorfose5.rdbtonosql.command.impl.generators.DocCommandGenerator;
import metamorfose5.rdbtonosql.command.impl.executors.MetamorfoseCommandExecutor;
import java.util.List;
import metamorfose5.rdbtonosql.command.Command;
import metamorfose5.rdbtonosql.command.CommandExecutor;
import metamorfose5.rdbtonosql.command.impl.executors.JDBCCommandExecutor;
import metamorfose5.util.MetamorfoseUtil;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class RunProcess {
    private static String msgs = "";
    private static String conversionProcessFilename = "";
    
    public static String command_executor_type = "spark";  // spark or JDBC
    public static NoSQLTargetModelEnum nosql_target_model = NoSQLTargetModelEnum.DOC; // doc or col
    public static String rdb_server = "localhost";
    public static String rdb_database = "dsvendas"; //"ds2_10mb";
    public static String rdb_user = "postgres";
    public static String rdb_passwd = "123456";
    public static String mongoserver = "localhost";
    
        
    //STEP 0: - Convert RDB to NoSQL using a ConversionProcess specification object.
    public static void run(String conversionProcessFilename){
        ConversionProcess conversionProcess = null;
        
        if (conversionProcessFilename!=null){
            // read JSON from disk.
            JSONObject jsonConversionProcess = JSONPersistence.loadJSONfromFile(conversionProcessFilename);
            // create the ConversionProcesso object by JSON.
            conversionProcess = ConversionProcessJson.fromJSON(jsonConversionProcess);
            // If there is a schema, then transform from RDB to NoSQL.
            if (!conversionProcess.getSchemas().isEmpty()) {
                System.out.println("\nMetamorfose.run().loadConversionProcess(): OK - processo de conversão carregado com sucesso.");
                
                // RUN CONVERSION PROCCESS ********************
                for (NoSQLSchema schema : conversionProcess.getSchemas()){
                    convert_RDB_to_NoSQL(schema, nosql_target_model);
                }
                
            } else {
                System.out.println("\nMetamorfose.run().loadConversionProcess(): ERRO - o processo de conversão não contém esquemas para processar!"); 
            }                
        } else {
            System.out.println("\nMetamorfose.run().loadConversionProcess(): ERRO - não foi informado a localização do arquivo ConversionProcess!");
        }
    }
    
    //STEP 0: - Convert RDB to NoSQL using a NoSQL Schema.
    public static void run(NoSQLSchema schema){
        // RUN CONVERSION PROCCESS ********************
        convert_RDB_to_NoSQL(schema, nosql_target_model);
    }
    
    // STEP 1 -  Call the Metamorfose to convert an RDB to NoSQL, according target NoSQL Model
    public static void convert_RDB_to_NoSQL(NoSQLSchema schema, NoSQLTargetModelEnum targetNoSQLModel){
        String msg = "";
        
        for (int i=0; i<schema.getEntities().size(); i++){
            msgs += msg = "\n\nMetamorfose.DAG ENTITY: " + schema.getEntityName(i);
            System.out.print(msg);

            // Step 2 -  Gera conjunto de comandos para converter RDB para NoSQL (Doc ou Col).
            List<Command> generatedCommands = generateCommands(schema.getEntities().get(i), targetNoSQLModel);
            
            if (command_executor_type.equals("spark")) {

                // Step 3 - Usa Metamorfose para converter as instancias do RDB para NoSQL(JSON)
                List<Row> convertedInstances = convertInstances(schema.getEntityName(i), schema.getEntities().get(i), generatedCommands);

                // Step 4 - Salva as instancias geradas no MongoDB.
                saveToMongoDB(schema.getName(), schema.getEntityName(i), convertedInstances);
            
            } else if (command_executor_type.equals("JDBC")){
                // EXEPERIMENTAL
                JSONArray convertedInstances = convertInstancesJDBC(schema.getEntityName(i), schema.getEntities().get(i), generatedCommands);
                saveToMongoDB_JDBC(schema.getName(), schema.getEntityName(i), convertedInstances);
            }
        }
    }
    
    //STEP 2: - Gerar Commandos de Transformação de Dados
    private static List<Command> generateCommands(DirectedAcyclicGraph<TableVertex,RelationshipEdge> entityDAG, NoSQLTargetModelEnum targetNoSQLModel){
        String msg = "";
        long ini = 0, fim = 0;
        CommandGenerator commandGen = null;
        
        ini = System.currentTimeMillis();        
                
        // Define um objeto CommandGenerator, de acordo com o modelo de dados destino (Documento ou Família de Colunas).
        if (targetNoSQLModel==NoSQLTargetModelEnum.DOC) commandGen = new DocCommandGenerator();
        if (targetNoSQLModel==NoSQLTargetModelEnum.COL) commandGen = new ColCommandGenerator();
        
        // Gera os comandos, de acordo com o DAG de entrada.
        commandGen.generateCommands( entityDAG );
        msgs += "\n"+commandGen.printCommands();
        
        fim = System.currentTimeMillis();
        msgs += msg = "\nMetamorfose.generateCommands(): "+((fim-ini)/1000)+" segs.";
        System.out.print(msg);
        
        return commandGen.getCommands();
    }
    
    //STEP 3: - Transformando dados (Ler dados do RDB, transformar em memória, retornar array)
    private static List<Row> convertInstances(String entityName, DirectedAcyclicGraph<TableVertex,RelationshipEdge> entityDAG, List<Command> generatedCommands){
        String msg = "";
        long ini = 0, fim = 0;    
        
        ini = System.currentTimeMillis();
        
        // Objeto para ler, transformar e retornar instancias de objetos do RDB no formato JSON
        MetamorfoseCommandExecutor executor = new MetamorfoseCommandExecutor(rdb_server, rdb_database, rdb_user, rdb_passwd);
        executor.setGraph( entityDAG );
        executor.setCommands( (ArrayList<Command>) generatedCommands );
        
        // Método execute() faz a leitura dos dados do RDB, converte e retorna um array de objetos JSON como resultado.
        executor.execute();      
        
        // Recuperando objetos transformados (essa instrução executa de "fato" das transformações).
        List<Row> entitiesArray = executor.getMetamorfose().getEntity(entityName).getData().collectAsList();
            
        fim = System.currentTimeMillis();
        msgs += msg = "\nMetamorfose.convertInstances.execute(): "+((fim-ini)/1000)+" segs.";
        System.out.print(msg);
        
        // Retorna as entidades geradas pelo Metamorfose.
        return entitiesArray;
    }
    
    //STEP 4: - SAVE TO MONGODB: Estabelece conexão, Cria DB, Cria coleção, Persiste dados.
    private static void saveToMongoDB(String databaseName, String collectionName, List<Row> entities){
        String msg = "";
        System.out.println("Metamorfose.saveToMongoDB(): Conectando no MongoDB...");
        MongoClient mongoClient = new MongoClient(mongoserver);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        System.out.println("Metamorfose.saveToMongoDB(): Conexão bem sucedida!");
        
        long ini, fim = 0;

        msgs += msg = "\nMetamorfose.saveToMongo().Input: " + entities.size() + " JSON Objects.";
        System.out.print(msg);
        
        ini = System.currentTimeMillis();
        for (Row objRow : entities){
            String objLimpo = MetamorfoseUtil.creatingJSONObject(objRow)
                                .toString()
                                .replace("\"{", "{")
                                .replace("}\"", "}") // Gambiara!!!!!
                                .replace("\\", "");  // LEIA: ao chamar creatingJSONObject(objRow), um objeto JSON com apenas os campos level=1 é retornado. Os demais campos estão armazenados como String. Para recuperar o JSON completo basta remover algumas aspas que objRow armazena. 
            
            Document doc = Document.parse(objLimpo);
            collection.insertOne(doc);
        }
        fim = System.currentTimeMillis();
        msgs += msg = "\nMetamorfose.saveToMongoDB().InsertObj: "+((fim-ini)/1000)+" segs.";
        System.out.print(msg);
        msgs += msg = "\nMetamorfose.saveToMongoDB().Output (collection "+collectionName+"): "+collection.count() + " Objects.";
        System.out.println(msg);
        
        mongoClient.close();
    }
    
    
    
    
    
    
    
    
    /////////////////////////////////////////////
    // EXPERIMENTAL... JDBC COMMAND EXECUTOR    
    // A implementação com spark funciona bem! 
    // Minha implementação JDBC tem problemas de desempenho aos executar joins entre entidades.
    /////////////////////////////////////////////
    
    //STEP 3: - Transformando dados (Ler dados do RDB, transformar em memória, retornar array)
    private static JSONArray convertInstancesJDBC(String entityName, DirectedAcyclicGraph<TableVertex,RelationshipEdge> entityDAG, List<Command> generatedCommands){
        String msg = "";
        long ini = 0, fim = 0;    
        
        ini = System.currentTimeMillis();
        
        // Objeto para ler, transformar e retornar instancias de objetos do RDB no formato JSON
        JDBCCommandExecutor executor = new JDBCCommandExecutor(rdb_server, rdb_database, rdb_user, rdb_passwd);
        executor.setGraph( entityDAG );
        executor.setCommands( (ArrayList<Command>) generatedCommands );
        
        // Método execute() faz a leitura dos dados do RDB, converte e retorna um array de objetos JSON como resultado.
        executor.execute();      
        
        // Recuperando objetos transformados (essa instrução executa de "fato" das transformações).
        JSONArray arrayEntities = executor.getEntity(entityName);
            
        fim = System.currentTimeMillis();
        msgs += msg = "\nMetamorfose.convertInstances.execute(): "+((fim-ini)/1000)+" segs.";
        System.out.print(msg);
        
        // Retorna as entidades geradas pelo Metamorfose.
        return arrayEntities;
    }
    
    //STEP 4: - SAVE TO MONGODB: Estabelece conexão, Cria DB, Cria coleção, Persiste dados.
    private static void saveToMongoDB_JDBC(String databaseName, String collectionName, JSONArray entities){
        String msg = "";
        System.out.println("Metamorfose.JDBC().saveToMongoDB(): Conectando no MongoDB...");
        MongoClient mongoClient = new MongoClient(mongoserver);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        System.out.println("Metamorfose.JDBC().saveToMongoDB(): Conexão bem sucedida!");
        
        long ini, fim = 0;

        msgs += msg = "\nMetamorfose.JDBC().saveToMongo().Input: " + entities.length() + " JSON Objects.";
        System.out.print(msg);
        
        ini = System.currentTimeMillis();
        for (int i=0; i<entities.length(); i++){
            Document doc = Document.parse(entities.getJSONObject(i).toString());
            collection.insertOne(doc);
        }
        fim = System.currentTimeMillis();
        msgs += msg = "\nMetamorfose.JDBC().saveToMongoDB().InsertObj: "+((fim-ini)/1000)+" segs.";
        System.out.print(msg);
        msgs += msg = "\nMetamorfose.JDBC().saveToMongoDB().Output (collection "+collectionName+"): "+collection.count() + " Objects.";
        System.out.println(msg);
        
        mongoClient.close();
    }
}