/*
    This class shows how to embedded two RDB entities into a JSON document.
    The mapping are defined manually, using the Mapping object.
    Then, Metamorfose is used to load, transform and print the transformed data.
 */
package rdbtonosql.jdbc_impl;

import metamorfose5.core.jdbc_impl.JDBCMetamorfose;
import metamorfose5.map.script.Util;
import metamorfose5.map.script.impl.JSEmbeddedCommands;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.Mapping;
import metamorfose5.map.executor.MappingExecutor;

/**
 *
 * @author Evandro
 */
public class Manual_Mappings_RDB_to_NoSQL {        
    public static void main(String[] args) {
        
        // STEP 1 - mapping definitions (input RDB tables: Orders and Orderlines)        
        Mapping mapping = new Mapping();
        
        //  Here, we are manually mapping source fields to target fields (ONE-TO-ONE).
        mapping.mapFields(FieldMapping.builder()
                            .casting("id_order", "string", "id_order", "string").get());
        
        mapping.mapFields(FieldMapping.builder()
                            .casting("orderdate", "string", "orderdate", "string").get());
        
        mapping.mapFields(FieldMapping.builder()
                            .casting("customerid", "string", "customerid", "string").get());
        
        mapping.mapFields(FieldMapping.builder()
                            .casting("tax", "string", "tax", "string").get());
        
        mapping.mapFields(FieldMapping.builder()
                            .casting("totalamount", "string", "totalamount", "string").get());        
        
        // Here, we are mapping MANY fields to ONE field...
        String embeddedFields[] = {"orderlineid", "orderid", "prod_id", "quantity", "orderlinedate"};
        String jScript = JSEmbeddedCommands.getManyEmbeddedScript("orderlines", Util.getArrayList(embeddedFields));
        
        mapping.mapFields( FieldMapping.builder()
                            .addSourceField("orderlineid", "string")
                            .addSourceField("orderid", "string")
                            .addSourceField("prod_id", "string")
                            .addSourceField("quantity", "string")
                            .addSourceField("orderlinedate", "string")
                            .addTargetField("orderlines", "string") // target field!
                            .UDF("arrayEmbedded", jScript) // UDF responsable to convert the source fields to the target field.
                            .get()
        );      
        
        // STEP 2 - executor object. 
        //  This object encapsulates the logic to transform input entities to output entities, according to the mappings.
        MappingExecutor executor = new MappingExecutor(mapping);     
        
        // STEP 3 - Metamorfose object. This object is responsable to execute the transformation logic over the input data.        
        JDBCMetamorfose mjdbc = new JDBCMetamorfose();
        
        // Here, we are loading the input data from the RDB ds2_10mb into the memory.        
        mjdbc.load().fromJDBC("orders", "localhost", "ds2_10mb", "postgres", "123456", "orders");
        mjdbc.load().fromJDBC("orderlines", "localhost", "ds2_10mb", "postgres", "123456", "orderlines");
        
        // Then, we are joinning the Orders and Orderlines on the id_order and orderid fields, creating the new entity orders_orderlines.
        mjdbc.joinEntities("orders_orderlines", "orders", "orderlines", "id_order", "orderid");
        
        // Then, we call a mapreduce transformation over orders_orderlines entity using the mapping definitions encapsulated in executor object.
        //  as result, a new entity called 'results' is created.
        mjdbc.mapreduceTransformTo("orders_orderlines", "id_order", executor, "results");
        
        // STEP 4 - Finally, the transformed data ('results') are printed in the console. 
        for (int i=0; i<10; i++){
            System.out.println(mjdbc.getEntity("results").getData().get(i));
        }
    }                  
}
