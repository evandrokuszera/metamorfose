/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testes;

import metamorfose.converters.Converter;
import metamorfose.main.Framework;
import metamorfose.map.EntityMap;
import metamorfose.map.FieldMap;
import metamorfose.model.Entity;
import metamorfose.transformations.udf.UserDefinedFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Evandro
 */
public class ComoUsarUDFparaTransformacoes {

    public static void main(String[] args) {

        EntityMap entityMap = new EntityMap("CSV_to_DATASET", new Entity("DATASET"), new Entity("CSV"), new Converter());
        entityMap.mapFields("ano", "INT", "ANO_CENSO", "STRING", "","");
        entityMap.mapFields("sexo", "INT", "TP_SEXO", "STRING", "", "sexoUDF");
        entityMap.mapFields("tipo_convenio_pp", "INT", "ID_TIPO_CONVENIO_PODER_PUBLICO", "STRING", "", "convenioPoderPublicoTransformationUDF");
        entityMap.mapFields("exclusiva_especial", "INT", "FK_COD_MOD_ENSINO", "STRING", "", "mod_ensinoUDF");

        Dataset<Row> records = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV");
        
        Dataset<Row> results = null;
        
        // Aplicando UDFs sobre o dataset
        UserDefinedFunctions.register_udfs();
                
        for (FieldMap f : entityMap.getFieldMappings()) {
            if (f.getUdf() != null) {
                // Registra UDF
                Framework.getSparkSession().udf().register(
                                                    f.getUdf(), 
                                                    UserDefinedFunctions.getUDFbyName(f.getUdf()), 
                                                    DataTypes.IntegerType);
                // Aplica UDF sobre dataset
                results = records.withColumn(f.getSourceFieldName(), functions.callUDF(f.getUdf(), records.col(f.getSourceFieldName())));
                
            }
        }
                
        results.show(false);

//        JavaRDD<Row> RDDrecords = records.toJavaRDD();
//
//        JavaRDD<Row> results = RDDrecords.map(record -> {
//
//            List<Object> values = new ArrayList<>();
//
//            for (FieldMap f : entityMap.getFieldMappings()) {
//
//                if (f.getUdf() != null) {
//                    values.add(record.getAs(f.getSourceFieldName()));
//                } else {
//                    values.add(entityMap.getConverter().convertValues(f.getTargetField(), record.getAs(f.getSourceFieldName())));
//                }
//
//            }
//
//            return RowFactory.create(values.toArray());
//        });
//
//        StructType schema = SparkSchemaCreator.createSchemaFromEntity(entityMap.getTargetEntity());
//
//        Dataset<Row> datasetFinal = Framework.getSparkSession().createDataFrame(results, schema);
//
//        datasetFinal.show(false);
    }
    
    

}
