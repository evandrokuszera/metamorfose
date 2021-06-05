/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.util;

import java.io.Serializable;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class MetamorfoseUtil implements Serializable {

    // Cria um campo JSON com nome fieldName conforme conteúdo da variável fieldValue
    // O campo é adicionado no objeto outputJSON. O campo pode ser: simples, objeto embutido ou array de objetos.
    public static void creatingJSONField(String fieldName, String fieldValue, JSONObject outputJSON) {
        
        // Esses dois testes garantem que todos os objetos JSON terão os mesmos campos, mesmo que o campo tenha valor null.
        if (fieldValue == null) {
            outputJSON.put(fieldName, fieldValue);
        }
        else if (fieldValue.length() == 0) {
            outputJSON.put(fieldName, fieldValue);
        }
        
        else if (fieldValue.trim().charAt(0) == '['){
            try {            
                JSONArray jsonArray = new JSONArray(fieldValue);
                outputJSON.put(fieldName, jsonArray);
                return;
            } catch (JSONException ex) { }
        } else if (fieldValue.trim().charAt(0) == '['){ //'['
            try {
                JSONObject jsonObj = new JSONObject(fieldValue);
                outputJSON.put(fieldName, jsonObj);
                return;
            } catch (JSONException ex) { }
        } else {
            // Se chegou até aqui, então fieldValue é um tipo primitivo...
            outputJSON.put(fieldName, fieldValue);
        }
    }

    // Recebe um objeto Row (Spark) e transforma em objeto JSON com todos campos.
    public static JSONObject creatingJSONObject(Row rowObj) {
        JSONObject jsonObj = new JSONObject();
        
        for (String fieldName : rowObj.schema().fieldNames()) {            
            String fieldValue = "";  // ****************************************   SERÁ QUE DEVO USAR UMA CONSTANTE PARA SINALIZAR VALOR NULO, POR EXEMPLO: 'METAMORFOSE_NULL'
            if (rowObj.getAs(fieldName) != null){
                fieldValue = rowObj.getAs(fieldName).toString();
                creatingJSONField(fieldName, fieldValue, jsonObj);
            } else {
                jsonObj.put(fieldName, fieldValue);
            }
        }

        return jsonObj;
    }

//    // Recebe um objeto JSON e mapeamentos (EntityMap) e retorna um objeto Row (Spark) com os campos nível 1 do objeto JSON.
//    public static Row creatingRowObject(JSONObject jsonObj, EntityMap<Dataset<Row>, DataType> mappings) {
//        List<Object> outputValues = new ArrayList<>();
//        
//        for (FieldMap<DataType> fieldMap : mappings.getFieldMaps()) {
//            for (Field field : fieldMap.getTargetFields()) {
//                if (jsonObj.get(field.getName()) instanceof JSONObject) {
//                    outputValues.add(jsonObj.getJSONObject(field.getName()).toString());
//                } else if (jsonObj.get(field.getName()) instanceof JSONArray) {
//                    outputValues.add(jsonObj.getJSONArray(field.getName()).toString());
//                } else {
//                    //outputValues.add(jsonObj.getString(field.getName())); // se o campo não for String lança exception!
//                    outputValues.add(jsonObj.get(field.getName()).toString());
//                }
//            }
//        }
//        return RowFactory.create(outputValues.toArray());
//    }
//    
//    // Recebe mapeamentos (EntityMap) e retorna um schema com base nos campos alvo (Target Fields)
//    // Esse schema é usado para criar um Dataset<Row>.
//    public static StructType createDatasetSchema(EntityMap<Dataset<Row>, DataType> mappings){
//        List<StructField> fields = new ArrayList<>();
//
//        for (FieldMap<DataType> fieldMap : mappings.getFieldMaps()) {
//            StructField field = DataTypes.createStructField(
//                                            fieldMap.getTargetFields().get(0).getName(),
//                                            DataTypes.StringType,
//                                            true);
//            fields.add(field);
//        }
//        StructType schema = DataTypes.createStructType(fields);        
//        return schema;
//    }

}
