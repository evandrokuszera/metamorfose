/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.script;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class Util implements Serializable {

    // Cria um campo JSON com nome fieldName conforme conteúdo da variável fieldValue
    // O campo é adicionado no objeto outputJSON. O campo pode ser: simples, objeto embutido ou array de objetos.
    public static void creatingJSONField(String fieldName, String fieldValue, JSONObject outputJSON) {
        
        if (fieldValue.trim().charAt(0) == '['){
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
    
    public static ArrayList<String> getArrayList(String[] arrayFields){
        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i<arrayFields.length; i++){
            fields.add(arrayFields[i]);
        }
        return fields;
    }

   
}
