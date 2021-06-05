/*
 * Os sourceFields são encapsulados como documento JSON com o nome dado pela variável targetField.
 * A variável nesting_key é usada PARA CRIAR um sub objeto JSON.
 * Contexto: realizar aninhamento múltiplo de dados dentro de um documento JSON que representa uma família de colunas do modelo NoSQL colunar.
 */
package metamorfose5.map.java_udf.impl;

import metamorfose5.map.Field;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.java_udf.JavaUDF;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class CreateNestedNestedJSONObject implements JavaUDF, Serializable {
    private String nesting_key = "";
    private String targetField = "";
    private List<String> sourceFields = new ArrayList<>();
    
    @Override
    public void setFieldMapping(FieldMapping fieldMapping) {
        // Recuperando os campos de origem do mapeamento.
        for (Field f : fieldMapping.getSourceFields()){
            sourceFields.add( f.getName() );
        }
        // Recuperando o campo destino, assumindo que tem somente um campo destino.
        targetField = fieldMapping.getTargetFields().get(0).getName();
        
        nesting_key = fieldMapping.getProperties().getProperty("nesting_key");
    }

    @Override
    public Object executeUDF(Object values) {
        JSONObject input = new JSONObject(values.toString());
        JSONObject output = new JSONObject();
        
        if (!input.getJSONObject("object1").has(targetField)){
            String key = input.getJSONObject("object1").getString(nesting_key);
            
            output.put(targetField, new JSONObject());
            output.getJSONObject(targetField).put(key, new JSONObject());
            
            for (String sField : sourceFields){
                output.getJSONObject(targetField).getJSONObject(key).put(sField, input.getJSONObject("object1").getString(sField));
            }        
            
        } else {
            output.put(targetField, input.getJSONObject("object1").getJSONObject(targetField));
        }
        
        if (input.has("object2")){
            String key = input.getJSONObject("object2").getString(nesting_key);
            output.getJSONObject(targetField).put(key, new JSONObject());
            for (String sField : sourceFields){
                output.getJSONObject(targetField).getJSONObject(key).put(sField, input.getJSONObject("object2").getString(sField));
            }            
        }
        return output.toString();
    }
}





//    public static String nested_nested(Object values){
//        JSONObject input = new JSONObject(values.toString());
//        JSONObject output = new JSONObject();
//        
//        if (!input.getJSONObject("object1").has("orderlines_cf")){
//            String key = input.getJSONObject("object1").getString("orderlineid");
//            
//            output.put("orderlines_cf", new JSONObject());
//            output.getJSONObject("orderlines_cf").put(key, new JSONObject());
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderlineid", input.getJSONObject("object1").getString("orderlineid"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderid", input.getJSONObject("object1").getString("orderid"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("prod_id", input.getJSONObject("object1").getString("prod_id"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("quantity", input.getJSONObject("object1").getString("quantity"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderlinedate", input.getJSONObject("object1").getString("orderlinedate"));            
//            
//        } else {
//            output.put("orderlines_cf", input.getJSONObject("object1").getJSONObject("orderlines_cf"));
//        }
//        
//        if (input.has("object2")){
//            String key = input.getJSONObject("object2").getString("orderlineid");
//            output.getJSONObject("orderlines_cf").put(key, new JSONObject());
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderlineid", input.getJSONObject("object2").getString("orderlineid"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderid", input.getJSONObject("object2").getString("orderid"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("prod_id", input.getJSONObject("object2").getString("prod_id"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("quantity", input.getJSONObject("object2").getString("quantity"));
//            output.getJSONObject("orderlines_cf").getJSONObject(key).put("orderlinedate", input.getJSONObject("object2").getString("orderlinedate"));
//        }
//        return output.toString();
//    }
