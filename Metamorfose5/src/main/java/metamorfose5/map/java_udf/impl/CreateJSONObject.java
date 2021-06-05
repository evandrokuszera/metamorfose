/*
 * Encapsula os sourceFields como um objeto JSON.
 * A variável objectName representa o nome do objeto JSON gerado.
 * Contexto: usado para encapsular os campos de origem em um objeto JSON.
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
public class CreateJSONObject implements JavaUDF, Serializable {
    private String objectName = "";
    private List<String> sourceFields = new ArrayList();
    
    @Override
    public void setFieldMapping(FieldMapping fieldMapping) {
        // O campos de origem serão os campos do objeto embutido no array que será gerado.
        for (Field f : fieldMapping.getSourceFields()){
            sourceFields.add( f.getName() );
        }
        // O campo de destino é o nome do array no objeto que será gerado.
        objectName = fieldMapping.getTargetFields().get(0).getName();
    }
    
    @Override
    public Object executeUDF(Object values) { 
        JSONObject input = new JSONObject(values.toString());
        JSONObject output = new JSONObject();
        
        if (input.has("object1"))
            input = input.getJSONObject("object1");
        
        if (!input.has(objectName)){
            output.put(objectName, new JSONObject());
        
            for (String fieldName : sourceFields){
                output.getJSONObject(objectName).put(fieldName, input.get(fieldName));
            }
        } else {
            output.put(objectName, input.getJSONObject(objectName));
        }
        
        return output.toString();        
    }    
}




//    public static String oneNesting(Object values){
//        JSONObject input = new JSONObject(values.toString());
//        JSONObject output = new JSONObject();
//        
//        if (!input.getJSONObject("object1").has("order_cf")){
//            output.put("order_cf", new JSONObject());
//            output.getJSONObject("order_cf").put("orderdate", input.getJSONObject("object1").getString("orderdate"));
//            output.getJSONObject("order_cf").put("customerid", input.getJSONObject("object1").getString("customerid"));
//            output.getJSONObject("order_cf").put("tax", input.getJSONObject("object1").getString("tax"));
//            output.getJSONObject("order_cf").put("totalamount", input.getJSONObject("object1").getString("totalamount"));
//        } else {
//            output.put("order_cf", input.getJSONObject("object1").getJSONObject("order_cf"));
//        }
//        
//        return output.toString();
//    }
