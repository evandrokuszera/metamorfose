/*
 * Encapsula os sourceFields dentro dentro de um array de objetos embutidos.
 * A variável targetFieldName especifica o nome do array de objetos embutidos.
 * Contexto: usando para embutir o lado N de um relacionamento como um array no objeto do lado 1.
 */
package metamorfose5.map.java_udf.impl;

import metamorfose5.map.Field;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.java_udf.JavaUDF;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class CreateJSONArray implements JavaUDF, Serializable {
    private String targetFieldName = "";
    private List<String> sourceFields = new ArrayList();
    
    @Override
    public void setFieldMapping(FieldMapping fieldMapping) {
        // O campos de origem serão os campos do objeto embutido no array que será gerado.
        for (Field f : fieldMapping.getSourceFields()){
            sourceFields.add( f.getName() );
        }
        // O campo de destino é o nome do array no objeto que será gerado.
        targetFieldName = fieldMapping.getTargetFields().get(0).getName();
    }
    
    @Override
    public Object executeUDF(Object values) {   
        JSONObject input = new JSONObject(values.toString());
        JSONObject output = new JSONObject();
        
        if (!input.getJSONObject("object1").has(targetFieldName)){            
            // criando o array...
            output.put(targetFieldName, new JSONArray());
            // criando o objeto que será inserido no array
            JSONObject embeddedObj = new JSONObject();            
            for (String fieldName : sourceFields){
                embeddedObj.put(fieldName, input.getJSONObject("object1").get(fieldName));
            }
            // insere objeto criado no array
            output.getJSONArray(targetFieldName).put(embeddedObj);
            // se tem object2
            if (input.has("object2")){
                // insere no array
                output.getJSONArray(targetFieldName).put(input.getJSONObject("object2"));
            }
        } else {
            output = input.getJSONObject("object1");
            if (input.has("object2")){
                output.getJSONArray(targetFieldName).put(input.getJSONObject("object2"));
            }
        }  
        return output.toString();        
    }    
}
