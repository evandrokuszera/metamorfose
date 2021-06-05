/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.executor;

import metamorfose5.map.script.JavascriptEngine;
import metamorfose5.map.Field;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.Mapping;
import metamorfose5.map.UDFType;
import metamorfose5.map.java_udf.JavaUDF;
import metamorfose5.map.java_udf.JavaUDFLoader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class MappingExecutor implements Serializable {
    private Mapping mapping;
    private JavascriptEngine jsEngine;

    public MappingExecutor(Mapping mapping) {
        this.mapping = mapping;
        this.jsEngine = new JavascriptEngine();
        // carrega os scripts do objeto mapping no jsEngine.
        jsEngine.setScripts(this.mapping.getScripts());
    }

    public Mapping getMapping() {
        return mapping;
    }
    
    // Extrai field para transformação de dados.
    private JSONObject extractFields(JSONObject obj, FieldMapping fm) {
        
        JSONObject inputFields = new JSONObject();

        // for each field in FieldMapping, extractFields only source fields necessary to transform
        for (Field field : fm.getSourceFields()) {
            if (obj.has(field.getName())){
                inputFields.put(field.getName(), obj.get(field.getName()));
            } else {
                inputFields.put(field.getName(), "");
            }
        }
        
        return inputFields;
    }    
    
    // Executa transformação de dados (JScript ou Java) ou CASTING.
    private JSONObject transform(JSONObject inputFields, FieldMapping fm, JSONObject outputObj) {  
        
        JSONObject outputFieldsData = null;
        
        // Se tem UDF, então chamar código da UDF para transformar dados...
        if (fm.getUDF() != null){ 
            if (fm.getUDF().getUdfType() == UDFType.JAVASCRIPT){
                // Call JSEngine to transforming data.
                outputFieldsData = new JSONObject((String) jsEngine.executeTransformationJSON(fm.getUDF().getJsFunctionName(), inputFields));
            } else if (fm.getUDF().getUdfType() == UDFType.JAVA){
                // Load a javaUDF object.
                JavaUDF javaUDF = JavaUDFLoader.getJavaUDFClass(fm.getUDF().getJavaClassName());
                // Set current FieldMapping object to javaUDF object. This is utilized to pass some parameters to the UDF.
                javaUDF.setFieldMapping(fm);
                // Call executeUDF method from javaUDF object.
                outputFieldsData = new JSONObject((String) javaUDF.executeUDF(inputFields));
            }            
        // Senão, é um CASTING, então verificar se os dados estão ou não dentro de 'object1' e 'object2'. Caso afirmativo, apenas extrair os dados e passar a diante.
        } else {
            if (inputFields.has("object2")){ // 'object2' somente existe quando a transformação é proveniente de função mapreduce, caso contrário não há 'object2'.
                inputFields = inputFields.getJSONObject("object2");
            } else if (inputFields.has("object1")) { // 'object1' se chegar a entrar nesse IF, significa que é um MapReduce e que o registro não foi processado por não ter formado um grupo de registros.
                inputFields = inputFields.getJSONObject("object1");
            }
            
            // Mapeia campo de origem para campo de destino
            outputFieldsData = new JSONObject();
            outputFieldsData.put(fm.getTargetFields().get(0).getName(), 
                        inputFields.get(fm.getSourceFields().get(0).getName())
            );
        }
        
        // Filter Fields: build outputJSONObj according target fields specification provided by mapping specifications.
        for (int i = 0; i < fm.getTargetFields().size(); i++) {
            outputObj.put(
                    fm.getTargetFields().get(i).getName(), // Key
                    outputFieldsData.get(fm.getTargetFields().get(i).getName()) // Value
            );
        }
        
        // Return outputObj generated from transformations...
        return outputObj;
    }
    
      
    /////////////////////////////////////////////////////////////////////
    // Variações do método execute, com um ou dois parâmetro de entrada.
    /////////////////////////////////////////////////////////////////////

    // Usar para funções MAP ###############################################################################################
    public JSONObject execute(JSONObject inputObj) {

        JSONObject outputObj = new JSONObject();
        
        // For each field mapping, extractFields and transform data fields.
        for (FieldMapping fm : mapping.getFieldMappings()) {
            
            // extracting data fields from inputObj
            JSONObject inputFields = extractFields(inputObj, fm);
            // executing transformations
            transform(inputFields, fm, outputObj);
            
            // Nested field mapping, verifica existência e executa field mapping aninhados...
            FieldMapping nested_fm = fm.getNested();
            while (nested_fm != null){
                // extracting data fields from outputObj**
                JSONObject inputFieldsAninhado = extractFields(outputObj, nested_fm); // ** transformação sobre os dados do outputObj, no lugar do inputObj.
                // executing transformations
                transform(inputFieldsAninhado, nested_fm, outputObj);
                // verifica existência...
                nested_fm = nested_fm.getNested();
            }
            
        }
        
        // return output object
        return outputObj;
    }   
    
    // Usar para funções MAPREDUCE ########################################################################################
    public JSONObject execute(JSONObject inputObj1, JSONObject inputObj2) {

        JSONObject outputObj = new JSONObject();
        
        // For each field mapping, extractFields and transform data fields.
        for (FieldMapping fm : mapping.getFieldMappings()) {
            
            // extracting data fields from inputObj
            JSONObject inputFields = new JSONObject();
            inputFields.put("object1", inputObj1);        //inputFields.put("object1", extractFields(inputObj1, fm));
            if (inputObj2 != null) inputFields.put("object2", extractFields(inputObj2, fm));            
            
            // executing transformations
            transform(inputFields, fm, outputObj);
            
            // Nested field mapping, verifica existência e executa field mapping aninhados...
            FieldMapping nested_fm = fm.getNested();
            while (nested_fm != null){
                // extracting data fields from outputObj**
                JSONObject inputFieldsAninhado = extractFields(outputObj, nested_fm); // ** transformação sobre os dados do outputObj, no lugar do inputObj.
                // executing transformations
                transform(inputFieldsAninhado, nested_fm, outputObj);
                // verifica existência...
                nested_fm = nested_fm.getNested();
            }
            
        }
                
        // return output object
        return outputObj;
    }    
    
    // Usar para funções FLATMAP (Experimental) ##########################################################################
    // AINDA NÃO ESTÁ FUNCIONANDO!
    public List<JSONObject> executeFlat(JSONObject inputObj) {
        
        List<JSONObject> outputObjList = new ArrayList<>();
        
        JSONObject outputObj = new JSONObject();
        
        // For each field mapping, extractFields and transform data fields.
        for (FieldMapping fm : mapping.getFieldMappings()) {
            
            // extracting data fields from inputObj
            JSONObject inputFields = extractFields(inputObj, fm);
            // executing transformations
            transform(inputFields, fm, outputObj);
            
            // Nested field mapping, verifica existência e executa field mapping aninhados...
            FieldMapping nested_fm = fm.getNested();
            while (nested_fm != null){
                // extracting data fields from outputObj
                JSONObject inputFieldsAninhado = extractFields(outputObj, nested_fm);
                // executing transformations
                transform(inputFieldsAninhado, nested_fm, outputObj);
                // verifica existência...
                nested_fm = nested_fm.getNested();
            }
            
        }        
        
        // return output object
        //return outputObj; 
        
        // Experimental... não sei se ficou genérico e extensível de forma suficente.
        String targetField = mapping.getFieldMappings().get(0).getTargetFields().get(0).getName();
        
        for (int i=0; i<outputObj.getJSONArray(targetField).length(); i++){
            outputObjList.add(outputObj.getJSONArray(targetField).getJSONObject(i));
        }
        
        return outputObjList;
    }
}