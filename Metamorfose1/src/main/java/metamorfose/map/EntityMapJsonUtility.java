/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.map;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import metamorfose.converters.Converter;
import metamorfose.model.Entity;
import metamorfose.transformations.javascript.Script;
import metamorfose.transformations.javascript.TransformationType;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class EntityMapJsonUtility {
    
    public static void saveToJSON(EntityMap entityMap, String fileName){
        
        // ###############################################################################
        // Passo 1: Cria objeto JSON a partir dos dados do EntityMap        
        JSONObject map = new JSONObject();
        
        // Entity
        map.put("name", entityMap.getName());
        map.put("annotations", entityMap.getAnnotations());
        map.put("source_name", entityMap.getSourceEntity().getName());
        map.put("target_name", entityMap.getTargetEntity().getName());
        
        // Fields
        JSONArray fieldMapArray = new JSONArray();
        
        // lê mapeamento dos campos
        for (FieldMap fm : entityMap.getFieldMappings()){
            JSONObject fieldMap = new JSONObject();
            fieldMap.put("source_field", fm.getSourceFieldName());
            fieldMap.put("source_datatype", entityMap.getSourceEntity().getFieldByName(fm.getSourceFieldName()).getDataType());
            fieldMap.put("target_field", fm.getTargetFieldName());
            fieldMap.put("target_datatype", entityMap.getTargetEntity().getFieldByName(fm.getTargetFieldName()).getDataType());
            fieldMap.put("transformer", fm.getTransformationName());
            //fieldMap.put("udf", fm.getUdf());
            fieldMap.put("transformationType", fm.getTransformationType());
            
                                   
            fieldMapArray.put(fieldMap);
        }
        // adiciona vetor de mapeamentos entre campos no array mappings        
        map.put("mappings", fieldMapArray);
        
        // Scripts
        JSONArray scriptsArray = new JSONArray();
        
        for (Script s : entityMap.getScripts()){
            JSONObject script = new JSONObject();
            script.put("function_name", s.getFunctionName());
            script.put("function_impl", s.getFunctionImplementation());
            
            scriptsArray.put(script);
        }
        // adiciona os scripts javascript no array scripts
        map.put("scripts", scriptsArray);
        
        
        // ###############################################################################
        // Passo 2: Persistir JSON no disco.
        saveJSONObjectToFile(map, fileName);
    }
    
    public static EntityMap loadFromJSON(String fileName){
        // ###############################################################################
        // Passo 1: lendo json do disco  
        JSONObject jsonObject = loadJSONObjectFromFile(fileName);
        
        // ###############################################################################
        // Passo 2: criando objeto EntityMap        
        Entity source = new Entity(jsonObject.get("source_name").toString());
        Entity target = new Entity(jsonObject.get("target_name").toString());
        
        EntityMap em = new EntityMap(
                            jsonObject.get("name").toString(),
                            target,
                            source,
                            null);
        
        //em.setAnnotations(jsonObject.get("annotations").toString());
                            
        JSONArray fieldMapArray = jsonObject.getJSONArray("mappings");
        // Criando objetos FieldMap
        for (int i=0; i<fieldMapArray.length(); i++){
            JSONObject jsonField = fieldMapArray.getJSONObject(i);
//            em.mapFields(
//                    jsonField.get("target_field").toString(), 
//                    jsonField.get("target_datatype").toString(), 
//                    jsonField.get("source_field").toString(), 
//                    jsonField.get("source_datatype").toString(), 
//                    jsonField.get("transformer").toString(),
//                    jsonField.get("udf").toString()
//            );
            
            em.mapFields(
                    jsonField.get("target_field").toString(), 
                    jsonField.get("target_datatype").toString(), 
                    jsonField.get("source_field").toString(), 
                    jsonField.get("source_datatype").toString(), 
                    jsonField.get("transformer").toString(),
                    TransformationType.valueOf( jsonField.get("transformationType").toString() ),
                    "" 
            );            
        }
        
        // Pequena gambiara. O método acima em.mapFields(...) gera entradas no array Scripts quando TransformationType = JAVASCRIPT.
        // No entanto, é o trecho de código a seguir que carrega CORRETAMENTE os scripts.
        em.getScripts().clear();
        
        JSONArray scriptsArray = jsonObject.getJSONArray("scripts");
        // Criando objetos Script
        for (int i=0; i<scriptsArray.length(); i++){
            JSONObject jsonField = scriptsArray.getJSONObject(i);
            Script script = new Script(
                                        jsonField.get("function_name").toString(), 
                                        jsonField.get("function_impl").toString()
            );   
            em.getScripts().add(script);
        }
        
        return em;
    }
    
    // Funções de Apoio : salvar em disco
    private static void saveJSONObjectToFile(JSONObject obj, String fileName){
        FileWriter fw;
        try {
            fw = new FileWriter(fileName);
            obj.write(fw);
            fw.close();
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }
    
    // Funções de Apoio : ler do disco
    private static JSONObject loadJSONObjectFromFile(String fileName){
        String jsonText = "";
        
        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            
            String linha = br.readLine();
            while (linha != null){
                jsonText += linha;
                linha = br.readLine();                
            }
            fr.close();            
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        return new JSONObject(jsonText);
    }
    
    
    //*****************************************************************************************************************
    //*****************************************************************************************************************
    // TESTES PARA PERSISTIR UM ENTITYMAP E CARREGAR UM ENTITYMAP DO DISCO    
    public static void main(String[] args) {
        
        test_01_persist_EntityMap_to_JSON();
        
        test_02_load_EntityMap_from_JSON();
        
    }
    
    public static void test_02_load_EntityMap_from_JSON() {
        EntityMap entityMap = loadFromJSON("d:\\teste.json");        
        
        // imprimindo mapeamentos...
        System.out.println("TARGET_ENTITY : SOURCE_ENTITY : TRANSFORMATION");
        for (FieldMap m : entityMap.getFieldMappings()){
            System.out.println(m.getTargetFieldName() + " : " + m.getSourceFieldName() + " : " + m.getTransformationName());
        }
    }
    
    public static void test_01_persist_EntityMap_to_JSON() {
        Entity source = new Entity();
        Entity target = new Entity();
        
        source.setName("Entity1");
        source.addField("id", "INT");
        source.addField("name", "STRING");
        source.addField("salary", "DOUBLE");
        
        target.setName("Entity2");
        target.addField("id_emp", "INT");
        target.addField("name_emp", "STRING");
        target.addField("salary_emp", "DOUBLE");
        
        EntityMap mapping = new EntityMap("TESTE", target, source, new Converter());
        
        mapping.mapFields("id_emp", "id", null);
        mapping.mapFields("name_emp", "name", null);
        mapping.mapFields("salary_emp", "salary", null);
        
        saveToJSON(mapping, "d:\\teste.json");
        
    }
    
}
