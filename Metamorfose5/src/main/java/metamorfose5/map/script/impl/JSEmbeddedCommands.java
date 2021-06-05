/*
 * This class provides some ready Javascript transformation scripts.
 *
 * Examples:
 *  - getManyEmbeddedJSTransformation() return JS script to create an array of documents of side many and embed this array in document of side one. Using it with MapReduceTransformation.
 *  - getOneEmbeddedJSTransformation() return JS script to create an embed document of side one in document of side many. Using it with MapTransformation.
 */
package metamorfose5.map.script.impl;

import metamorfose5.map.FieldMapping;
import java.util.ArrayList;

/**
 *
 * @author Evandro
 */
public class JSEmbeddedCommands {
    
    // Embute os valores em um JSON Document
    // Função que pode chamá-la: MapTransformation
    public static String getOneEmbeddedScript(String oneSideEntityName, ArrayList<String> oneSideFieldNames){
        
        String functionCode = "";
        functionCode += "function docEmbedded(values) {";
        functionCode += " input = JSON.parse(values);";
        functionCode += " output = JSON.parse('{}');";                                                   
        functionCode += " output."+oneSideEntityName+" = JSON.parse('{}');";      
        for (String field : oneSideFieldNames){
        functionCode += " output."+oneSideEntityName+"."+field+" = input."+field+";";
        }                                                        
        functionCode += " return JSON.stringify(output);";
        functionCode += "}";        
        return functionCode;
    }
    
    // Embute os valores em um array de Document
    // Função que pode chamá-la: MapReduceTransformation
    public static String getManyEmbeddedScript(String manySideEntityName, ArrayList<String> manySideFieldNames){
        String functionCode = "";
        functionCode += "function arrayEmbedded(values) {";
        functionCode += " input = JSON.parse(values);";
        functionCode += " output = JSON.parse('{}');";
        functionCode += " if(!input.object1.hasOwnProperty('"+manySideEntityName+"')){";
        functionCode += "      embedded_object = JSON.parse('{}');";
        for (String field : manySideFieldNames){
        functionCode += "      embedded_object."+field+" = input.object1."+field+";";
        }        
        functionCode += "      if (input.object2 != null)";
        functionCode += "           output."+manySideEntityName+" = [embedded_object, input.object2];";
        functionCode += "      else";
        functionCode += "           output."+manySideEntityName+" = [embedded_object];";   
        functionCode += " } else {";
        functionCode += "      output = input.object1;";
        functionCode += "      if (input.object2 != null) output."+manySideEntityName+".push(input.object2);";                  
        functionCode += " }";
        functionCode += " return JSON.stringify(output);";
        functionCode += "}";
        return functionCode;
    }
    
    // Embute os valores em um JSON Document, onde os valores são achatados (flattened) pelo campo id da entidade embutida
    // Função que pode chamá-la: MapReduceTransformation
    // Dependendo do contexto, manySideEntityName pode ser visto como o nome da Família de Colunas que será aninhada a uma entidade principal.
    public static String getManyNestingScript(String manySideEntityName, ArrayList<String> manySideFieldNames, String flattenedKey){
        String functionCode = "";
        functionCode += "function "+manySideEntityName+"_manyNesting(values) {";
        functionCode += " input = JSON.parse(values);";
        functionCode += " output = JSON.parse('{}');";                          
        functionCode += " if(!input.object1.hasOwnProperty('"+manySideEntityName+"')){";
        functionCode += "     nesting_key = input.object1."+flattenedKey+";";              
        functionCode += "     output."+manySideEntityName+" = JSON.parse('{}');";
        for (String field : manySideFieldNames){
        functionCode += "     output."+manySideEntityName+"[nesting_key+'_"+field+"'] = input.object1."+field+";";
        }        
        functionCode += " } else {";
        functionCode += "     output."+manySideEntityName+" = input.object1."+manySideEntityName+";";
        functionCode += " }";
        functionCode += " if(input.object2 != null){";
        functionCode += "      nesting_key = input.object2."+flattenedKey+";";      
        for (String field : manySideFieldNames){
        functionCode += "      output."+manySideEntityName+"[nesting_key+'_"+field+"'] = input.object2."+field+";";
        }
        functionCode += " }";          
        functionCode += " return JSON.stringify(output);";
        functionCode += "}";               
        return functionCode;
    } 
    
    // Embute os valores em um JSON Document. Esse script é similar ao método getOneEmbeddedScript. 
    // Porém, deve ser usada em MapReduceTransformation, ele tem os objetos object1 e object2.
    // Função que pode chamá-la: MapReduceTransformation
    public static String getDocNestingScript(String oneSideEntityName, ArrayList<String> oneSideFieldNames){
        String functionCode = "";
        functionCode += "function oneNesting(values) {";
        functionCode += " input = JSON.parse(values);";
        functionCode += " output = JSON.parse('{}');";
        functionCode += " if (!input.object1.hasOwnProperty('"+oneSideEntityName+"')){";
        functionCode += "     output."+oneSideEntityName+" = JSON.parse('{}');";
        for (String field : oneSideFieldNames){
        functionCode += "     output."+oneSideEntityName+"."+field+" = input.object1."+field+";";
        }
        functionCode += " } else {";
        functionCode += "     output."+oneSideEntityName+" = input.object1."+oneSideEntityName+";";
        functionCode += " }";                                                     
        functionCode += " return JSON.stringify(output);";
        functionCode += "}";
        return functionCode;
    }
    
    
    
    
    
    
    
    
    // Função para criar JS para criar um array de documentos embutidos
    //  Recebe um FieldMapping e extrai as informações para criar o JS.
    //  Considera que o FieldMapping está estruturado da seguinte forma:
    //      - Tem um ou mais source fields (campos que será embutidos).
    //      - Tem APENAS um target field (nome do array de documentos embutidos).
    public static String getJS_embedded_array(FieldMapping fm){
        ArrayList<String> embeddedFields = new ArrayList();
        
        for (int i=0; i<fm.getSourceFields().size(); i++){
            embeddedFields.add(fm.getSourceFields().get(i).getName());
        }
        
        return getManyEmbeddedScript(fm.getTargetFields().get(0).getName(), embeddedFields);        
    }
    
}