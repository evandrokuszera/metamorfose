/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map;

import metamorfose5.map.script.Script;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Evandro
 */
public class Mapping implements Serializable {
    private String name;    
    private List<FieldMapping> fieldMappings = new ArrayList<>(); 
        
    public void mapFields(FieldMapping fieldMapping){        
        fieldMappings.add(fieldMapping);
    }
       
    public List<FieldMapping> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(List<FieldMapping> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    // Retorna lista de scripts dos FieldMappings.
    public List<Script> getScripts(){
        List<Script> scripts = new ArrayList();
        
        for (FieldMapping fm : fieldMappings){
            
            if (fm.getUDF() != null){
                if (fm.getUDF().getUdfType()==UDFType.JAVASCRIPT){
                    Script script = new Script(fm.getUDF().getJsFunctionName(), fm.getUDF().getJsFunctionCode());
                    scripts.add(script);
                }
            }
            
            // recuperando os scripts de nested field mappings
            FieldMapping nested_fm = fm.getNested();
            while (nested_fm != null){
                if (nested_fm.getUDF() != null){
                    if (fm.getUDF().getUdfType()==UDFType.JAVASCRIPT){
                        Script script = new Script(nested_fm.getUDF().getJsFunctionName(), nested_fm.getUDF().getJsFunctionCode());
                        scripts.add(script);
                    }
                }
                nested_fm = nested_fm.getNested();
            }
        }
        
        return scripts;
    }
   
    public Script getScriptyByFunctionName(String functionName){
        for (Script s : this.getScripts()){
            if (s.getFunctionName().equals(functionName)){
                return s;
            }
        }
        return null;
    }
    
    public String toString(){
        String msg = "Mapping: " + this.name + "[\n";   
        
        for (FieldMapping m : this.fieldMappings){
            msg += "  " + m.toString();
            msg += "\n";
        }
        
        msg += "]";
        
        return msg;
    }
}
