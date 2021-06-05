/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Evandro
 */
public class FieldMapping implements Serializable {
    private List<Field> sourceFields;
    private List<Field> targetFields;    
    private UDF UDF;
    private FieldMapping nested;
    private FieldMapping parent;
    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    
    public List<Field> getSourceFields() {
        return sourceFields;
    }

    public void setSourceFields(List<Field> sourceFields) {
        this.sourceFields = sourceFields;
    }

    public List<Field> getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(List<Field> targetFields) {
        this.targetFields = targetFields;
    }

    public UDF getUDF() {
        return UDF;
    }

    public void setUDF(UDF UDF) {
        this.UDF = UDF;
    }

    public FieldMapping getNested() {
        return this.nested;
    }
    
    public boolean hasNested(){
        if (this.nested != null){
            return true;
        } else {
            return false;
        }
    }
    
    // **************************************************************************
    // Construtor privado.
    private FieldMapping(){
        sourceFields = new ArrayList<>();
        targetFields = new ArrayList<>();
        properties = new Properties();
    }
    
    public static FieldMapping builder(){
        return new FieldMapping();
    }
    
    public FieldMapping addSourceField(Field field){
        getSourceFields().add(field);
        return this;
    }
    
    public FieldMapping addSourceField(String fieldName, String dataType){        
        getSourceFields().add(new Field(fieldName, dataType));
        return this;
    }
    
    public FieldMapping addTargetField(Field field){
        getTargetFields().add(field);
        return this;
    }
    
    public FieldMapping addTargetField(String fieldName, String dataType){
        getTargetFields().add(new Field(fieldName, dataType));
        return this;
    }
        
    public FieldMapping UDF(UDF udf){
        setUDF(udf);
        return this;
    }
          
    public FieldMapping UDF(String jsFunctionName, String jsFunctionCode){
        setUDF(new UDF(jsFunctionName, jsFunctionCode));
        return this;
    }
    
    public FieldMapping UDF(String javaClassName){
        setUDF(new UDF(javaClassName));
        return this;
    }
    
    public FieldMapping Properties(String key, Object value){
        getProperties().put(key, value);
        return this;
    }
    
    public FieldMapping casting(String sourceField, String sourceDataType, String targetField, String targetDataType){
        getSourceFields().add(new Field(sourceField, sourceDataType));
        getTargetFields().add(new Field(targetField, targetDataType));
        return this;
    }
    
    public FieldMapping nested(){
        this.nested = new FieldMapping();
        this.nested.parent = this;
        return this.nested;
    }
    
    public FieldMapping get(){  
        FieldMapping temp = this;
        while (temp.parent != null){
            temp = temp.parent;
        }
        return temp;
    }     

    @Override
    public String toString() {        
        String msg = "FieldMapping{" + "sourceFields=" + sourceFields + ", targetFields=" + targetFields + ", UDF=" + UDF;   
        if (nested != null){
            msg += "\n"+ putTabs(tabIndex);
            tabIndex++;
            msg += nested.toString(); // chamada recursiva...            
        }
        msg += '}';
        tabIndex = 1;
        
        return msg;
    }
    private static int tabIndex = 1; // usado apenas para formatação da função toString().
    
    // Retorna um cadeia de '\t' de acordo com tabIndex. Serve para formatar uma determinada String.
    private String putTabs(int tabIndex){
        String tabs = "";
        for (int i=1; i<=tabIndex; i++){
            tabs += '\t';
        }
        return tabs;
    }
    
}
