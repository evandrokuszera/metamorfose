/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Evandro
 */
public class Entity implements Serializable {
    private String name;
    private List<Field> fields;
    
    public Entity(){
        this.fields = new ArrayList();
    }
    
    public Entity(String name){
        this();
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public Field getFieldByName(String name){
        for (Field f : this.fields){
            if (f.getName().equals(name)){
                return f;
            }
        }
        return null;
    }
    
    public void addField(String fieldName, String dataType){
        Field field = new Field();
        field.setName(fieldName);
        field.setDataType(dataType);
        
        this.fields.add(field);
    }

    @Override
    public String toString() {
        return this.fields.toString();
    }    
}
