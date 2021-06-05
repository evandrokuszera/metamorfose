/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class Field<F> implements Serializable{
    private String name;
    private F type;
    private Object constantValue;

    public Field(String name, F type) {
        this.name = name;
        this.type = type;
    }
    
    public Field(String name, F type, Object constantValue) {
        this.name = name;
        this.type = type;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setDataType(F type) {
        this.type = type;
    }

    public F getDataType() {
        return this.type;
    }

    public Object getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }
    
    public String toString(){
        return this.name + ":" + this.type.toString();
    }
    
}
