/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map;

/**
 *
 * @author Evandro
 */
public enum UDFType {
    JAVA(1), JAVASCRIPT(2);
    
    private final int type;
    
    UDFType(int typeValue){
        type = typeValue;
    }
    
    public int getTypeValue(){
        return type;
    }
}
