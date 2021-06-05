/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.converters;

import java.io.Serializable;
import metamorfose.model.Field;

/**
 *
 * @author Evandro
 */
public class Converter implements Serializable {
    
    // Nesse exemplo, essa função converte os valores do CSV para o tipo de dados do Schema Alvo (TargetEntity)
    public static Object convertValues(Field targetField, Object value){
        Object returnValue = null;
        
        if (value == null){
            return returnValue;
        }
        
        switch (targetField.getDataType()){
            case "STRING":
                returnValue = String.valueOf(value);
                break;
            case "BIGINT":
                returnValue = Long.parseLong( String.valueOf(value) );                
                break;
            case "INTEGER": case "INT": case "TINYINT":  case "SMALLINT":
                returnValue = Integer.parseInt( String.valueOf(value) );                
                break;
            case "DOUBLE":
                returnValue = Double.parseDouble( String.valueOf(value) );
                break;
            case "FLOAT":
                returnValue = Float.parseFloat( String.valueOf(value) );
                break;
            case "BOOLEAN":                
                if (Integer.parseInt( String.valueOf(value) ) == 0)
                    returnValue = Boolean.parseBoolean("false");
                else 
                    returnValue = Boolean.parseBoolean("true");
                break;
            case "DATE":
                // SEM SUPORTE. TODO!!!
                returnValue = String.valueOf(value);
                break;
        }     
        return returnValue;
    }
    
}
