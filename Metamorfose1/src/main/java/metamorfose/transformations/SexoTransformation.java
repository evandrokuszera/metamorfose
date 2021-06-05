/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class SexoTransformation implements Transformation, Serializable {

    @Override
    public Object executeTransformation(Object[] value) {
        Object returnValue = null;
        
        if (value == null) return null;
        if (value[0] == null) return null;
        
        // Exemplo de transformação...
        switch (value[0].toString().toUpperCase()) {
            case "F":
                returnValue = 2;
                break;
            case "M":
                returnValue = 1;
                break;
        }
        
        return returnValue;
    }

}
