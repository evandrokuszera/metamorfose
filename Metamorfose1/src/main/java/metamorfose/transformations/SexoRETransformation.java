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
public class SexoRETransformation implements Transformation, Serializable {

    @Override
    public Object executeTransformation(Object[] value) {
        Object returnValue = null;
        
        if (value == null) return null;
        if (value[0] == null) return null;
        
        // Exemplo de transformação...
        int intValue = (Integer) value[0];
        
        switch (intValue) {
            case 1:
                returnValue = "F";
                break;
            case 0:
                returnValue = "M";
                break;
        }
        
        return returnValue;
    }

}
