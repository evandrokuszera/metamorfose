/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations;

/**
 *
 * @author Evandro
 */
public class ExclusivaEspecialTransformation implements Transformation {

    @Override
    public Object executeTransformation(Object[] value) {
        boolean result = false;
        
        if (value == null) return null;
        if (value[0] == null) return null;        
        
        int arg_as_int = Integer.parseInt(value[0].toString());
        
        switch (arg_as_int) {
            case 1:
                result = false;
                break;
            case 2:
                result = true;
                break;
            case 3:
                result = false;
                break;
        }

        return result;
    }
    
}
