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
public class ConcatTransformation implements Transformation {

    @Override
    public Object executeTransformation(Object[] value) {
        
        if (value == null) return null;
        if (value[0] == null) return null;
        
        String result = "";
        for (int i=0; i<value.length; i++){
            if (value[i] != null){
                result += String.valueOf(value[i]);
            }
        }
        
        return result;
    }
    
}
