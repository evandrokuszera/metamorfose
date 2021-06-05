/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package functions;

import metamorfose.transformations.Transformation;

/**
 *
 * @author Evandro
 */
public class Sub implements Transformation{
    @Override
    public Object executeTransformation(Object[] values) {
        double v1 = 0;
        double v2 = 0;
        
        if (values!=null){
            if (values[0] != null)
                v1 = Double.parseDouble(values[0].toString());
            if (values[1]!=null)
                v2 = Double.parseDouble(values[1].toString());
        }        
        return v1-v2;
    }
}
