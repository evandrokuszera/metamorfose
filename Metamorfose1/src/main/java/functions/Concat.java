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
public class Concat implements Transformation {
    @Override
    public Object executeTransformation(Object[] values) {            
        return values[0] + " " + values[1];
    }
}
