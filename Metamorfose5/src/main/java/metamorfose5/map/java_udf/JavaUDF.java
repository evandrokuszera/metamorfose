/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.java_udf;

import metamorfose5.map.FieldMapping;

/**
 *
 * @author Evandro
 */
public interface JavaUDF {
    public Object executeUDF(Object values);
    public void setFieldMapping(FieldMapping fieldMapping);
}
