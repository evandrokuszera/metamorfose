/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.java_udf.impl;

import metamorfose5.map.FieldMapping;
import metamorfose5.map.java_udf.JavaUDF;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class ClasseIlustrativaMap_apresentacao implements JavaUDF {

    @Override
    public Object executeUDF(Object values) {
        JSONObject input = new JSONObject(values.toString());
        JSONObject output = new JSONObject();
        // Begin: User Transformation Logic
        String fname = input.getString("fname");
        String lname = input.getString("lname");
        output.put("name", fname+" "+lname);
        // End: User Transformation Logic
        return output.toString();
    }

    @Override
    public void setFieldMapping(FieldMapping fieldMapping) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
