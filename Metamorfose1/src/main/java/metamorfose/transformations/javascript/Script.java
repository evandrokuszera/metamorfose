/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations.javascript;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class Script implements Serializable {
    private String description;
    private String functionName;
    private String functionImplementation;

    public Script(String functionName, String functionImplementation) {
        this.functionName = functionName;
        this.functionImplementation = functionImplementation;
    }
    
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getFunctionImplementation() {
        return functionImplementation;
    }

    public void setFunctionImplementation(String functionImplementation) {
        this.functionImplementation = functionImplementation;
    }    
}
