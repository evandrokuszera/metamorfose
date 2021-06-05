/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.script;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class Script implements Serializable {
    private String description;
    private String functionName;
    private String functionCode;

    public Script(String functionName, String functionCode) {
        this.functionName = functionName;
        this.functionCode = functionCode;
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

    public String getFunctionCode() {
        return functionCode;
    }

    public void setFunctionCode(String functionCode) {
        this.functionCode = functionCode;
    }    
}
