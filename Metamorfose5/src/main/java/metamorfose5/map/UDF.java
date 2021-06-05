/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class UDF implements Serializable {
    private String jsFunctionName;
    private String jsFunctionCode;
    private String javaClassName;
    private UDFType udfType;
        
    public UDF(String jsFunctionName, String jsFunctionCode) {
        this.jsFunctionName = jsFunctionName;
        this.jsFunctionCode = jsFunctionCode;
        this.udfType = UDFType.JAVASCRIPT;
    }
    
    public UDF(String javaClassName) {
        this.javaClassName = javaClassName;        
        this.udfType = UDFType.JAVA;
    }

    public String getJsFunctionName() {
        return jsFunctionName;
    }

    public void setJsFunctionName(String jsFunctionName) {
        this.jsFunctionName = jsFunctionName;
    }

    public String getJsFunctionCode() {
        return jsFunctionCode;
    }

    public void setJsFunctionCode(String jsFunctionCode) {
        this.jsFunctionCode = jsFunctionCode;
    }

    public String getJavaClassName() {
        return javaClassName;
    }

    public void setJavaClassName(String javaClassName) {
        this.javaClassName = javaClassName;
    }

    public UDFType getUdfType() {
        return udfType;
    }

    public void setUdfType(UDFType udfType) {
        this.udfType = udfType;
    }
    
    @Override
    public String toString() {
        if (udfType == UDFType.JAVASCRIPT)
            return "UDF{" + "jsFunctionName=" + jsFunctionName + ", jsFunctionCode=" + jsFunctionCode + ", udfType=" + udfType + '}';
        else 
            return "UDF{" + "javaClassName=" + javaClassName + ", udfType=" + udfType + '}';
    }
}
