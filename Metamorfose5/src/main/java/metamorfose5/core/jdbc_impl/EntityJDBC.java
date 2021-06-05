/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.jdbc_impl;

import java.io.Serializable;
import org.json.JSONArray;

/**
 *
 * @author Evandro
 */
public class EntityJDBC implements Serializable {
    private String name;
    private JSONArray data;
    
    public EntityJDBC(String name){
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
    
    public void setData(JSONArray data) {
        this.data = data;
    }

    public JSONArray getData() {
        return this.data;
    }

    public String toString(){
        return this.getName();
    }
}
