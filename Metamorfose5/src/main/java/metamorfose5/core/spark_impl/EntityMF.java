/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.spark_impl;

import java.io.Serializable;

/**
 *
 * @author Evandro
 */
public class EntityMF<D> implements Serializable {
    private String name;
    private D data;
    
    public EntityMF(String name){
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
    
    public void setData(D data) {
        this.data = data;
    }

    public D getData() {
        return this.data;
    }

    public String toString(){
        return this.getName();
    }
}
