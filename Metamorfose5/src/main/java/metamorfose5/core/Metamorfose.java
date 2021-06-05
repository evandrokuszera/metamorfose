/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core;

import java.util.List;

/**
 *
 * @author Evandro
 */
public interface Metamorfose<D,T,P,I> {
    //public void load(String entityName);
    public T load();
    public P persist();
    public I filter();
    
    public List<D> getEntities();
    public D getEntity(String entityName);   
}
