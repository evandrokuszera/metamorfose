/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.gui.model;

import metamorfose.map.EntityMap;
import org.apache.spark.sql.Dataset;

/**
 *
 * @author Evandro
 */
public class SparkDatasetInstance<T> {
    
    private Dataset<T> dataset;
    private String sourceType;
    private String tempViewName;
    private EntityMap entityMap;

    public SparkDatasetInstance(Dataset<T> dataset, String sourceType, String tempViewName) {
        this.dataset = dataset;
        this.sourceType = sourceType;
        this.tempViewName = tempViewName;
    }
    
    public Dataset<T> getDataset() {
        return dataset;
    }

    public void setDataset(Dataset<T> dataset) {
        this.dataset = dataset;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTempViewName() {
        return tempViewName;
    }

    public void setTempViewName(String tempViewName) {
        this.tempViewName = tempViewName;
    }  

    public EntityMap getEntityMap() {
        return entityMap;
    }

    public void setEntityMap(EntityMap entityMap) {
        this.entityMap = entityMap;
    }
    
    @Override
    public String toString() {
        return this.tempViewName + " (" + this.sourceType + ")";
    }
}
