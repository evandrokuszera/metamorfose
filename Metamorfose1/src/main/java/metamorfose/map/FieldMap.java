/*
 * Esta classe estabele o mapeamento entre campos de duas entidades.
 * Nesta versão os mapeamentos são estabelecidos pelo nome (String) dos campos da entidade de origem e destino.
 */
package metamorfose.map;

import java.io.Serializable;
import metamorfose.model.Field;
import metamorfose.transformations.javascript.TransformationType;

/**
 *
 * @author Evandro
 */
public class FieldMap implements Serializable {
    private Field source;
    private Field target;
    private EntityMap fatherEntityMap; // ponteiro para saber quem é o pai do mapeamento entre campos    
    private String transformationName;
    private TransformationType transformationType;
    private String udf;

    public String getUdf() {
        return udf;
    }

    public void setUdf(String udf) {
        this.udf = udf;
    }
    
    

    public FieldMap(String targetFieldName, String sourceFieldName, String transformationName, EntityMap entityMap) {
        this.fatherEntityMap = entityMap;
        
        this.source = this.fatherEntityMap.getSourceEntity().getFieldByName(sourceFieldName);
        this.target = this.fatherEntityMap.getTargetEntity().getFieldByName(targetFieldName);
        this.transformationName = transformationName;
    }
    
    public String getTargetFieldName() {
        return this.target.getName();
    }    

    public String getSourceFieldName() {
        return this.source.getName();
    }
    
    public String getSourceDataType(){
        return this.source.getDataType();
    }
    
    public String getTargetDataType(){
        return this.target.getDataType();
    }

    public Field getSourceField() {
        return source;
    }

    public Field getTargetField() {
        return target;
    }

    public String getTransformationName() {
        return transformationName;
    }

    public void setTransformationName(String transformationName) {
        this.transformationName = transformationName;
    }

    public TransformationType getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(TransformationType transformationType) {
        this.transformationType = transformationType;
    }
    
    @Override
    public String toString() {
        String out = "{";
        out += this.getSourceFieldName() + " -> ";
        out += this.getTargetFieldName() + ", ";
        out += "transformation: " + this.getTransformationName() + ", ";
        out += "transformationType: " + this.getTransformationType();
        
        return out;
    }
}
