/*
 * Esta classe estabelece mapeamento entre duas entidades (origem e destino).
 * Pensando em selecionar quais campos das entidades origem e destino participam do mapeamento, esse mapeamento é estabelecido através do array 'fieldMappings'.
*/
package metamorfose.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import metamorfose.converters.Converter;
import metamorfose.model.Entity;
import metamorfose.model.Field;
import metamorfose.transformations.javascript.Script;
import metamorfose.transformations.javascript.TransformationType;

/**
 *
 * @author Evandro
 */
public class EntityMap implements Serializable {
    private String name;
    private Entity sourceEntity;
    private Entity targetEntity;
    private String annotations;
    private List<Script> scripts;
    private List<FieldMap> fieldMappings;
    private Converter converter;

    public EntityMap(String name, Entity targetEntity, Entity sourceEntity, Converter converter) {
        this.name = name;
        this.sourceEntity = sourceEntity;
        this.targetEntity = targetEntity;
        this.converter = converter;
        this.fieldMappings = new ArrayList<>();  
        this.scripts = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAnnotations() {
        return annotations;
    }

    public void setAnnotations(String annotations) {
        this.annotations = annotations;
    }
    
    public Entity getSourceEntity() {
        return sourceEntity;
    }

    public void setSourceEntity(Entity sourceEntity) {
        this.sourceEntity = sourceEntity;
    }

    public Entity getTargetEntity() {
        return targetEntity;
    }

    public void setTargetEntity(Entity targetEntity) {
        this.targetEntity = targetEntity;
    }

    public Converter getConverter() {
        return converter;
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    public List<FieldMap> getFieldMappings() {
        return fieldMappings;
    }
    
    public FieldMap getFieldMapping(String targetFieldName){
        for (FieldMap m : this.fieldMappings){
            if (m.getTargetFieldName().equals(targetFieldName)){
                return m;
            }
        }
        return null;
    }
    
    public FieldMap getFieldMappingBySourceFieldName(String sourceFieldName){
        for (FieldMap m : this.fieldMappings){
            if (m.getSourceFieldName().equals(sourceFieldName)){
                return m;
            }
        }
        return null;
    }

    public List<Script> getScripts() {
        return scripts;
    }
    
    public Script getScriptyByFunctionName(String functionName){
        for (Script s : this.scripts){
            if (s.getFunctionName().equals(functionName)){
                return s;
            }
        }
        return null;
    }
    
        
    // This method create mapping using 'existing' fields in source/target entities.
    public void mapFields(String targetFieldName, String sourceFieldName, String transformationName) {
        if (this.sourceEntity.getFieldByName(sourceFieldName) == null) {
            throw new IllegalArgumentException("EntityMap: Field name doesn´t exist in the entity! You must provide a field name that exist in the SourceEntity.");
        }

        if (this.targetEntity.getFieldByName(targetFieldName) == null) {
            throw new IllegalArgumentException("EntityMap: Field name doesn´t exist in the entity! You must provide a field name that exist in the TargetEntity.");
        }

        FieldMap fieldMap = new FieldMap(targetFieldName, sourceFieldName, transformationName, this);        
        this.fieldMappings.add( fieldMap );
    }
    
    // This method create new fields e add in source/target entities. After that, the mapping between is created.
    public void mapFields(String newTargetFieldName, String targetDataType, String newSourceFieldName, String sourceDataType, String transformationName, String udfName) {
        
        if (this.targetEntity.getFieldByName(newTargetFieldName) == null) {            
            Field target = new Field(newTargetFieldName, targetDataType);    // Create target field      
            this.targetEntity.getFields().add(target);  // Add target field in targetEntity
        } else {
            //throw new IllegalArgumentException("EntityMap: Field name " + newTargetFieldName + " already exist in the Target Entity!");
        }

        if (this.sourceEntity.getFieldByName(newSourceFieldName) == null) {            
            Field source = new Field(newSourceFieldName,sourceDataType); // Create source field   
            this.sourceEntity.getFields().add(source); // Add source field in sourceEntity
        } else {
            //throw new IllegalArgumentException("EntityMap: Field name " + newSourceFieldName + " already exist in the Source Entity!");
        }
        
        FieldMap fieldMap = new FieldMap(newTargetFieldName, newSourceFieldName, transformationName, this);  
        
        // Atributo teste
        fieldMap.setUdf(udfName);
        
        this.fieldMappings.add( fieldMap );        
    }

    
    
    // EXPERIMENTO JAVASCRIPT    
    
    // This method create new fields e add in source/target entities. After that, the mapping between is created.
    public void mapFields(String newTargetFieldName, 
                          String targetDataType, 
                          String newSourceFieldName, 
                          String sourceDataType, 
                          String transformationName, 
                          TransformationType transformationType,
                          String scriptImplementation) {
        
        if (this.targetEntity.getFieldByName(newTargetFieldName) == null) {            
            Field target = new Field(newTargetFieldName, targetDataType);    // Create target field      
            this.targetEntity.getFields().add(target);  // Add target field in targetEntity
        } else {
            //throw new IllegalArgumentException("EntityMap: Field name " + newTargetFieldName + " already exist in the Target Entity!");
        }

        if (this.sourceEntity.getFieldByName(newSourceFieldName) == null) {            
            Field source = new Field(newSourceFieldName,sourceDataType); // Create source field   
            this.sourceEntity.getFields().add(source); // Add source field in sourceEntity
        } else {
            //throw new IllegalArgumentException("EntityMap: Field name " + newSourceFieldName + " already exist in the Source Entity!");
        }
        
        FieldMap fieldMap = new FieldMap(newTargetFieldName, newSourceFieldName, transformationName, this);        
        fieldMap.setTransformationType(transformationType);
        
        // Se o tipo da transformação for JAVASCRIPT, a implementação do script é mantida nesta classe (EntityMap).
        // Motivo: para ter desempenho é necessário carregar todos os scripts que serão usados durante as transformações de uma vez só.
        if (transformationType == transformationType.JAVASCRIPT){
            this.scripts.add(new Script(transformationName, scriptImplementation));
        }
        
        this.fieldMappings.add( fieldMap );        
    }    
    
    // FIM EXPERIMENTO JAVASCRIPT
    
    
    
    
    
    
    @Override
    public String toString() {
        String out = "";
        for (FieldMap fm : this.fieldMappings){
            if (out.length() !=0){
                out += ", ";
            }
            out += "{" + fm.getSourceFieldName() + " -> " + fm.getTargetFieldName() + "}";
        }
        out = "FieldMaps: [" + out + "]";
        
        return out;
    }
}
