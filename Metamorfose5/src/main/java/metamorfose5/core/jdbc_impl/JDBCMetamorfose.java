/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.core.jdbc_impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import metamorfose5.core.Metamorfose;
import metamorfose5.map.executor.MappingExecutor;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Evandro
 */
public class JDBCMetamorfose implements Serializable, Metamorfose {
//    private HashMap<String,EntityJDBC> entities = null;
    private ArrayList<EntityJDBC> entities = new ArrayList<>();
    
    
    // Creating a new entity
    public void createEntity(String entityName, JSONArray records) {
        // First, I am going remove if entityName has already existed.
        removeEntity(entityName);

        // Second, creating a new entity.
        EntityJDBC newEntity = new EntityJDBC(entityName);
        newEntity.setData(records);
        this.entities.add(newEntity);
    }

    // Removing an existing entity
    public void removeEntity(String entityName) {
        for (EntityJDBC entity : this.entities){
            if (entityName.equals(entity.getName())) {
               this.entities.remove(entity);
            }
        }
    }
    
    @Override
    public EntityJDBC getEntity(String entityName) {
        for (EntityJDBC entity : this.entities){
            if (entityName.equals(entity.getName())) {
               return entity;
            }
        }
        return null;
    }
    
    @Override
    public ArrayList<EntityJDBC> getEntities() {
        return this.entities;
    }
    
    public void printEntities(){
        System.out.println("JDBC Metamorfose Entities: ");
        for (EntityJDBC entity : this.entities){
            System.out.println(entity.getName());
        }
    }
    

    // Código que faz join entre dois arrays de objetos JSON contidos em entitiesPool (join sem otimizações, preciso melhorar esse código).
    // Trabalho Futuro: procurar algoritmos ou novas estratégias para realizar JOIN.
    public void joinEntities(String joinEntityName, String entityOne, String entityMany, String keyOne, String keyMany){    
        JSONArray sourceEntities = this.getEntity(entityOne).getData();
        JSONArray targetEntities = this.getEntity(entityMany).getData();
        
        JSONArray joinedEntities = new JSONArray();
        
        // Para cada targetEntity, localizar e inserir as sourceEntities relacionadas
        for (int t = 0; t<targetEntities.length(); t++){            
            String targetKeyValue = targetEntities.getJSONObject(t).getString(keyMany);
            boolean foundRelatedEntity = false;
            for (int s = 0; s<sourceEntities.length(); s++){
                String sourceKeyValue = sourceEntities.getJSONObject(s).getString(keyOne);
                // Comparando as chaves entre Target e Source...
                if (targetKeyValue.equalsIgnoreCase(sourceKeyValue)){
                    // Chaves iguais.
                    foundRelatedEntity = true;
                    // cria newJoinedEntity
                    JSONObject newJoinedEntity = new JSONObject(targetEntities.getJSONObject(t).toString());
                    // adicionando sourceEntity fields em newJoinedEntity                    
                    for (String fieldName : sourceEntities.getJSONObject(s).keySet()){
                        newJoinedEntity.put(fieldName, sourceEntities.getJSONObject(s).get(fieldName));
                    }
                    // adiciona newJoinedEntity no array joinedEntities                    
                    joinedEntities.put(newJoinedEntity);
                }
            }
            // Não achou entidade relacionada em sourceEntities, então insere apenas a entidade targetEntity no array joinedEntities (garantindo o RIGHT JOIN)
            if (!foundRelatedEntity){
                JSONObject newJoinedEntity = new JSONObject(targetEntities.getJSONObject(t).toString());
                // adiciona newJoinedEntity no array joinedEntities
                joinedEntities.put(newJoinedEntity);
            }
        }
        // substitui a targetEntities pela versão joinedEntities
        this.createEntity(joinEntityName, joinedEntities);
        System.out.println("joined: "+joinedEntities.length());        
    }

    @Override
    public JDBCLoad load() {
        return new JDBCLoad(this);
    }

    @Override
    public Object persist() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object filter() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    // Using jsonModel
    public void mapTransformTo(String entityName, MappingExecutor executor, String newEntityName) {
        // Array que recebera as entidades transformadas.
        JSONArray transformedEntities =  new JSONArray();
        
        for (int j = 0; j<this.getEntity(entityName).getData().length(); j++){
            transformedEntities.put( executor.execute(this.getEntity(entityName).getData().getJSONObject(j)) );
        }
    }
        
    public void mapreduceTransformTo(String entityName, String groupKey, MappingExecutor executor, String newEntityName) {
            // Array que recebera as entidades transformadas.
            JSONArray transformedEntities =  new JSONArray();
            // Para cada instância: são executadas as transformações e o resultado é adicionado em transformedEntities.

            // Para MapReduce, preciso agrupar as entidades de acordo com GroupingKey (preciso simular um MapReduce).
            JSONArray groupedEntities = new JSONArray();
            String groupKeyValue = this.getEntity(entityName).getData().getJSONObject(0).getString(groupKey);

            // Percorre as instâncias de joinedEntities
            for (int j = 0; j<this.getEntity(entityName).getData().length(); j++){
                // Passo 1: Agrupando as entidades por groupKeyValue                        
                if(this.getEntity(entityName).getData().getJSONObject(j).getString(groupKey).equalsIgnoreCase(groupKeyValue)){
                    groupedEntities.put(this.getEntity(entityName).getData().getJSONObject(j));

                // Passo 2: transforma os objetos do grupo
                } else {
                    // alcancei o próximo grupo... atualiza groupKeyValue
                    groupKeyValue = this.getEntity(entityName).getData().getJSONObject(j).getString(groupKey);
                    // adiciona objeto criado em transformedEntities
                    transformedEntities.put(executeReduceGroupEntities(groupedEntities, executor));
                    // reinicializa groupEntities, para a próxima rodada.
                    groupedEntities = new JSONArray();
                    // adiciona primeiro objeto do próximo grupo (j neste ponto aponta para instância do próximo grupo)
                    groupedEntities.put(this.getEntity(entityName).getData().getJSONObject(j));
                }
                if ((j+1) == this.getEntity(entityName).getData().length()){ // alcancei o último registro... então processa groupedEntities
                    transformedEntities.put(executeReduceGroupEntities(groupedEntities, executor));
                }
            } // Fim For joinedEntities

            // Atualiza objetos no entitiesPool (troca instâncias antigas pelas instâncias transformadas)
            this.createEntity(newEntityName, transformedEntities);
            System.out.println("Transformed entities: " + transformedEntities.length());
        
    } // Fim execute()
    
    
    // Achar nome melhor... análogo ao reduce que uso no Metamorfose...
    private JSONObject executeReduceGroupEntities(JSONArray groupedEntities, MappingExecutor executor){
        // processa os objetos do grupo
        Iterator it = groupedEntities.iterator();
        JSONObject obj1 = (JSONObject) it.next();
        JSONObject obj2 = null;
        while (it.hasNext()){
            obj2 = (JSONObject) it.next();
            obj1 = executor.execute(obj1, obj2);
        }
        // caso não haja obj2 (ou seja, apenas um objeto no grupo), então chama a função execute com o segundo parâmetro igual a null
        if (obj2 == null){
            obj1 = executor.execute(obj1, null);
        }
        // retorna objeto criado;
        return obj1;
    } 
        
}