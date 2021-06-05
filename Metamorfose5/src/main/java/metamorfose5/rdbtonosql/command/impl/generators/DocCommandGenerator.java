/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command.impl.generators;

import metamorfose5.rdbtonosql.command.CommandGenerator;
import metamorfose5.rdbtonosql.command.Command;
import metamorfose5.rdbtonosql.command.join_spec.MultipleJoinSpec;
import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import java.util.ArrayList;
import java.util.List;
import metamorfose5.map.Field;
import metamorfose5.map.FieldMapping;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedAcyclicGraph;
import metamorfose5.map.Mapping;
import metamorfose5.map.script.impl.JSEmbeddedCommands;

/**
 *
 * @author Evandro
 */
public class DocCommandGenerator extends CommandGenerator {    

    public DocCommandGenerator() {
        super.setCommandGenerator(this);
    }
    
    @Override
    public Command create(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g, TableVertex leafVertex){
        
        // Recupera lista de sucessores do vértice corrente
        List<TableVertex> successorListOf = Graphs.successorListOf(g, leafVertex);

        // Assumindo que o vértice corrente tem apenas um sucessor (VERIFICAR ISSO)
        TableVertex sucessor = successorListOf.get(0);

        // Tenho nesse ponto: leafVertex e seu sucessor...
        RelationshipEdge edge = g.getEdge(leafVertex, sucessor);
        
        // Criação do Command.
        // Command é composto por joinSpec, EntityMap e Primitiva Metamorfose (true ou false)
        Command cmd = null;
        MultipleJoinSpec joinSpec = null;
        Mapping mapping = null;
        
        joinSpec = new MultipleJoinSpec(sucessor.getName(), sucessor.getPk(), edge.getKeyTarget());
        joinSpec.addBinaryJoin( sucessor.getName(),
                                edge.getSource().getName(),
                                edge.getTarget().getName(),
                                edge.getKeySource(),
                                edge.getKeyTarget());



        // Cria command para transformação de dados.        
        switch (edge.getTypeofNesting()) {
            case "one_embedded":  
                mapping = createOneEmbeddedEntityMap(leafVertex, sucessor);
                cmd = new Command(joinSpec, mapping, false);                                
                break;
            case "many_embedded":
                mapping = createManyEmbeddedEntityMap(sucessor, leafVertex);
                cmd = new Command(joinSpec, mapping, true); 
                break;
        }
        return cmd;
    }
    
    //########################################################################################################################################
    // MÉTODOS PRIVADOS PARA CRIAR OS MAPEAMENTOS PARA ANINHAR AS ENTIDADES, CONFORME MODELO ORIENTADO A DOCUMENTOS.
    //  - Estão sendo usadas UDF em Javascript.
    //########################################################################################################################################
    
    private Mapping createManyEmbeddedEntityMap(TableVertex oneSide, TableVertex manySide) {
        Mapping mapping = new Mapping();
        mapping.setName(oneSide.getName() + " (1)< -- (n)" + manySide.getName());

        // LADO 1
        // Criando fieldMaps para o lado UM do relacionamento.
        for (String field : oneSide.getFields()) {
            FieldMapping fm = FieldMapping.builder()
                    .addSourceField(new Field(field, "string"))
                    .addTargetField(new Field(field, "string"))
                    .get();

            mapping.mapFields(fm);
        }

        // LADO MUITOS
        // Criando fieldMaps para o lado MUITOS do relacionamento.
        FieldMapping fm = FieldMapping.builder();

        for (String field : manySide.getFields()) {
            fm.addSourceField(new Field(field, "string"));
        }
        fm.addTargetField(new Field(manySide.getName(), "string"));
        
        // Construindo a transformação ManyEmbedded via Javascript
        ArrayList<String> fieldNames = new ArrayList<>();
        for (String fieldName : manySide.getFields()) {
            fieldNames.add(fieldName);
        }
        String jScript = JSEmbeddedCommands.getManyEmbeddedScript(manySide.getName(), fieldNames);
        
        fm.UDF("arrayEmbedded", jScript);
        
        // Atualizando o vertex oneSide para conter o novo array de documentos embutidos
        oneSide.getFields().add(manySide.getName());

        mapping.mapFields(fm.get());
        return mapping;        
    }

    private Mapping createOneEmbeddedEntityMap(TableVertex oneSide, TableVertex manySide) {
        Mapping mapping = new Mapping();
        mapping.setName(oneSide.getName() + " (1) --> (n)" + manySide.getName());

        // LADO MUITOS
        // Criando fieldMaps para o lado MUITOS do relacionamento.
        for (String field : manySide.getFields()) {
            FieldMapping fm = FieldMapping.builder()
                    .addSourceField(new Field(field, "string"))
                    .addTargetField(new Field(field, "string"))
                    .get();

            mapping.mapFields(fm);
        }

        // LADO UM
        // Criando fieldMaps para o lado MUITOS do relacionamento.
        FieldMapping builder = FieldMapping.builder();
        for (String field : oneSide.getFields()) {
            builder.addSourceField(new Field(field, "string"));
        }
        builder.addTargetField(new Field(oneSide.getName(), "string"));

        // Construindo a transformação OneEmbedded via Javascript (classe JSEmbeddedCommands)
        ArrayList<String> fieldNames = new ArrayList<>();
        for (String fieldName : oneSide.getFields()) {
            fieldNames.add(fieldName);
        }
        //String jScript = JSEmbeddedCommands.getOneEmbeddedScript(manySide.getName(), fieldNames);
        String jScript = JSEmbeddedCommands.getOneEmbeddedScript(oneSide.getName(), fieldNames);
        
        builder.UDF("docEmbedded", jScript);

        // Atualizando o vertex manySide para conter o novo campo embutido
        manySide.getFields().add(oneSide.getName());

        mapping.mapFields(builder.get());
        return mapping;
    }    
}
