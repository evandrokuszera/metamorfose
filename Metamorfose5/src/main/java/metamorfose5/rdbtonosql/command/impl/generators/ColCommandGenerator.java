/*
 * This class traverse a graph 'g' and generate a EntityMap collection to nesting entities inside main entity.
 */
package metamorfose5.rdbtonosql.command.impl.generators;

import metamorfose5.rdbtonosql.command.CommandGenerator;
import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import metamorfose5.rdbtonosql.command.Command;
import metamorfose5.rdbtonosql.command.join_spec.MultipleJoinSpec;
import java.util.ArrayList;
import metamorfose5.map.FieldMapping;
import metamorfose5.map.Mapping;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedAcyclicGraph;
import metamorfose5.map.script.impl.JSEmbeddedCommands;

/**
 *
 * @author Evandro
 */
public class ColCommandGenerator extends CommandGenerator {       

    public ColCommandGenerator() {
        super.setCommandGenerator(this);
    }       
    
    @Override
    public Command create(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g, TableVertex vertex){        
        //////////////////////////////////////////////////////////////////////////////////////
        // STEP 0: Criando um clone do vertex raiz (alvo da transformação).
        // Esse clone será usado na última transformação, onde seus dados serão utilizados para criar a família de colunas e rowkey da tabela raiz        
        //vertexRaizClone = cloneTableVertex(getRootVertex(g));           
        //VERICAR ESSA QUESTÃO............... COLOQUEI NO COMMANDGENERATOR....
        
        //////////////////////////////////////////////////////////////////////////////////////
        // STEP 1: caminhando no grafo para criar a especificação de Join ENTRE vertex folha E vertex raiz do grafo.
        TableVertex vertexAux = vertex;

        // Cria objeto joinSpec
        MultipleJoinSpec joinSpec = new MultipleJoinSpec(
                getVertexRaizClone().getName(), 
                getVertexRaizClone().getPk(), 
                getVertexRaizClone().getPk());

        // Percorre grafo do vértice folha até vértice raiz.
        while (Graphs.successorListOf(g, vertexAux).size() > 0) {
            // Recupera sucessor, assumindo que o vértice corrente tem apenas um sucessor (VERIFICAR ISSO)
            TableVertex sucessor = Graphs.successorListOf(g, vertexAux).get(0);
            // Recupera aresta entre vertex e seu sucessor...
            RelationshipEdge edge = g.getEdge(vertexAux, sucessor);
            // Cria joinSpec entre vertex e sucessor
            joinSpec.addBinaryJoin(
                    vertexAux.getName() + "_" + sucessor.getName(),
                    edge.getSource().getName(),
                    edge.getTarget().getName(),
                    edge.getKeySource(),
                    edge.getKeyTarget());

            // Operação para caminhar em direção ao vertex raiz.
            vertexAux = sucessor;
        }

        //////////////////////////////////////////////////////////////////////////////////////
        // STEP 2: Cria command para transformação de dados.   
        // se o grafo 'g' tem somente dois vertexs significa que todos os vertexs foram processados e estamos na última transformação.
        //  o vertex que resta representa a entidade alvo da transformação.
        //  no mapeamento final deve ser adicionado a rowkey e família de colunas da entidade alvo da transformação.
        Command cmd = null;
        Mapping mapping = null;  
        
        if (g.vertexSet().size() == 2) { // se verdadeira, último mapeamento
            mapping = createManyNestingMapping(vertexAux, vertex, true);  
        } else { // demais mapeamentos
            mapping = createManyNestingMapping(vertexAux, vertex, false);
        }
        cmd = new Command(joinSpec, mapping, true);       

        return cmd;
    }       

    

    // Esse método cria um EntityMap para realizar a transformação do RDB para JSON no formato NoSQL Colunar
    //  Esse formato NoSQL Colunar embute todas as tabelas relacionadas a tabela alvo como famílias de colunas.
    //  Quando encontra relacionamento 1:n, embute o lado n renomeando as colunas usando a pk do lado n para compor os nomes das colunas.
    private Mapping createManyNestingMapping(TableVertex oneSide, TableVertex manySide, boolean isLastConversion) {
        Mapping mapping = new Mapping();
        mapping.setName(oneSide.getName() + " (1)< -- (n as Column Family)" + manySide.getName());
        
        ////////////////////////////////////////////////////////////////////////////////
        // Grafo.vertex == 2
        // se última conversão, então:
        if (isLastConversion) {                         
            // Cria rowkey e...
            String rowkey = getVertexRaizClone().getPk();
            
            FieldMapping fm = FieldMapping.builder()
                    .addSourceField(rowkey, "String")
                    .addTargetField("rowkey", "String")
                    .get();
//            FieldMap fm = FieldMapBuilder.builder()
//                        .addSourceField(new Field(rowkey, DataTypes.StringType))
//                        .addTargetField(new Field("rowkey", DataTypes.StringType))
//                        .fieldMapType(FieldMapType.OneToOne)
//                        .transformation(new CastingTransformation()).get();
            
            mapping.mapFields(fm);
            
            // Cria família de colunas para os dados da entidade raiz, copiando os dados do vertexRaizClone
            FieldMapping fmLado1 = FieldMapping.builder();
            for (String field : getVertexRaizClone().getFields()) {
                fmLado1.addSourceField(field, "String");
                oneSide.getFields().remove(field); // removendo o campo já transformado o vertex recuperado o grafo (vertex raiz modificado pelas transformações).
            } 
            fmLado1.addTargetField(getVertexRaizClone().getName(), "String");
            fmLado1.UDF("oneNesting", JSEmbeddedCommands.getDocNestingScript(getVertexRaizClone().getName(), getVertexRaizClone().getFields()));
            mapping.mapFields(fmLado1.get());
            
//            FieldMapBuilder builderLado1 = FieldMapBuilder.builder();
//            for (String field : getVertexRaizClone().getFields()) {
//                builderLado1.addSourceField(new Field(field, DataTypes.StringType));
//                oneSide.getFields().remove(field); // removendo o campo já transformado o vertex recuperado o grafo (vertex raiz modificado pelas transformações).
//            }
//            builderLado1.addTargetField(new Field(getVertexRaizClone().getName(), DataTypes.StringType));
//            builderLado1.fieldMapType(FieldMapType.ManyToOne);
//            builderLado1.transformation(JSEmbeddedCommands.getDocNestingScript(getVertexRaizClone().getName(), getVertexRaizClone().getFields()));
//            mapping.mapFields(builderLado1.get());
            
        }
        

        ////////////////////////////////////////////////////////////////////////////////
        // LADO 1
        // cria fieldMaps para o lado UM do relacionamento.
        for (String field : oneSide.getFields()) {
            FieldMapping fm = FieldMapping.builder()
                    .addSourceField(field, "String")
                    .addTargetField(field, "String")
                    .get();

            mapping.mapFields(fm);
        }
            
        ////////////////////////////////////////////////////////////////////////////////
        // LADO MUITOS
        // Criando fieldMaps para o lado MUITOS do relacionamento.
        FieldMapping builder = FieldMapping.builder();

        for (String field : manySide.getFields()) {
            builder.addSourceField(field, "String");
        }
        builder.addTargetField(manySide.getName(), "String");

        // Construindo a transformação ManyNesting via Javascript
        ArrayList<String> fieldNames = new ArrayList<>();
        for (String fieldName : manySide.getFields()) {
            fieldNames.add(fieldName);
        }
        builder.UDF(manySide.getName()+"_manyNesting", JSEmbeddedCommands.getManyNestingScript(manySide.getName(), fieldNames, manySide.getPk()));

        // Atualizando o vertex oneSide para conter o novo documento embutido
        oneSide.getFields().add(manySide.getName());

        mapping.mapFields(builder.get());
        return mapping;
    }

}
