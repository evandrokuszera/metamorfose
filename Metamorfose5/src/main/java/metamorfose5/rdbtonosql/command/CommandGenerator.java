/*
 * This class traverse a graph 'g' and generate a EntityMap collection according to types of edges (one_embedded, many_embedded, etc).
 */
package metamorfose5.rdbtonosql.command;

import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import java.util.ArrayList;
import java.util.Iterator;
import org.jgrapht.graph.DirectedAcyclicGraph;

/**
 *
 * @author Evandro
 */
public abstract class CommandGenerator {
    private ArrayList<Command> commandArray; 
    private TableVertex vertexRaizClone; 
    private CommandGenerator commandGenerator;
    
    // Método deve ser implementado pelas classe concretas. Cada classe concreta implementa lógica para criar um Command de acordo com o modelo NoSQL desejado.
    protected abstract Command create(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g, TableVertex vertex);

    public CommandGenerator() {        
        this.commandArray = new ArrayList<>();
    }

    public void setCommandGenerator(CommandGenerator commandGenerator) {
        this.commandGenerator = commandGenerator;
    }

    public TableVertex getVertexRaizClone() {
        return vertexRaizClone;
    }

    public ArrayList<Command> getCommands() {
        return commandArray;
    }
    
    public String printCommands(){
        String msg = "";
        int i = 1;
        for (Command cmd : commandArray){
            System.out.println("COMMAND: "+i);
            System.out.println(cmd);
            msg += "COMMAND: "+(i)+"\n";
            msg += cmd+"\n";
            i++;
        }   
        return msg;
    }
    
    // Método para percorrer caminhos do nó folha até nó raiz e gerar Commands.
    public void generateCommands(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g) {
        
        // Clona o grafo 'g' para que o algoritmo abaixo não altere o grafo original.
        DirectedAcyclicGraph<TableVertex, RelationshipEdge> graph = (DirectedAcyclicGraph<TableVertex, RelationshipEdge>) g.clone();
                
        this.vertexRaizClone = cloneTableVertex(getRootVertex(graph));

        // Enquanto o grafo 'g' ainda não foi reduzido a um vertex, processa todos os vértices e gera a coleção de entityMaps
        while (graph.vertexSet().size() > 1) {
            // recupera o conjunto de vértices folha (leafVertexs)
            ArrayList<TableVertex> leafVertexs = getVertexsWithZeroInDegree(graph);
            // para cada leafVertex cria um Command 
            for (TableVertex leafVertex : leafVertexs) {
                // cria comando...
                Command cmd = commandGenerator.create(graph, leafVertex);
                // adiciona comando na lista de comandos.
                commandArray.add(cmd);            
                // Remove leafVertex que foi processado.            
                graph.removeVertex(leafVertex);
            }
            
        }
    }       

    
    
    //########################################################################################################################################
    // MÉTODOS PRIVADOS DE APOIO.
    //########################################################################################################################################
    
    
    // Recupera todos os vértices folhas.
    // Vértice Folha = vertexs que NÃO possuem arestas de entrada.
    private ArrayList<TableVertex> getVertexsWithZeroInDegree(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g) {
        ArrayList<TableVertex> vertexs_with_zero_inDegree = new ArrayList<>();
        // Percorrendo conjunto de vértices do graph 'g'
        Iterator<TableVertex> vertexIterator = g.vertexSet().iterator();
        while (vertexIterator.hasNext()) {
            TableVertex vertex = vertexIterator.next();
            // Recuperando apenas vértices sem arestas de entrada (vértices folha).
            if (g.inDegreeOf(vertex) == 0) {
                vertexs_with_zero_inDegree.add(vertex);
            }
        }

        return vertexs_with_zero_inDegree;
    }
    
    // Recupera o vertex raiz. Esse vertex representa a entidade alvo da transformação.
    private TableVertex getRootVertex(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g) {
        TableVertex root = null;
        // Percorrendo conjunto de vértices do graph 'g'
        Iterator<TableVertex> vertexIterator = g.vertexSet().iterator();
        while (vertexIterator.hasNext()) {
            TableVertex vertex = vertexIterator.next();
            // Recuperando apenas o vértice sem arestas de saída. Esse vertice é raiz do grafo.
            if (g.outDegreeOf(vertex) == 0) {
                root = vertex;
            }
        }
        return root;
    }

    // Faz um clone do vertex passado por parâmetro
    private TableVertex cloneTableVertex(TableVertex vertex) {
        // Cria vertex copia...
        TableVertex newTableVertex = new TableVertex(vertex.getName(), vertex.getTableName(), vertex.getPk());
        // Cria copia de cada filed do vertex
        for (String field : vertex.getFields()) {
            newTableVertex.getFields().add(field);
        }
        return newTableVertex;
    }

}
