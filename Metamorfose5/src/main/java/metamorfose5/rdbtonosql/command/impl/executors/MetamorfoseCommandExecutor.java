/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command.impl.executors;

import metamorfose5.rdbtonosql.command.Command;
import metamorfose5.rdbtonosql.command.CommandExecutor;
import metamorfose5.rdbtonosql.command.join_spec.BinaryJoinSpec;
import dag.model.TableVertex;
import java.util.Iterator;
import metamorfose5.core.spark_impl.SparkMetamorfose;
import metamorfose5.map.executor.MappingExecutor;
import metamorfose5.util.MetamorfoseUtil;

/**
 *
 * @author Evandro
 */
public class MetamorfoseCommandExecutor extends CommandExecutor {
    private SparkMetamorfose metamorfose;
    
    public MetamorfoseCommandExecutor(String dbserver, String dbname, String dbuser, String dbpassword) {
        this.setDBConnectionProperties(dbserver, dbname, dbuser, dbpassword);        
    }

    public SparkMetamorfose getMetamorfose() {
        return metamorfose;
    }
    
    public void instanciarMetamorfose(){
        this.metamorfose = new SparkMetamorfose();
    }
    
    @Override
    public void execute() {
        this.metamorfose = new SparkMetamorfose();
        
        // Carregando as entidades (RDB) do grafo no Metamorfose.
        loadGraphEntities();
        
        // Este método recebe o DAG por parâmetro e gera um conjunto de Operation que encapsula informações para transformar RDB para NoSQL através do Metamorfose.
        // ACHO QUE VEM DE FORA.... commandGenerator.generateCommands(graph);
        
        // Percorrendo a coleção de Operation geradas no passo anterior.
        // Para cada comando:
        //  a) faz a junção entre entidades do DAG
        //  b) chama primitiva Metamorfose para carregar, transformar e gerar objetos JSON a partir do RDB.
        for (Command cmd : this.getCommands()){         
                        
            // Faz junção das entidades do DAG.
            joinEntites(cmd, metamorfose);
            
            // Executa primitiva Metamorfose para transformar dados.
            if (cmd.isMapreduceOperation()){                    
                metamorfose.mapreduceTransformTo(  
                        cmd.getJoinEntities().getJoinedEntityName(),                        
                        cmd.getJoinEntities().getGroupingKey(),
                        new MappingExecutor(cmd.getMapping()), 
                        cmd.getJoinEntities().getJoinedEntityName());

            } else {
                metamorfose.mapTransformTo(   
                        cmd.getJoinEntities().getJoinedEntityName(), 
                        new MappingExecutor(cmd.getMapping()), 
                        cmd.getJoinEntities().getJoinedEntityName());                    
            }
                     
        } // fim for commands       
    }
    
    // Carrega todas as entidades RDB do DAG no Metamorfose Framework (Spark)
    public void loadGraphEntities(){
        // Percorrendo conjunto de vértices (tabela RDB) do 'graph' e carregando como entidades no Metamorfose Framework.
        Iterator<TableVertex> vertexIterator = this.getGraph().vertexSet().iterator();        
        while (vertexIterator.hasNext()){
            TableVertex vertex = vertexIterator.next();            
            metamorfose.load().fromJDBC(vertex.getName(), this.getDbname(), this.getDbuser(), this.getDbpassword(), vertex.getName());      
        }        
    }
    
    
    // Realizando a junção das entidades do DAG.
    // Fluxo das junções: source (leaf) para target (root) vertex. 
    // Essa função tem por objetivo eliminar o uso do SQL para junção das entidades.
    public void joinEntites(Command command, SparkMetamorfose metamorfose){
        // Quantas junções serão necessárias?
        int numberOfBinaryJoins = command.getJoinEntities().getBinaryJoinArray().size();
        // Se necessário junção entre várias tabelas (usar dataset temporário para NÃO modificar os datasets existentes)
        if (numberOfBinaryJoins > 1) {
            int joinCount = 1;
            String sourceEntityName = "";
            String targetEntityName = "";
            String joinedEntityName = "";            

            while (joinCount <= numberOfBinaryJoins) {
                // Recupera um binaryJoin objetc
                BinaryJoinSpec joinSpec = command.getJoinEntities().getBinaryJoinArray().get(joinCount - 1);
                // Controla a criação de datasets temporários para junção entre várias tabelas.
                if (joinCount == 1) { // primeira junção entre tabelas...
                    sourceEntityName = joinSpec.getSourceEntityName();
                    targetEntityName = joinSpec.getTargetEntityName();
                    joinedEntityName = "MetamorfoseTempDataset";
                } else if (joinCount == numberOfBinaryJoins) { // última junção entre tabelas...
                    sourceEntityName = "MetamorfoseTempDataset";
                    targetEntityName = joinSpec.getTargetEntityName();
                    joinedEntityName = joinSpec.getTargetEntityName();
                } else { // junções intermediárias entre tabelas.
                    sourceEntityName = "MetamorfoseTempDataset";
                    targetEntityName = joinSpec.getTargetEntityName();
                    joinedEntityName = "MetamorfoseTempDataset";
                }
                // criando junção de dataset através do Metamorfose...
                if (joinCount == numberOfBinaryJoins){
                    // Atenção: na última junção, entre vértice RAIZ e demais vértices, usar RIGHJOIN para recuperar todas as entidades do vértice RAIZ.
                    metamorfose.rightJoinEntities(
                            joinedEntityName,
                            sourceEntityName,
                            targetEntityName,
                            joinSpec.getKeySource(),
                            joinSpec.getKeyTarget());
                } else {
                    metamorfose.joinEntities(
                            joinedEntityName,
                            sourceEntityName,
                            targetEntityName,
                            joinSpec.getKeySource(),
                            joinSpec.getKeyTarget());
                }

                joinCount++;
            }
            metamorfose.removeEntity("MetamorfoseTempDataset");
            // Se junção entre duas tabelas
        } else {
            BinaryJoinSpec joinSpec = command.getJoinEntities().getBinaryJoinArray().get(0);
            metamorfose.rightJoinEntities(
                    command.getJoinEntities().getJoinedEntityName(),
                    joinSpec.getSourceEntityName(),
                    joinSpec.getTargetEntityName(),
                    joinSpec.getKeySource(),
                    joinSpec.getKeyTarget());
        }
        
    } // Fim joinEntities
    
}
