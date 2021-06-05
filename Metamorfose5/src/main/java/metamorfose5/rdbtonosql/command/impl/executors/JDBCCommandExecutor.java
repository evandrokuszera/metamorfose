/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command.impl.executors;

import org.json.JSONObject;
import metamorfose5.rdbtonosql.command.Command;
import metamorfose5.rdbtonosql.command.CommandExecutor;
import metamorfose5.rdbtonosql.command.join_spec.BinaryJoinSpec;
import jdbc_connection.PostgresConnection;
import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.json.JSONArray;
import metamorfose5.map.executor.MappingExecutor;

/**
 *
 * @author Evandro
 */
public class JDBCCommandExecutor extends CommandExecutor {
    private HashMap<String,JSONArray> entitiesPool = null; // Objetos em memória para realizar as transformações
    private String sqlFilter = "";
    
    public JDBCCommandExecutor(String dbserver, String dbname, String dbuser, String dbpassword) {
        this.setDBConnectionProperties(dbserver, dbname, dbuser, dbpassword);        
    }

    public HashMap<String, JSONArray> getEntities() {
        return entitiesPool;
    }
    
    public JSONArray getEntity(String entityName) {
        return entitiesPool.get(entityName);
    }

    public void setSqlFilter(String sqlFilter) {
        this.sqlFilter = " WHERE " + sqlFilter;
    }
    
    @Override
    public void execute() {
        
        // Resumo do método execute():
        //   - Carrega entidades na memória
        //   - Faz junção entre duas entidades (source --> target)
        //   - Transforma as instâncias das entidades que sofreram junção
            
        // carrega as entidades em memória... (vários pontos para discutir e otimizar aqui... carregar uma por vez, todas, um grupo... de registros?).
        System.out.println("Loading all graph entities in memory...\n");
        entitiesPool = loadGraphEntities(this.getGraph());
            
        if (this.getCommands().size() == 0){
            System.out.println("Error: there are no commands to be execute!");
            return;
        }
            
        // Percorre os comandos...
        int count = 1;
        for (Command cmd : this.getCommands()){
            System.out.println("EXECUTING COMMAND: " + count + "/" + this.getCommands().size()); System.out.println(cmd); count++;

            MappingExecutor executor = new MappingExecutor(cmd.getMapping());
            
            // Fazendo JOIN entre duas entidades de forma manual (desempenho está ruim, preciso melhorar isso!)
            System.out.println("Executing join entities algoritm...");
            executeJoinEntities(entitiesPool, cmd);

            // Recuperando a entidade transformada pelo método executeJoinEntities
            JSONArray joinedEntities = entitiesPool.get(cmd.getJoinEntities().getJoinedEntityName());

            // Se a junção não retornar instâncias, então vai para o próximo comando (cmd).
            if (joinedEntities.length() == 0) continue;

            // Array que recebera as entidades transformadas.
            JSONArray transformedEntities =  new JSONArray();

            // De acordo com o tipo de transformação (Map ou MapReduce) as instâncias de joinedEntities são recorridas.
            // Para cada instância: são executadas as transformações e o resultado é adicionado em transformedEntities.
            System.out.println("Transforming entities...");
            if (cmd.isMapreduceOperation()){
                // Para MapReduce, preciso agrupar as entidades de acordo com GroupingKey (preciso simular um MapReduce).
                JSONArray groupedEntities = new JSONArray();
                String groupingKey = joinedEntities.getJSONObject(0).getString(cmd.getJoinEntities().getGroupingKey());

                // Percorre as instâncias de joinedEntities
                for (int j = 0; j<joinedEntities.length(); j++){
                    // Passo 1: Agrupando as entidades por groupingKey                        
                    if(joinedEntities.getJSONObject(j).getString(cmd.getJoinEntities().getGroupingKey()).equalsIgnoreCase(groupingKey)){
                        groupedEntities.put(joinedEntities.getJSONObject(j));
//                        if ((j+1) == joinedEntities.length()){ // alcancei o último registro... então processa groupedEntities  /// remover esse código...
//                            transformedEntities.put(executeReduceGroupEntities(groupedEntities, executor));
//                        }
                    // Passo 2: transforma os objetos do grupo
                    } else {
                        // alcancei o próximo grupo... atualiza groupingKey
                        groupingKey = joinedEntities.getJSONObject(j).getString(cmd.getJoinEntities().getGroupingKey());
                        // adiciona objeto criado em transformedEntities
                        transformedEntities.put(executeReduceGroupEntities(groupedEntities, executor));
                        // reinicializa groupEntities, para a próxima rodada.
                        groupedEntities = new JSONArray();
                        // adiciona primeiro objeto do próximo grupo (j neste ponto aponta para instância do próximo grupo)
                        groupedEntities.put(joinedEntities.getJSONObject(j));
                    }
                    
                    // Passo 1.1: Alcancei o último registro do grupo (agrupamento terminado)... então posso processar groupedEntities e transformar as entidades.
                    if ((j+1) == joinedEntities.length()){ 
                        transformedEntities.put(executeReduceGroupEntities(groupedEntities, executor));
                    }
                    
                } // Fim For joinedEntities
            // Map    
            } else {
                for (int j = 0; j<joinedEntities.length(); j++){
                    transformedEntities.put( executor.execute(joinedEntities.getJSONObject(j)) );
                }
            }
            // Atualiza objetos no entitiesPool (troca instâncias antigas pelas instâncias transformadas)
            entitiesPool.put(cmd.getJoinEntities().getJoinedEntityName(), transformedEntities);
            System.out.println("Transformed entities: " + transformedEntities.length());
        
        } // For cmdGen.getCommands()
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
    
    // COBRINDO JOIN N-ARIOS...
    // Código que faz join entre dois arrays de objetos JSON contidos em entitiesPool (join sem otimizações, preciso melhorar esse código).
    // Trabalho Futuro: procurar algoritmos ou novas estratégias para realizar JOIN.
    private void executeJoinEntities(HashMap<String,JSONArray> entitiesPool, Command cmd){    
        
        for (BinaryJoinSpec join : cmd.getJoinEntities().getBinaryJoinArray()){
            
            JSONArray sourceEntities = entitiesPool.get(join.getSourceEntityName());
            JSONArray targetEntities = entitiesPool.get(join.getTargetEntityName());
            String targetKeyName = join.getKeyTarget();
            String sourceKeyName = join.getKeySource();
            
            System.out.println("source: " + join.getSourceEntityName() + " = " + sourceEntities.length());
            System.out.println("target: " + join.getTargetEntityName() + " = " + targetEntities.length());
            
            JSONArray joinedEntities = new JSONArray();
        
            // Para cada targetEntity, localizar e inserir as sourceEntities relacionadas
            for (int t = 0; t<targetEntities.length(); t++){            
                String targetKeyValue = targetEntities.getJSONObject(t).getString(targetKeyName);
                boolean foundRelatedEntity = false;
                for (int s = 0; s<sourceEntities.length(); s++){
                    String sourceKeyValue = sourceEntities.getJSONObject(s).getString(sourceKeyName);
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
            entitiesPool.replace(join.getTargetEntityName(), joinedEntities); // acredito que a semântica desse código deva ser: "create_or_replace".
            System.out.println("joined: "+joinedEntities.length());  
        }
    }
    
    
//    // ESTA IMPLEMENTAÇÃO FUNCIONA BEM PARA JOINS BINÁRIOS... MAS NÃO COBRE JOIN COM 3 OU MAIS TABELAS/ENTIDADES
//    // VOU TENTAR IMPLEMENTAR OUTRA VERSÃO...
//    // Código que faz join entre dois arrays de objetos JSON contidos em entitiesPool (join sem otimizações, preciso melhorar esse código).
//    // Trabalho Futuro: procurar algoritmos ou novas estratégias para realizar JOIN.
//    private void executeJoinEntities(HashMap<String,JSONArray> entitiesPool, Command cmd){    
//        // ATENÇÃO: se getBinaryJoinArray tiver 2 ou mais elementos esse código não cobre a situação VERIFICAR, VERIFICAR, VERIFICAR... ACHO QUE PARA ESTRATÉGIA COLGENERATOR TEREMOS EXCEPTION.
//        JSONArray sourceEntities = entitiesPool.get(cmd.getJoinEntities().getBinaryJoinArray().get(0).getSourceEntityName());
//        JSONArray targetEntities = entitiesPool.get(cmd.getJoinEntities().getBinaryJoinArray().get(0).getTargetEntityName());
//        String targetKeyName = cmd.getJoinEntities().getBinaryJoinArray().get(0).getKeyTarget();
//        String sourceKeyName = cmd.getJoinEntities().getBinaryJoinArray().get(0).getKeySource();
//        
//        System.out.println("source: " + cmd.getJoinEntities().getBinaryJoinArray().get(0).getSourceEntityName() + " = " + sourceEntities.length());
//        System.out.println("target: " + cmd.getJoinEntities().getBinaryJoinArray().get(0).getTargetEntityName() + " = " + targetEntities.length());
//        
//        JSONArray joinedEntities = new JSONArray();
//        
//        // Para cada targetEntity, localizar e inserir as sourceEntities relacionadas
//        for (int t = 0; t<targetEntities.length(); t++){            
//            String targetKeyValue = targetEntities.getJSONObject(t).getString(targetKeyName);
//            boolean foundRelatedEntity = false;
//            for (int s = 0; s<sourceEntities.length(); s++){
//                String sourceKeyValue = sourceEntities.getJSONObject(s).getString(sourceKeyName);
//                // Comparando as chaves entre Target e Source...
//                if (targetKeyValue.equalsIgnoreCase(sourceKeyValue)){
//                    // Chaves iguais.
//                    foundRelatedEntity = true;
//                    // cria newJoinedEntity
//                    JSONObject newJoinedEntity = new JSONObject(targetEntities.getJSONObject(t).toString());
//                    // adicionando sourceEntity fields em newJoinedEntity                    
//                    for (String fieldName : sourceEntities.getJSONObject(s).keySet()){
//                        newJoinedEntity.put(fieldName, sourceEntities.getJSONObject(s).get(fieldName));
//                    }
//                    // adiciona newJoinedEntity no array joinedEntities                    
//                    joinedEntities.put(newJoinedEntity);
//                }
//            }
//            // Não achou entidade relacionada em sourceEntities, então insere apenas a entidade targetEntity no array joinedEntities (garantindo o RIGHT JOIN)
//            if (!foundRelatedEntity){
//                JSONObject newJoinedEntity = new JSONObject(targetEntities.getJSONObject(t).toString());
//                // adiciona newJoinedEntity no array joinedEntities
//                joinedEntities.put(newJoinedEntity);
//            }
//        }
//        // substitui a targetEntities pela versão joinedEntities
//        entitiesPool.replace(cmd.getJoinEntities().getBinaryJoinArray().get(0).getTargetEntityName(), joinedEntities);
//        System.out.println("joined: "+joinedEntities.length());        
//    }
    
    // Carrega as entidades do RDB em memória.
    private HashMap<String,JSONArray> loadGraphEntities(DirectedAcyclicGraph<TableVertex, RelationshipEdge> graph){
        HashMap<String,JSONArray> entitiesPool =  new HashMap<>();
        PostgresConnection connection = new PostgresConnection(this.getDbuser(), this.getDbpassword(), this.getDbserver(), this.getDbname());
        connection.openConnection();
        
        // Percorrendo conjunto de vértices (tabela RDB) do 'graph' e carregando como entidades na MEMÓRIA.
        Iterator<TableVertex> vertexIterator = graph.vertexSet().iterator();        
        while (vertexIterator.hasNext()){
            TableVertex vertex = vertexIterator.next();      
            // Para cada vertex...
            try {
                // carrega as entidades sem filtro e sem ordenação (está com desempenho muito ruim)
                //String sql = "Select * From " + vertex.getTableName();
                // faz right join até vértice raiz...
                String sql = buildSQLRightJoinBetweenCurrentAndRootVertices(graph, vertex.getTableName()) + this.sqlFilter; // + " ORDER BY " + GraphUtils.getRootVertex(graph).getPk();
                System.out.println("JDBCCommandExecutor.loadGraphEntities.sql: " + sql);
                
//                if (vertex.getTableName().equals("orders")) sql += " where id_order in (1,2,3)";
//                if (vertex.getTableName().equals("orderlines")) sql += " where prod_id in (9117, 3353, 2778, 4774, 3648, 9523, 6358, 1417, 1567, 1298, 9990, 5130, 6127, 6376,4936,2926,2834,8078,698,5260)";
//                if (vertex.getTableName().equals("products")) sql += " where id_prod in (9117, 3353, 2778, 4774, 3648, 9523, 6358, 1417, 1567, 1298, 9990, 5130, 6127, 6376,4936,2926,2834,8078,698,5260)";
                
                PreparedStatement stmt = connection.getConnection().prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                JSONArray jsonArray = new JSONArray();    
                // carrega em memória os dados do registro (entitiesPool)...
                while (rs.next()){
                    JSONObject obj = new JSONObject();
                    // Transformando o registro em objeto JSON
                    for (int i=1; i<=rs.getMetaData().getColumnCount(); i++){
                        // Inserindo campo-valor no obj JSON
                        obj.put(rs.getMetaData().getColumnName(i), rs.getString(i));
                    }
                    //jsonArray.put(obj);
                    if (obj.keySet().size() > 0) { jsonArray.put(obj); }
                }
                // Adiciona o array de objs JSON no entitiesPool
                entitiesPool.put(vertex.getTableName(), jsonArray);
            } catch (SQLException ex) {
                System.out.println(ex);
            }
        }
        connection.closeConnection();  
        return entitiesPool;
    }
    
    // Retorna comando SQL com a junção entre os vértices currentVertex e rootVertex
    private static String buildSQLRightJoinBetweenCurrentAndRootVertices(DirectedAcyclicGraph<TableVertex, RelationshipEdge> g, String currentVertexName){
        String sql = "";
        TableVertex vertexAux = null;
        // acha o currentVertex de interesse...
        Iterator<TableVertex> vertexIterator = g.vertexSet().iterator();
        while (vertexIterator.hasNext()){
            TableVertex vertex = vertexIterator.next();
            if (vertex.getTableName().equalsIgnoreCase(currentVertexName)){
                vertexAux = vertex;
                break;
            }
        }
//        sql += "Select * From " + vertexAux.getTableName();
        sql += "Select Distinct " + currentVertexName + ".* From " + vertexAux.getTableName();        
        // Percorre grafo do vértice folha até vértice raiz (MESMO CÓDIGO DO COLCOMMANDGENERATOR).
        while (Graphs.successorListOf(g, vertexAux).size() > 0) {
            // Recupera sucessor, assumindo que o vértice corrente tem apenas um sucessor (VERIFICAR ISSO)
            TableVertex sucessor = Graphs.successorListOf(g, vertexAux).get(0);
            // Recupera aresta entre vertex e seu sucessor...
            RelationshipEdge edge = g.getEdge(vertexAux, sucessor);
            // Cria right join entre vertex e sucessor
//            sql += " RIGHT JOIN " + sucessor.getTableName() + " ON " + edge.getKeySource() + " = " + edge.getKeyTarget();
            sql += " RIGHT JOIN " + sucessor.getTableName() + " ON " + edge.getSource().getTableName()+"."+edge.getKeySource() + " = " + edge.getTarget().getTableName()+"."+edge.getKeyTarget();
            // Operação para caminhar em direção ao vertex raiz.
            vertexAux = sucessor;
        }
        return sql;
    }
}