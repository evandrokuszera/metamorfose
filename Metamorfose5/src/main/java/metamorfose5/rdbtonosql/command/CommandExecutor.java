/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command;

import dag.model.RelationshipEdge;
import dag.model.TableVertex;
import java.util.ArrayList;
import org.jgrapht.graph.DirectedAcyclicGraph;

/**
 *
 * @author Evandro
 */
public abstract class CommandExecutor {
    private DirectedAcyclicGraph<TableVertex, RelationshipEdge> graph;    
    private ArrayList<Command> commands;
    private String dbname;
    private String dbuser;
    private String dbpassword;
    private String dbserver;

    public CommandExecutor() {
        this.dbname = "metamorfose";
        this.dbuser = "postgres";
        this.dbpassword = "123456";
        this.dbserver = "localhost";
    }

    public void setDBConnectionProperties(String dbserver, String dbname, String dbuser, String dbpassword) {
        this.dbserver = dbserver;
        this.dbname = dbname;
        this.dbuser = dbuser;
        this.dbpassword = dbpassword;
    }

    public String getDbname() {
        return dbname;
    }

    public String getDbuser() {
        return dbuser;
    }

    public String getDbpassword() {
        return dbpassword;
    }

    public String getDbserver() {
        return dbserver;
    }
    
    public void setCommands(ArrayList<Command> commands) {
        this.commands = commands;
    }

    public ArrayList<Command> getCommands() {
        return commands;
    }

    public void setGraph(DirectedAcyclicGraph<TableVertex, RelationshipEdge> graph) {
        this.graph = graph;
    }

    public DirectedAcyclicGraph<TableVertex, RelationshipEdge> getGraph() {
        return graph;
    }
    
    public void printCommands(){
        for (Command cmd : this.getCommands()){
            System.out.println(cmd);
        }
    }
    
    public abstract void execute();
    
}
