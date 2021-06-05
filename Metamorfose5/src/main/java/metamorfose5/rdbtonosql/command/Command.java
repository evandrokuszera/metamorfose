/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command;

import metamorfose5.rdbtonosql.command.join_spec.MultipleJoinSpec;
import metamorfose5.map.Mapping;

/**
 *
 * @author Evandro
 */
public class Command {
    private MultipleJoinSpec joinEntities;
    private Mapping mapping;
    private boolean mapreduceOperation;

    public Command(MultipleJoinSpec joinEntities, Mapping mapping, boolean mapreduceOperation) {
        this.joinEntities = joinEntities;
        this.mapping = mapping;
        this.mapreduceOperation = mapreduceOperation;
    }
    
    public boolean isMapreduceOperation() {
        return mapreduceOperation;
    }

    public void setMapreduceOperation(boolean mapreduceOperation) {
        this.mapreduceOperation = mapreduceOperation;
    }
    
    public MultipleJoinSpec getJoinEntities() {
        return joinEntities;
    }

    public void setJoinEntities(MultipleJoinSpec joinEntities) {
        this.joinEntities = joinEntities;
    }

    public Mapping getMapping() {
        return mapping;
    }

    public void setMapping(Mapping mapping) {
        this.mapping = mapping;
    }   

    @Override
    public String toString() {
        return joinEntities + "\n" + mapping;
    }
    
    
}
