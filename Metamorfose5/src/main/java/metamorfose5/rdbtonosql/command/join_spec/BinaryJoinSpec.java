/*
 * This class represents a binary join operation between two entities.
 *  joinEntityName represents the new entity name of join operation.
 */
package metamorfose5.rdbtonosql.command.join_spec;

/**
 *
 * @author Evandro
 */
public class BinaryJoinSpec extends JoinSpec{
    private String joinEntityName;
    private String sourceEntityName;
    private String targetEntityName;
    private String keySource;
    private String keyTarget;
    
    public BinaryJoinSpec(String joinEntityName, String sourceEntityName, String targetEntityName, String keySource, String keyTarget) {
        this.joinEntityName = joinEntityName;
        this.sourceEntityName = sourceEntityName;
        this.targetEntityName = targetEntityName;
        this.keySource = keySource;
        this.keyTarget = keyTarget;
    }

    public String getJoinEntityName() {
        return joinEntityName;
    }

    public void setJoinEntityName(String joinEntityName) {
        this.joinEntityName = joinEntityName;
    }

    public String getSourceEntityName() {
        return sourceEntityName;
    }

    public void setSourceEntityName(String sourceEntityName) {
        this.sourceEntityName = sourceEntityName;
    }

    public String getTargetEntityName() {
        return targetEntityName;
    }

    public void setTargetEntityName(String targetEntityName) {
        this.targetEntityName = targetEntityName;
    }

    public String getKeySource() {
        return keySource;
    }

    public void setKeySource(String keySource) {
        this.keySource = keySource;
    }

    public String getKeyTarget() {
        return keyTarget;
    }

    public void setKeyTarget(String keyTarget) {
        this.keyTarget = keyTarget;
    }

    @Override
    public String toString() {
        return "JoinEntitiesOperation{" + "joinEntityName=" + joinEntityName + ", sourceEntityName=" + sourceEntityName + ", targetEntityName=" + targetEntityName + ", keySource=" + keySource + ", keyTarget=" + keyTarget + '}';
    }

    @Override
    public String getSQLCommand() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
