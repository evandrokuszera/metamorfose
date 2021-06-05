/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.rdbtonosql.command.join_spec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author Evandro
 */
public class MultipleJoinSpec extends JoinSpec {
    private String groupingKey;

    public String getGroupingKey() {
        return groupingKey;
    }

    public void setGroupingKey(String groupingKey) {
        this.groupingKey = groupingKey;
    }
    
    
    
    private String joinedEntityPK; // pk que identifica os registros depois da junção entre entidades.
    private String joinedEntityName; // nome da entidade criada a partir da operação de junção.
    private ArrayList<BinaryJoinSpec> binaryJoinArray;

    public MultipleJoinSpec(String joinedEntityName, String joinedEntityPK) {
        this.joinedEntityPK = joinedEntityPK;
        this.joinedEntityName = joinedEntityName;
        this.binaryJoinArray = new ArrayList<>();
    }
    
    public MultipleJoinSpec(String joinedEntityName, String joinedEntityPK, String groupingKey) {
        this.joinedEntityPK = joinedEntityPK;
        this.joinedEntityName = joinedEntityName;
        this.binaryJoinArray = new ArrayList<>();
        
        this.groupingKey = groupingKey;
    }
    
    public void addBinaryJoin(BinaryJoinSpec binaryJoin){
        this.binaryJoinArray.add(binaryJoin);
    }
    
    public void addBinaryJoin(String joinedEntityName, String sourceEntityName, String targetEntityName, String keySource, String keyTarget){
        BinaryJoinSpec joinSpec = new BinaryJoinSpec(joinedEntityName, sourceEntityName, targetEntityName, keySource, keyTarget);
        this.binaryJoinArray.add(joinSpec);
    }

    public String getJoinedEntityPK() {
        return joinedEntityPK;
    }

    public void setJoinedEntityPK(String joinedEntityPK) {
        this.joinedEntityPK = joinedEntityPK;
    }

    public String getJoinedEntityName() {
        return joinedEntityName;
    }

    public void setJoinedEntityName(String joinedEntityName) {
        this.joinedEntityName = joinedEntityName;
    }

    public ArrayList<BinaryJoinSpec> getBinaryJoinArray() {
        return binaryJoinArray;
    }

    public void setBinaryJoinArray(ArrayList<BinaryJoinSpec> binaryJoinArray) {
        this.binaryJoinArray = binaryJoinArray;
    }

    @Override
    public String toString() {
        String msg = "MultipleJoinSpec{\n";
        msg += "  joinedEntityName=" + joinedEntityName + ", ";
        msg += "joinedEntityPK=" + joinedEntityPK + ", ";
        msg += "GroupingKey=" + groupingKey+",\n";
        for (BinaryJoinSpec join : this.getBinaryJoinArray()){
            msg += "  " + join + "\n";
        }
        msg += "}";
        
        return msg;
    }
    
    // Bulding a SQL command according with the BinaryJoinSpec objects.
    //  This SQL command join all the tables and projects only the leaf fields and root fields of graph (graph as a tree).
    public String getSQLCommand(){
        
        String FIELDS = "";
        String TABLES = "";
        String FILTERS = "";
        
        // Building FIELDS command
        int lastJoinOperationIndex = this.binaryJoinArray.size() - 1;
        
//        FIELDS = this.binaryJoinArray.get(lastJoinOperationIndex).getTargetEntityName() + ".*, ";
//        FIELDS += this.binaryJoinArray.get(0).getSourceEntityName() + ".* ";
        FIELDS = " * ";
        
        // Building TABLES command
        Set<String> tables = new HashSet<>();
        for (BinaryJoinSpec joinSpec : this.binaryJoinArray){
            tables.add(joinSpec.getSourceEntityName());
            tables.add(joinSpec.getTargetEntityName());
        }
        
        Iterator<String> tablesAsIterator = tables.iterator();
        while (tablesAsIterator.hasNext()){
            String VIRGULA = ", ";
            if (TABLES.length() == 0) VIRGULA = "";
               TABLES += VIRGULA + tablesAsIterator.next();

        }        
        
        // Building FILTERS command
        for (BinaryJoinSpec joinSpec : this.binaryJoinArray){
            String AND = " AND ";
            if (FILTERS.length() == 0) AND = "";
            
            FILTERS += " " + AND + joinSpec.getSourceEntityName()+ "." + joinSpec.getKeySource()+ " = " + joinSpec.getTargetEntityName()+ "." + joinSpec.getKeyTarget();
        }
        
        // Building final SQL command.
        String sql = "SELECT "+FIELDS+" FROM "+TABLES+" WHERE " + FILTERS;
                
        return sql;
    }    
    
    // TESTE
    // Monta e exibe uma instância de MultipleJoinEntitiesOperation
//    public static void main(String[] args) {
//        
//        MultipleJoinSpec mJoin = new MultipleJoinSpec("customer", "id_customer");
//                
//        mJoin.addMutipleJoinEntitiesOperation("product_item", "product", "item", "id_product", "product_id");
//        mJoin.addMutipleJoinEntitiesOperation("item_order", "item", "order", "order_id", "id_order");
//        mJoin.addMutipleJoinEntitiesOperation("customer_order", "order", "customer", "customer_id", "id_customer");
//        
//        System.out.println(mJoin);
//        
//        System.out.println("SQL: " + mJoin.getSQLCommand());
//        
//        
//    }
    
}
