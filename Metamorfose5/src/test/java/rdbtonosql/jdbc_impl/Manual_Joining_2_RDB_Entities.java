/*
   This class shows how to join to RDB entities using the Metamorfose.
   The result entities are printed in the console.
 */
package rdbtonosql.jdbc_impl;

import metamorfose5.core.jdbc_impl.JDBCMetamorfose;

/**
 *
 * @author Evandro
 */
public class Manual_Joining_2_RDB_Entities {
    public static void main(String[] args) {
        JDBCMetamorfose mf = new JDBCMetamorfose();
        
        mf.load().fromJDBC("customers", "localhost", "ds2_10mb", "postgres", "123456", "customers");
        mf.load().fromJDBC("orders", "localhost", "ds2_10mb", "postgres", "123456", "orders");
        mf.joinEntities("customers_orders", "customers", "orders", "id_customer", "customerid");
        
        for (int i=0; i<10; i++){
            System.out.println(mf.getEntity("customers_orders").getData().get(i));
        }
    }
}
