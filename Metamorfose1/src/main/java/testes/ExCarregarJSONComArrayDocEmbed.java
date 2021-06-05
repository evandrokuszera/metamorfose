// Exemplo de leitura de JSON com array de documentos embutidos e posterior EXPLOSAO do array para consulta via campos do array.
//
// JSON original, com array 'itens', sendo que cada elemento de 'itens1 tem os campos: desc, id_item, pedido_id, qtde, valor.
//+----------+---------+------------------------------------------------------------+
//|data      |id_pedido|itens                                                       |
//+----------+---------+------------------------------------------------------------+
//|26/11/2017|4        |[[Mouse,7,4,4,45], [Mouse,7,4,4,45]]                        |
//|24/11/2017|2        |[[Teclado,3,2,1,55], [Teclado,3,2,1,55]]                    |
//|25/11/2017|3        |[[Memoria 4GB,5,3,2,100], [Memoria 4GB,5,3,2,100]]          |
//|23/11/2017|1        |[[Mouse,2,1,1,45], [Mouse,2,1,1,45], [Pendrive 4G,1,1,2,10]]|
//+----------+---------+------------------------------------------------------------+
//
// JSON resultante da operação EXPLOSE no campo 'itens'. Cada documento embutido virou uma linha, aumentando o número de documentos JSON da lista.
//+----------+---------+-----------------------+
//|data      |id_pedido|itens                  |
//+----------+---------+-----------------------+
//|26/11/2017|4        |[Mouse,7,4,4,45]       |
//|26/11/2017|4        |[Mouse,7,4,4,45]       |
//|24/11/2017|2        |[Teclado,3,2,1,55]     |
//|24/11/2017|2        |[Teclado,3,2,1,55]     |
//|25/11/2017|3        |[Memoria 4GB,5,3,2,100]|
//|25/11/2017|3        |[Memoria 4GB,5,3,2,100]|
//|23/11/2017|1        |[Mouse,2,1,1,45]       |
//|23/11/2017|1        |[Mouse,2,1,1,45]       |
//|23/11/2017|1        |[Pendrive 4G,1,1,2,10] |
//+----------+---------+-----------------------+
//
// Após explosão do array é possível criar consulta usando como filtro o nome dos campos do array.
// Resultado da consulta (select * from pedidos where itens.qtde = 4) sobre o Dataset acima:
//
//+----------+---------+----------------+
//|data      |id_pedido|itens           |
//+----------+---------+----------------+
//|26/11/2017|4        |[Mouse,7,4,4,45]|
//|26/11/2017|4        |[Mouse,7,4,4,45]|
//+----------+---------+----------------+

package testes;

import metamorfose.main.Framework;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 *
 * @author Evandro
 */
public class ExCarregarJSONComArrayDocEmbed {

    public static void main(String[] args) {

        Dataset<Row> json = Framework.getSparkSession().read().json("D:\\notaql-dados\\pedidoItens.json");
        
        json.createOrReplaceTempView("pedidos");
        //json.printSchema();
        Framework.getSparkSession().sql("select * from pedidos").show(false);

        Dataset<Row> explosedJson = json.withColumn("itens", functions.explode(json.col("itens")));

        explosedJson.createOrReplaceTempView("pedidos2");
        //explosedJson.printSchema();
        Framework.getSparkSession().sql("select * from pedidos2").show(false);
        
        // Filtrando JSON pelo nome de um dos documentos embutidos no array de itens!!!!
        //Framework.getSparkSession().sql("select * from pedidos2 where itens.qtde = 4").show(false);
        //Framework.getSparkSession().sql("select id_pedido, itens.desc, itens.valor from pedidos2").show(false);
        Framework.getSparkSession().sql("select id_pedido, sum(itens.valor) as total from pedidos2 group by id_pedido").show(false);

    }
}
