/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testes;

import metamorfose.converters.Converter;
import metamorfose.main.Framework;
import metamorfose.map.EntityMap;
import metamorfose.model.Entity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author Evandro
 */
public class TesteSimcaqMatriculas {

    public static void main(String[] args) {
        EntityMap mapMat2013 = getMappingNColunas(0);

        Framework.addEntityMapQueue(mapMat2013);

        Dataset<Row> csvRows = Framework.getRecordsFromCSV("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_4000k.CSV");

        Dataset<Row> convertedRows = Framework.executeTransformationsQueue(csvRows);

        Framework.saveDatasetToJDBC(convertedRows, "simcaq", "mats", false);
    }

    public static EntityMap getMapping9Colunas() {
        EntityMap map = new EntityMap(
                "CSV_TO_POSTGRES",
                new Entity("postgres"),
                new Entity("CSV"),
                new Converter());

        map.mapFields("ano_censo", "INT", "ANO_CENSO", "STRING", "", "");
        map.mapFields("id", "INT", "PK_COD_MATRICULA", "STRING", "", "");
        map.mapFields("cod_aluno", "BIGINT", "FK_COD_ALUNO", "STRING", "", "");
        map.mapFields("nasc_dia", "INT", "NU_DIA", "STRING", "", "");
        map.mapFields("nasc_mes", "INT", "NU_MES", "STRING", "", "");
        map.mapFields("nasc_ano", "INT", "NU_ANO", "STRING", "", "");
        map.mapFields("idade_referencia", "INT", "NUM_IDADE_REFERENCIA", "STRING", "", "");
        map.mapFields("idade", "INT", "NUM_IDADE", "STRING", "", "");
        map.mapFields("sexo", "INT", "TP_SEXO", "STRING", "com.kuszera.simcaq.transformations.SexoTransformation", "");

        map.mapFields("duracao", "INT", "NU_DUR_ESCOLARIZACAO", "STRING", "", "");

        return map;
    }

    public static EntityMap getMapping20Colunas() {
        EntityMap map = new EntityMap(
                "CSV_TO_POSTGRES",
                new Entity("postgres"),
                new Entity("CSV"),
                new Converter());

        map.mapFields("ano_censo", "INT", "ANO_CENSO", "STRING", "", "");
        map.mapFields("id", "INT", "PK_COD_MATRICULA", "STRING", "", "");
        map.mapFields("cod_aluno", "BIGINT", "FK_COD_ALUNO", "STRING", "", "");
        map.mapFields("nasc_dia", "INT", "NU_DIA", "STRING", "", "");
        map.mapFields("nasc_mes", "INT", "NU_MES", "STRING", "", "");
        map.mapFields("nasc_ano", "INT", "NU_ANO", "STRING", "", "");
        map.mapFields("idade_referencia", "INT", "NUM_IDADE_REFERENCIA", "STRING", "", "");
        map.mapFields("idade", "INT", "NUM_IDADE", "STRING", "", "");
        map.mapFields("sexo", "INT", "TP_SEXO", "STRING", "com.kuszera.simcaq.transformations.SexoTransformation", "");

        map.mapFields("cor_raca_id", "INT", "TP_COR_RACA", "STRING", "", "");
        map.mapFields("nacionalidade", "INT", "TP_NACIONALIDADE", "STRING", "", "");
        map.mapFields("cod_pais_origem", "INT", "FK_COD_PAIS_ORIGEM", "STRING", "", "");
        map.mapFields("cod_estado_nasc", "INT", "FK_COD_ESTADO_NASC", "STRING", "", "");
        map.mapFields("cod_municipio_nasc", "INT", "FK_COD_MUNICIPIO_DNASC", "STRING", "", "");
        map.mapFields("cod_estado_atual", "INT", "FK_COD_ESTADO_END", "STRING", "", "");
        map.mapFields("cod_municipio_atual", "INT", "FK_COD_MUNICIPIO_END", "STRING", "", "");
        map.mapFields("zona_residencial", "INT", "ID_ZONA_RESIDENCIAL", "STRING", "", "");
        map.mapFields("aula_outro_local", "INT", "ID_TIPO_ATENDIMENTO", "STRING", "", "");
        map.mapFields("transporte_escolar_publico", "BOOLEAN", "ID_N_T_E_P", "STRING", "", "");
        map.mapFields("responsavel_transp", "INT", "ID_RESPONSAVEL_TRANSPORTE", "STRING", "", "");
        map.mapFields("transporte_vans_kombi", "BOOLEAN", "ID_TRANSP_VANS_KOMBI", "STRING", "", "");

        return map;
    }

    // Copia um número N de mapeamentos de outro objeto EntityMap.
    public static EntityMap getMappingNColunas(int numColunasCSV) {

        return TesteFramework.getMappingFromCSV(numColunasCSV);

    }

}
