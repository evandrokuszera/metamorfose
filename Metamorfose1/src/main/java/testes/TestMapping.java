/*
 * Teste da classe EntityMap.
 *  - objetivo: mostrar como estabelecer mapeamento entre entidades e exibir o resultado no console.
 *  - há duas assinaturas para o método MapFields:
 *    - uma que aceita os objetos Entities, o que força a instanciação e configuração dos mesmos previamente.
 *    - uma que aceita diretamente os nomes dos campos, tipos de dados, transformer e converter.
 *      * este cria objetos Entities para estabelecer os mapeamentos.
 *    Abaixo são apresentados dois exemplos para criar mapeamentos.
 */
package testes;

import metamorfose.converters.Converter;
import metamorfose.map.EntityMap;
import metamorfose.map.FieldMap;
import metamorfose.model.Entity;

/**
 *
 * @author Evandro
 */
public class TestMapping {
    
    public static void main(String[] args) {
        System.out.println("Metodo 01");
        metodo01();
        System.out.println("\nMetodo 02:");
        metodo02();
    }
    
    public static void metodo01() {
        
        Entity source = new Entity();
        Entity target = new Entity();
                
        source.addField("id", "INT");
        source.addField("name", "STRING");
        source.addField("salary", "DOUBLE");
        
        target.addField("id_emp", "INT");
        target.addField("name_emp", "STRING");
        target.addField("salary_emp", "DOUBLE");
        
        EntityMap mapping = new EntityMap("TESTE", target, source, new Converter());
        
        mapping.mapFields("id_emp", "id", null);
        mapping.mapFields("name_emp", "name", null);
        mapping.mapFields("salary_emp", "salary", null);       
        
        // imprimindo mapeamentos...
        System.out.println("TARGET_ENTITY : SOURCE_ENTITY : TRANSFORMATION");
        for (FieldMap m : mapping.getFieldMappings()){
            System.out.println(m.getTargetFieldName() + " : " + m.getSourceFieldName() + " : " + m.getTransformationName());
        }
    }  
    
    
    public static void metodo02() {    
        
        EntityMap mapping = new EntityMap("TESTE", new Entity("destino"), new Entity("origem"), new Converter());
        
        mapping.mapFields("id_emp", "INT", "id", "INT", "", "");
        mapping.mapFields("name_emp", "STRING", "name", "STRING", "", "");
        mapping.mapFields("salary_emp", "DOUBLE", "salary", "DOUBLE", "", "");       
        
        // imprimindo mapeamentos...
        System.out.println("TARGET_ENTITY : SOURCE_ENTITY : TRANSFORMATION");
        for (FieldMap m : mapping.getFieldMappings()){
            System.out.println(m.getTargetFieldName() + " : " + m.getSourceFieldName() + " : " + m.getTransformationName());
        }
    }    
}
