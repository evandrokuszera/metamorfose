/*
 * Lê arquivo .CSV linha a linha.
 * Altera o caracter separador de cada linha.
 * Grava as linhas em outro arquivo.
 */
package metamorfose.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *
 * @author Evandro
 */
public class ChangeCharacterDelimiterCSV {
    
    public static void main(String[] args) {
        String in = "D:\\DADOS\\2. Doutorado - UFPR\\Letícia\\1. RDB-to-NoSQL\\3. SimCaq\\Amostra de Dados Original - SIMCAQ\\2013_MATRICULA_SUL_100.CSV";
        String out = "D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_100.CSV";
        ChangeCharacterDelimiterCSV.change(in, out);
    }    
    
    public static void change(String path_in, String path_out){
        try {
            FileReader reader = new FileReader(path_in);
            BufferedReader bReader = new BufferedReader(reader);
            
            FileWriter writer = new FileWriter(path_out);
            PrintWriter pWriter = new PrintWriter(writer);
            
            String linha = bReader.readLine();
            long i = 1, teto=9999;
            while (linha != null){
                String nLinha = linha.replace("|", ",");
                
                //if (i==1) pWriter.println(getHeader("D:\\notaql-dados\\Amostra_SIMCAQ\\2013_MATRICULA_SUL_1k.CSV"));
                
                
                pWriter.println(nLinha);
                linha = bReader.readLine();
                
                if (i > teto){
                    System.out.println("Linha: " + i);
                    teto += 100000;
                }
                i++;
            }
            System.out.println("Total de linhas: " + i);
            reader.close();
            writer.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }
    
    public static String getHeader(String file){
        String header = "";
        try {
            FileReader reader = new FileReader(file);
            BufferedReader bReader = new BufferedReader(reader);            
                       
            String linha = bReader.readLine();
            
            if (linha != null){
                header = linha;
            }
            
            reader.close();
            
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        return header.replace("|", ",");
    }
    
}
