/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.gui;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import javax.swing.DefaultListModel;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import metamorfose.gui.model.SparkDatasetInstance;
import metamorfose.main.Framework;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Evandro
 */
public class MetamorfoseJFrame extends javax.swing.JFrame {

    private SparkSession sparkSession;
    private String filePath;
    private Dataset<Row> dataset;
    private Dataset<Row> sqlResults;

    DefaultListModel listModel = new DefaultListModel();
    DefaultTableModel tableModel;

    /**
     * Creates new form SimcaqJFrame
     */
    public MetamorfoseJFrame() {
        initComponents();
        
//        ImageIcon icone = new ImageIcon(this.getClass().getResource("db.jpg"));
//        this.setIconImage(icone.getImage());

//        sparkSession = SparkSession
//                .builder()
//                .appName("Simcaq")
//                .config("spark.master", "local")
//                .getOrCreate();
        sparkSession = Framework.getSparkSession();

        lstDataset.setModel(listModel);
        tableModel = (DefaultTableModel) tblResults.getModel();
    }

    private void addOrEditDatasetMapping() {
        if (lstDataset.getSelectedIndex() != -1) {
            SparkDatasetInstance selectedDatasetInstance = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());

            SchemaJDialog schemaDialog = new SchemaJDialog(this, true);

            // se o dataset selecionado tem um EntityMap associado é possível visualizar a definição dos mapeamentos
            //  essa situação ocorre quando o dataset selecionado é a origem de outro dataset transformado.
            if (selectedDatasetInstance.getEntityMap() == null) {
                schemaDialog.setSchema(selectedDatasetInstance.getDataset().schema());
                schemaDialog.loadTableFromSchema();
            } else {
                schemaDialog.setEntityMap(selectedDatasetInstance.getEntityMap());
                schemaDialog.loadTableFromEntityMap();
            }

            schemaDialog.setTitle("View Dataset Schema - ["+selectedDatasetInstance.getTempViewName()+"]");
            schemaDialog.setVisible(true);

            if (!schemaDialog.isCanceled()) {

                //this.sparkSession = Framework.getSparkSession();                           
                // seta o mapeamento do esquema de origem para destino.
                //Framework.setMapping(schemaDialog.getEntityMap());
                Framework.addEntityMapQueue(schemaDialog.getEntityMap());
                
                // transforma...
                //Dataset<Row> rowsTransformed = Framework.executeTransformations(  selectedDatasetInstance.getDataset()  );
                Dataset<Row> rowsTransformed = Framework.executeTransformationsQueue(selectedDatasetInstance.getDataset()); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // ********************************************PILHA*****************************************************************

                String datasetTempViewName;

                if (schemaDialog.isNewDataset()) { // registra na lista o Novo Dataset para visualização e seleção                        

                    SparkDatasetInstance dataset = new SparkDatasetInstance(rowsTransformed, "Dataset", schemaDialog.getNewDatasetName());
                    listModel.addElement(dataset);
                    datasetTempViewName = dataset.getTempViewName();
                    // atrelando o mapeamento realizado sobre o dataset original, dessa forma o usuário pode consultar o último mapeamento realizado.
                    selectedDatasetInstance.setEntityMap(schemaDialog.getEntityMap());

                } else { // atualiza o Dataset Existente na lista para visualização e seleção (o mapeamento altera o dataset selecionado)

                    selectedDatasetInstance.setDataset(rowsTransformed);
                    selectedDatasetInstance.setEntityMap(null); // como o dataset seleciona é modificado, o EntityMap antigo não tem mais validade sobre os novos campos transformados.
                    datasetTempViewName = selectedDatasetInstance.getTempViewName();

                }

                // registra Dataset para consulta via Apache Spark...
                rowsTransformed.createOrReplaceTempView(datasetTempViewName);

                JOptionPane.showMessageDialog(this, "Mapping Applied!");
            }

        } else {
            JOptionPane.showMessageDialog(this, "No dataset was selected.");
        }
    }

    private void removeAllContentFromTableResult() {
        tableModel.setColumnCount(0);
        while (tableModel.getRowCount() > 0) {
            tableModel.removeRow(0);
        }
    }

    private void loadTableFromDataset(Dataset<Row> rows) {

        removeAllContentFromTableResult();

        // Configurando o cabeçalho da tabela.
        tableModel.setColumnCount(0);
        int numFields = rows.schema().fieldNames().length;
        for (int i = 0; i < numFields; i++) {
            tableModel.addColumn(rows.schema().fieldNames()[i]);            
        }
        
        

        // Carregando os dados do dataset na tabela.
        rows.limit(Integer.parseInt(txtNumeroRows.getText())).collectAsList().forEach(r -> {
            Object[] tableRow = new Object[r.size()];

            for (int i = 0; i < r.size(); i++) {
                tableRow[i] = r.get(i);
            }

            tableModel.addRow(tableRow);
        });

    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        lblResultadoConsulta = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        jScrollPane2 = new javax.swing.JScrollPane();
        txtConsulta = new javax.swing.JTextArea();
        btnExecutar = new javax.swing.JButton();
        jScrollPane1 = new javax.swing.JScrollPane();
        lstDataset = new javax.swing.JList<>();
        jLabel5 = new javax.swing.JLabel();
        btnAdicionarDataSet = new javax.swing.JButton();
        btnRemoverDataset = new javax.swing.JButton();
        btnAdicionarMapeamento = new javax.swing.JButton();
        btnClearQuery = new javax.swing.JButton();
        btnQueryResultToDataset = new javax.swing.JButton();
        txtNumeroRows = new javax.swing.JTextField();
        jScrollPane4 = new javax.swing.JScrollPane();
        tblResults = new javax.swing.JTable();
        jMenuBar1 = new javax.swing.JMenuBar();
        mnuAcoes = new javax.swing.JMenu();
        mniUnirDatasets = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JPopupMenu.Separator();
        mniSaveToJSON = new javax.swing.JMenuItem();
        mniSaveToJDBC = new javax.swing.JMenuItem();
        mniSaveToCSV = new javax.swing.JMenuItem();
        jSeparator2 = new javax.swing.JPopupMenu.Separator();
        mniSair = new javax.swing.JMenuItem();
        mnuUtilitarios = new javax.swing.JMenu();
        mniSparkGUI = new javax.swing.JMenuItem();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("Metamorfose Framework 1.0");
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                formWindowClosing(evt);
            }
        });

        lblResultadoConsulta.setText("Results:");

        jLabel3.setText("Query:");

        txtConsulta.setColumns(20);
        txtConsulta.setRows(5);
        jScrollPane2.setViewportView(txtConsulta);

        btnExecutar.setText("Execute");
        btnExecutar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnExecutarActionPerformed(evt);
            }
        });

        lstDataset.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                lstDatasetMouseClicked(evt);
            }
        });
        jScrollPane1.setViewportView(lstDataset);

        jLabel5.setText("Datasets:");

        btnAdicionarDataSet.setText("Add");
        btnAdicionarDataSet.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnAdicionarDataSetActionPerformed(evt);
            }
        });

        btnRemoverDataset.setText("Remove");
        btnRemoverDataset.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnRemoverDatasetActionPerformed(evt);
            }
        });

        btnAdicionarMapeamento.setText("Add Mapping");
        btnAdicionarMapeamento.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnAdicionarMapeamentoActionPerformed(evt);
            }
        });

        btnClearQuery.setText("Clear");
        btnClearQuery.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnClearQueryActionPerformed(evt);
            }
        });

        btnQueryResultToDataset.setText("To Dataset");
        btnQueryResultToDataset.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnQueryResultToDatasetActionPerformed(evt);
            }
        });

        txtNumeroRows.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtNumeroRows.setText("20");

        tblResults.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {},
                {},
                {},
                {}
            },
            new String [] {

            }
        ));
        tblResults.setAutoResizeMode(javax.swing.JTable.AUTO_RESIZE_OFF);
        tblResults.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tblResultsMouseClicked(evt);
            }
        });
        jScrollPane4.setViewportView(tblResults);

        mnuAcoes.setText("Actions");

        mniUnirDatasets.setText("Dataset Union");
        mniUnirDatasets.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniUnirDatasetsActionPerformed(evt);
            }
        });
        mnuAcoes.add(mniUnirDatasets);
        mnuAcoes.add(jSeparator1);

        mniSaveToJSON.setText("Save to JSON");
        mniSaveToJSON.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSaveToJSONActionPerformed(evt);
            }
        });
        mnuAcoes.add(mniSaveToJSON);

        mniSaveToJDBC.setText("Save to JDBC");
        mniSaveToJDBC.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSaveToJDBCActionPerformed(evt);
            }
        });
        mnuAcoes.add(mniSaveToJDBC);

        mniSaveToCSV.setText("Save to CSV");
        mniSaveToCSV.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSaveToCSVActionPerformed(evt);
            }
        });
        mnuAcoes.add(mniSaveToCSV);
        mnuAcoes.add(jSeparator2);

        mniSair.setText("Close");
        mniSair.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSairActionPerformed(evt);
            }
        });
        mnuAcoes.add(mniSair);

        jMenuBar1.add(mnuAcoes);

        mnuUtilitarios.setText("Spark");

        mniSparkGUI.setText("Spark GUI");
        mniSparkGUI.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSparkGUIActionPerformed(evt);
            }
        });
        mnuUtilitarios.add(mniSparkGUI);

        jMenuBar1.add(mnuUtilitarios);

        setJMenuBar(jMenuBar1);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(layout.createSequentialGroup()
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel5)
                                    .addGroup(layout.createSequentialGroup()
                                        .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 291, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                            .addComponent(btnAdicionarDataSet, javax.swing.GroupLayout.PREFERRED_SIZE, 112, javax.swing.GroupLayout.PREFERRED_SIZE)
                                            .addComponent(btnAdicionarMapeamento, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 112, javax.swing.GroupLayout.PREFERRED_SIZE)
                                            .addComponent(btnRemoverDataset, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 112, javax.swing.GroupLayout.PREFERRED_SIZE))))
                                .addGap(18, 18, 18)
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel3)
                                    .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 400, Short.MAX_VALUE))
                                .addGap(2, 2, 2))
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                .addComponent(lblResultadoConsulta)
                                .addGap(0, 0, Short.MAX_VALUE)))
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(btnQueryResultToDataset, javax.swing.GroupLayout.DEFAULT_SIZE, 112, Short.MAX_VALUE)
                            .addComponent(btnExecutar, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(btnClearQuery, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(txtNumeroRows)))
                    .addComponent(jScrollPane4))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel3)
                    .addComponent(jLabel5))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(btnExecutar, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnQueryResultToDataset)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnClearQuery)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtNumeroRows, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(jScrollPane1)
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                        .addComponent(btnAdicionarDataSet)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnRemoverDataset)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnAdicionarMapeamento)
                        .addGap(58, 58, 58)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(lblResultadoConsulta)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane4, javax.swing.GroupLayout.DEFAULT_SIZE, 270, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void btnExecutarActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnExecutarActionPerformed

        if (txtConsulta.getText().length() > 0) {
            lblResultadoConsulta.setText("Results (executing...):");
            long inicio = System.currentTimeMillis();
            sqlResults = sparkSession.sql(txtConsulta.getText());
            loadTableFromDataset(sqlResults);
            long fim = System.currentTimeMillis();
            lblResultadoConsulta.setText("Results ("+tblResults.getRowCount()+" rows, "+ tblResults.getColumnCount() + " cols): executed in " + ((fim - inicio) / 1000) + "s.");            
        } else {
            JOptionPane.showMessageDialog(this, "No query inserted.");
        }
    }//GEN-LAST:event_btnExecutarActionPerformed

    private void formWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowClosing
        if (sparkSession != null) {
            sparkSession.close();
            JOptionPane.showMessageDialog(this, "Spark session finished!");
        }
    }//GEN-LAST:event_formWindowClosing

    private void btnAdicionarDataSetActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnAdicionarDataSetActionPerformed

        Object[] options = new Object[2];
        options[0] = "CSV";
        options[1] = "JDBC";

        String entrada = JOptionPane.showInputDialog(this,
                "Enter the data source.",
                "Load dataset",
                JOptionPane.QUESTION_MESSAGE,
                null,
                options,
                options[0]).toString();

        switch (entrada) {
            case "CSV":

                LoadCSVJDialog csvDialog = new LoadCSVJDialog(this, true);
                csvDialog.setVisible(true);

                if (!csvDialog.isCancel()) {
                    Dataset<Row> datasetCSV = sparkSession.read()
                                                            .option("header", "true")
                                                            .option("delimiter", csvDialog.getSeparador())
                                                            .csv(csvDialog.getCsv_path());
                    datasetCSV.createOrReplaceTempView(csvDialog.getDatasetname());

                    SparkDatasetInstance dataset = new SparkDatasetInstance(datasetCSV, "CSV", csvDialog.getDatasetname());
                    listModel.addElement(dataset);
                    JOptionPane.showMessageDialog(this, "CSV loaded!");
                }

                break;
            case "JDBC":

                String databaseName = JOptionPane.showInputDialog("Enter database name:", "metamorfose");
                if (databaseName == null) {
                    return;
                }
                String tableName = JOptionPane.showInputDialog("Enter table name:", "item");
                if (tableName == null) {
                    return;
                }
                String JDBCdatasetName = JOptionPane.showInputDialog("Enter dataset name:", tableName);
                if (JDBCdatasetName == null) {
                    return;
                }

                String msg = "Server: Localhost/Postgres";
                msg += "\nDb: " + databaseName;
                msg += "\nSchema: public";
                msg += "\nTable: " + tableName;
                msg += "\nUser e Password: default.";

                JOptionPane.showMessageDialog(this, msg, "Loading dataset from...", JOptionPane.INFORMATION_MESSAGE);

                Dataset<Row> datasetJDBC = Framework.getRecordsFromJDBC(databaseName, tableName);
                datasetJDBC.createOrReplaceTempView(JDBCdatasetName);

                SparkDatasetInstance dataset = new SparkDatasetInstance(datasetJDBC, "JDBC", JDBCdatasetName);
                listModel.addElement(dataset);
                JOptionPane.showMessageDialog(this, "Dataset JDBC loaded!");

                break;
        }


    }//GEN-LAST:event_btnAdicionarDataSetActionPerformed

    private void btnRemoverDatasetActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnRemoverDatasetActionPerformed
        if (lstDataset.getSelectedIndex() != -1) {
            SparkDatasetInstance selectedDatasetInstance = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());
            sparkSession.sqlContext().dropTempTable(selectedDatasetInstance.getTempViewName());
            listModel.remove(lstDataset.getSelectedIndex());
        } else {
            JOptionPane.showMessageDialog(this, "No dataset was selected.");
        }
    }//GEN-LAST:event_btnRemoverDatasetActionPerformed

    private void lstDatasetMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_lstDatasetMouseClicked
        if (lstDataset.getSelectedIndex() != -1 && evt.getClickCount() == 2) {
            SparkDatasetInstance selectedDatasetInstance = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());

            if (txtConsulta.getText().length() == 0) {
                txtConsulta.setText("SELECT * FROM ");
            }

            txtConsulta.append(selectedDatasetInstance.getTempViewName());
            txtConsulta.setFocusable(true);
        }
    }//GEN-LAST:event_lstDatasetMouseClicked

    private void btnAdicionarMapeamentoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnAdicionarMapeamentoActionPerformed
        addOrEditDatasetMapping();

//        if (lstDataset.getSelectedIndex() != -1) {
//            SparkDatasetInstance selectedDatasetInstance = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());
//
//            SchemaJDialog schemaDialog = new SchemaJDialog(this, true);
//            schemaDialog.setSchema(selectedDatasetInstance.getDataset().schema());
//            schemaDialog.loadTableFromSchema();
//            schemaDialog.setVisible(true);
//
//            if (!schemaDialog.isCanceled()) {
//
//                //this.sparkSession = Framework.getSparkSession();                           
//                // seta o mapeamento do esquema de origem para destino.
//                //Framework.setMapping(schemaDialog.getEntityMap());
//                Framework.addEntityMapQueue(schemaDialog.getEntityMap());
//
//                // transforma...
//                //Dataset<Row> rowsTransformed = Framework.executeTransformations(  selectedDatasetInstance.getDataset()  );
//                Dataset<Row> rowsTransformed = Framework.executeTransformationsQueue(selectedDatasetInstance.getDataset()); // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//                // ********************************************PILHA*****************************************************************
//
//                String datasetTempViewName;
//
//                if (schemaDialog.isNewDataset()) { // registra na lista o Novo Dataset para visualização e seleção                        
//
//                    SparkDatasetInstance dataset = new SparkDatasetInstance(rowsTransformed, "Dataset", schemaDialog.getNewDatasetName());
//                    listModel.addElement(dataset);
//                    datasetTempViewName = dataset.getTempViewName();
//
//                } else { // atualiza o Dataset Existente na lista para visualização e seleção
//
//                    selectedDatasetInstance.setDataset(rowsTransformed);
//                    datasetTempViewName = selectedDatasetInstance.getTempViewName();
//
//                }
//
//                // registra Dataset para consulta via Apache Spark...
//                rowsTransformed.createOrReplaceTempView(datasetTempViewName);
//
//                JOptionPane.showMessageDialog(this, "Mapeamento Aplicado!");
//            }
//
//        } else {
//            JOptionPane.showMessageDialog(this, "Nenhum dataset foi selecionado.");
//        }
    }//GEN-LAST:event_btnAdicionarMapeamentoActionPerformed

    private void btnClearQueryActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnClearQueryActionPerformed
        removeAllContentFromTableResult();
        txtConsulta.setText("");
        lblResultadoConsulta.setText("Results:");
    }//GEN-LAST:event_btnClearQueryActionPerformed

    private void btnQueryResultToDatasetActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnQueryResultToDatasetActionPerformed
        String datasetTempViewName = JOptionPane.showInputDialog("Enter the new dataset name.");
        if (datasetTempViewName != null) {

            SparkDatasetInstance dataset = new SparkDatasetInstance(this.sqlResults, "Dataset", datasetTempViewName);
            listModel.addElement(dataset);
            this.sqlResults.createOrReplaceTempView(datasetTempViewName);

        }
    }//GEN-LAST:event_btnQueryResultToDatasetActionPerformed

    private void mniUnirDatasetsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniUnirDatasetsActionPerformed

        String datasetTempViewName = JOptionPane.showInputDialog("Enter the new dataset name.");
        if (datasetTempViewName != null) {

            ArrayList<Dataset<Row>> datasets = new ArrayList();

            for (int i : lstDataset.getSelectedIndices()) {

                SparkDatasetInstance datasetSelected = (SparkDatasetInstance) listModel.get(i);

                datasets.add(datasetSelected.getDataset());

            }

            Dataset<Row> union = datasets.get(0);

            for (int i = 1; i < datasets.size(); i++) {
                union = union.union(datasets.get(i));
            }

            SparkDatasetInstance dataset = new SparkDatasetInstance(union, "Dataset", datasetTempViewName);
            listModel.addElement(dataset);
            union.createOrReplaceTempView(datasetTempViewName);

        }


    }//GEN-LAST:event_mniUnirDatasetsActionPerformed

    private void mniSparkGUIActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSparkGUIActionPerformed
        try {
            Runtime.getRuntime().exec("cmd.exe /C start iexplore.exe http://localhost:4040");
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }//GEN-LAST:event_mniSparkGUIActionPerformed

    private void mniSaveToJSONActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSaveToJSONActionPerformed
        if (lstDataset.getSelectedIndex() == -1) {
            JOptionPane.showMessageDialog(this, "No dataset was selected.");
        } else {
            SparkDatasetInstance datasetSelected = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());

            JFileChooser chooser = new JFileChooser();
            FileNameExtensionFilter filter = new FileNameExtensionFilter("JSON Files", "json");
            chooser.setCurrentDirectory(new File("D:\\metamorfose\\demo"));
            chooser.setFileFilter(filter);
            int returnVal = chooser.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                Framework.saveRecordsToJSON(datasetSelected.getDataset(), chooser.getSelectedFile().getAbsolutePath());
                JOptionPane.showMessageDialog(this, "JSON saved at " + chooser.getSelectedFile().getAbsolutePath());
            }
        }
    }//GEN-LAST:event_mniSaveToJSONActionPerformed

    private void mniSaveToJDBCActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSaveToJDBCActionPerformed
        if (lstDataset.getSelectedIndex() == -1) {
            JOptionPane.showMessageDialog(this, "No dataset was selected.");
            return;
        }

        String databaseName = JOptionPane.showInputDialog("Enter the database name:", "metamorfose");
        if (databaseName == null) {
            return;
        }
        String tableName = JOptionPane.showInputDialog("Enter the table name:", "people");
        if (tableName == null) {
            return;
        }

        Object[] options = new Object[2];
        options[0] = "Overwrite";
        options[1] = "Append";

        String saveMode = JOptionPane.showInputDialog(this,
                "Enter the write mode of target table.",
                "Save Mode",
                JOptionPane.QUESTION_MESSAGE,
                null,
                options,
                options[0]).toString();

        boolean appendMode = true;
        if (saveMode.equals(options[0])) {
            appendMode = false;
        }

        String msg = "Server: Localhost/Postgres";
        msg += "\nDb: " + databaseName;
        msg += "\nSchema: public";
        msg += "\nTable: " + tableName;
        msg += "\nSave Mode: " + saveMode;
        msg += "\nUser e Password: default.";

        JOptionPane.showMessageDialog(this, msg, "Saving dataset in...", JOptionPane.INFORMATION_MESSAGE);

        SparkDatasetInstance datasetSelected = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());

        Framework.saveDatasetToJDBC(datasetSelected.getDataset(), databaseName, tableName, appendMode);

        JOptionPane.showMessageDialog(this, "Dataset salved!", "Saving dataset", JOptionPane.INFORMATION_MESSAGE);
    }//GEN-LAST:event_mniSaveToJDBCActionPerformed

    private void mniSaveToCSVActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSaveToCSVActionPerformed
        if (lstDataset.getSelectedIndex() == -1) {
            JOptionPane.showMessageDialog(this, "No dataset was selected.");
        } else {
            SparkDatasetInstance datasetSelected = (SparkDatasetInstance) listModel.get(lstDataset.getSelectedIndex());

            JFileChooser chooser = new JFileChooser();
            FileNameExtensionFilter filter = new FileNameExtensionFilter("CSV Files", "csv");
            chooser.setCurrentDirectory(new File("D:\\metamorfose\\demo"));
            chooser.setFileFilter(filter);
            int returnVal = chooser.showSaveDialog(this);

            if (returnVal == JFileChooser.APPROVE_OPTION) {
                Framework.saveRecordsToCSV(datasetSelected.getDataset(), chooser.getSelectedFile().getAbsolutePath());
                JOptionPane.showMessageDialog(this, "CSV saved in " + chooser.getSelectedFile().getAbsolutePath());
            }
        }
    }//GEN-LAST:event_mniSaveToCSVActionPerformed

    private void mniSairActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSairActionPerformed
        this.dispose();
    }//GEN-LAST:event_mniSairActionPerformed

    private void tblResultsMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tblResultsMouseClicked
        // Por algum motivo não consigo capturar duplo click na coluna.
        // Somente se o duplo click for na linha.
        // Deve ser por causa do auto_resize_off
//        if (evt.getClickCount() == 2) {
//
//            TableColumnModel colModel = tblResults.getColumnModel();
//            int vColIndex = colModel.getColumnIndexAtX(evt.getX());
//            if (vColIndex == -1) {
//                return;
//            }
//
//            String columnName = tblResults.getColumnName(vColIndex);
//            txtConsulta.append(columnName);
//        }
    }//GEN-LAST:event_tblResultsMouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(MetamorfoseJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(MetamorfoseJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(MetamorfoseJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(MetamorfoseJFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        //</editor-fold>
        //</editor-fold>
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new MetamorfoseJFrame().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnAdicionarDataSet;
    private javax.swing.JButton btnAdicionarMapeamento;
    private javax.swing.JButton btnClearQuery;
    private javax.swing.JButton btnExecutar;
    private javax.swing.JButton btnQueryResultToDataset;
    private javax.swing.JButton btnRemoverDataset;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JMenuBar jMenuBar1;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane4;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JLabel lblResultadoConsulta;
    private javax.swing.JList<String> lstDataset;
    private javax.swing.JMenuItem mniSair;
    private javax.swing.JMenuItem mniSaveToCSV;
    private javax.swing.JMenuItem mniSaveToJDBC;
    private javax.swing.JMenuItem mniSaveToJSON;
    private javax.swing.JMenuItem mniSparkGUI;
    private javax.swing.JMenuItem mniUnirDatasets;
    private javax.swing.JMenu mnuAcoes;
    private javax.swing.JMenu mnuUtilitarios;
    private javax.swing.JTable tblResults;
    private javax.swing.JTextArea txtConsulta;
    private javax.swing.JTextField txtNumeroRows;
    // End of variables declaration//GEN-END:variables
}
