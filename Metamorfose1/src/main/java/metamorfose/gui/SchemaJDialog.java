/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.gui;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import javax.swing.DefaultCellEditor;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import metamorfose.converters.Converter;
import metamorfose.map.EntityMap;
import metamorfose.map.EntityMapJsonUtility;
import metamorfose.map.FieldMap;
import metamorfose.model.Entity;
import metamorfose.transformations.javascript.Script;
import metamorfose.transformations.javascript.TransformationType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Evandro
 */
public class SchemaJDialog extends javax.swing.JDialog {

    private StructType schema;
    private DefaultTableModel model;
    private EntityMap entityMap;
    private boolean canceled = true;
    private boolean newDataset = false;
    private String newDatasetName;

    private String mapPathFile;
    private String mapCurrentDirectory;

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public boolean isNewDataset() {
        return newDataset;
    }

    public String getNewDatasetName() {
        return newDatasetName;
    }

    public EntityMap getEntityMap() {
        return entityMap;
    }

    public void setEntityMap(EntityMap entityMap) {
        this.entityMap = entityMap;
    }

    public void removeAllTableRows() {
        // Removendo todas as linhas da JTable
        while (model.getRowCount() > 0) {
            model.removeRow(0);
        }
    }

    public void loadTableFromSchema() {

        for (StructField f : schema.fields()) {

            Object[] linha = new Object[8];

            linha[0] = f.name();
            linha[1] = f.dataType().toString().replace("Type", "").toUpperCase();
            linha[2] = "--->";
            linha[3] = "";
            linha[4] = "";
            linha[5] = "";
            linha[6] = "";
            linha[7] = "";

            model.addRow(linha);
        }
    }

    public void loadTableFromEntityMap() {
        // Carregando mapeamentos na JTable        
        for (FieldMap fm : this.entityMap.getFieldMappings()) {
            Object[] linha = new Object[8];

            linha[0] = fm.getSourceFieldName();
            linha[1] = fm.getSourceDataType();
            linha[2] = "--->";
            linha[3] = fm.getTargetFieldName();
            linha[4] = fm.getTargetDataType();
            linha[5] = fm.getTransformationType().toString();
            linha[6] = fm.getTransformationName();

            Script script = this.entityMap.getScriptyByFunctionName(fm.getTransformationName());
            if (script != null) {
                linha[7] = script.getFunctionImplementation();
            } else {
                linha[7] = "";
            }

            model.addRow(linha);
        }
    }

    public void loadMatchingMapsToTable() {
        // Considerando que a tabela já tenha os campos de origem carregados...
        // Esse método carrega os FieldMaps que correspondem com os campos de origem carregados.
        // Resultado: talvez alguns campos de origem não tenham campos destinos correspondentes. O usuário deve terminar o mapeamento.        
        for (int i = 0; i < tblSchema.getRowCount(); i++) {

            // Procura fieldMap de acordo com o nome do campo de origem.
            FieldMap fm = entityMap.getFieldMappingBySourceFieldName(model.getValueAt(i, 0).toString());

            if (fm != null) {
                model.setValueAt(fm.getTargetFieldName(), i, 3);
                model.setValueAt(fm.getTargetDataType(), i, 4);
                model.setValueAt(fm.getTransformationName(), i, 6);
            }
        }
    }

    public void createEntityMapFromTable() {
        Entity source = new Entity("SOURCE");
        Entity target = new Entity("TARGET");

        entityMap = new EntityMap("MAPPING", target, source, new Converter());
        entityMap.setAnnotations("ANNOTATIONS...");

        for (int i = 0; i < tblSchema.getRowCount(); i++) {

            if (model.getValueAt(i, 3).toString().length() != 0) { // se tiver mapeamento (campo destino) definido pelo usuário no campo em questão, então adiciona no objeto EntityMap

//                entityMap.mapFields(
//                        model.getValueAt(i, 3).toString(),
//                        model.getValueAt(i, 4).toString(),
//                        model.getValueAt(i, 0).toString(),
//                        "STRING", // alterar essa questão, com estou trabalhando como CSV considero tudo como String, mas em outros casos lançará exception.
//                        model.getValueAt(i, 5).toString(),
//                        "");
                entityMap.mapFields(
                        model.getValueAt(i, 3).toString(),
                        model.getValueAt(i, 4).toString(),
                        model.getValueAt(i, 0).toString(),
                        //"STRING", // alterar essa questão, com estou trabalhando como CSV considero tudo como String, mas em outros casos lançará exception.
                        model.getValueAt(i, 1).toString(),
                        model.getValueAt(i, 6).toString(),
                        TransformationType.valueOf(model.getValueAt(i, 5).toString()),
                        model.getValueAt(i, 7).toString()
                );
            }

        }
    }

    private void configJTableLayout() {
        // Largura padrão das colunas
        tblSchema.getColumnModel().getColumn(0).setPreferredWidth(220);
        tblSchema.getColumnModel().getColumn(1).setPreferredWidth(90);
        tblSchema.getColumnModel().getColumn(2).setPreferredWidth(50);
        tblSchema.getColumnModel().getColumn(3).setPreferredWidth(220);
        tblSchema.getColumnModel().getColumn(4).setPreferredWidth(90);
        tblSchema.getColumnModel().getColumn(5).setPreferredWidth(130);
        tblSchema.getColumnModel().getColumn(6).setPreferredWidth(200);
        tblSchema.getColumnModel().getColumn(7).setPreferredWidth(200);

        // Desligar o autoresize, caso contrario não funciona a configuração do tamanho das colunas.
        tblSchema.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

        // Configurando o alinhamento da coluna e cabeçalho da coluna 2 para centralizado.
        DefaultTableCellRenderer centralizado = new DefaultTableCellRenderer();
        centralizado.setHorizontalAlignment(SwingConstants.CENTER);
        tblSchema.getColumnModel().getColumn(2).setCellRenderer(centralizado);
        tblSchema.getColumnModel().getColumn(2).setHeaderRenderer(centralizado);
    }

    private void createComboBoxTransformationType() {

        JComboBox comboTransformationType = new JComboBox();
        comboTransformationType.addItem("CASTING");
        comboTransformationType.addItem("JAVA");
        comboTransformationType.addItem("JAVASCRIPT");

        tblSchema.getColumnModel().getColumn(5).setCellEditor(new DefaultCellEditor(comboTransformationType));

    }

    private void createComboBoxDataTypes() {

        JComboBox comboDataTypes = new JComboBox();
        //comboDataTypes.addItem("BINARY");
        comboDataTypes.addItem("BOOLEAN");
        //comboDataTypes.addItem("CALENDARINTERVAL");
        comboDataTypes.addItem("DATE");
        comboDataTypes.addItem("DOUBLE");
        comboDataTypes.addItem("FLOAT");
        comboDataTypes.addItem("INTEGER");
        comboDataTypes.addItem("LONG");
        comboDataTypes.addItem("NULL");
        comboDataTypes.addItem("SHORT");
        comboDataTypes.addItem("STRING");
        //comboDataTypes.addItem("TIMESTAMP");

        tblSchema.getColumnModel().getColumn(1).setCellEditor(new DefaultCellEditor(comboDataTypes));
        tblSchema.getColumnModel().getColumn(4).setCellEditor(new DefaultCellEditor(comboDataTypes));

    }

    private void showScriptDialog() {
        if (tblSchema.getSelectedRow() == -1) {
            JOptionPane.showMessageDialog(this, "No mapping was selected!");
            return;
        }
        
        if (!model.getValueAt(tblSchema.getSelectedRow(), 5).toString().equalsIgnoreCase("JAVASCRIPT")) {
            JOptionPane.showMessageDialog(this, "Transformation type must be Javascript!");
            return;
        }

        if (model.getValueAt(tblSchema.getSelectedRow(), 6).toString().length() == 0) {
            JOptionPane.showMessageDialog(this, "It is necessary enter the transform function name!");
            return;
        }

        String functionName = model.getValueAt(tblSchema.getSelectedRow(), 6).toString();
        String sourceFieldName = model.getValueAt(tblSchema.getSelectedRow(), 0).toString();

        String scriptTemplate = "";

        if (sourceFieldName.contains("$LIST")) {
            String customizedField = sourceFieldName.trim();
            customizedField = customizedField.replace("$LIST(", "");
            customizedField = customizedField.replace(")", "");

            String fieldElements[] = customizedField.split(",");
            scriptTemplate = "function " + functionName + " (values) {\n";

            int count = 0;
            for (String fieldName : fieldElements) {
                scriptTemplate += "\t" + fieldName.trim() + " = values["+count+"];\n";
                count++;
            }
            scriptTemplate += "}";

        } else if (sourceFieldName.contains("$VALUE")) {
            scriptTemplate = "function " + functionName + " (values) {\n"
                    + "\tif (values[0]==null) return;\n"
                    + "}";
        } else {
            scriptTemplate = "function " + functionName + " (values) {\n"
                    + "\tif (values[0]==null) return;\n"
                    + "\t" + sourceFieldName + " = values[0];\n"
                    + "}";
        }

        if (model.getValueAt(tblSchema.getSelectedRow(), 7).toString().length() != 0) {
            scriptTemplate = model.getValueAt(tblSchema.getSelectedRow(), 7).toString();
        }

        ScriptJDialog tela = new ScriptJDialog(null, true);

        tela.setScript(scriptTemplate);
        tela.setVisible(true);

        if (!tela.isCancel()) {
            model.setValueAt(tela.getScript(), tblSchema.getSelectedRow(), 7);
        }
    }

    /**
     * Creates new form SchemaJDialog
     */
    public SchemaJDialog(java.awt.Frame parent, boolean modal) {
        super(parent, modal);
        initComponents();

        model = (DefaultTableModel) this.tblSchema.getModel();

        configJTableLayout();
        createComboBoxTransformationType();
        createComboBoxDataTypes();

        mapCurrentDirectory = "D:\\metamorfose\\demo";

        // Código para 'maximizar' o JDialog, ou seja, ocupar o tamanho máximo da tela, pois um JDialog não tem como ser maximizado.
        Toolkit tk = Toolkit.getDefaultToolkit();
        Dimension screenSize = tk.getScreenSize();
        this.setSize(screenSize.width, screenSize.height - 50);

    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jMenu1 = new javax.swing.JMenu();
        jPanel3 = new javax.swing.JPanel();
        jScrollPane2 = new javax.swing.JScrollPane();
        tblSchema = new javax.swing.JTable();
        btnNovaLinhaTabela = new javax.swing.JButton();
        btnNovaLinhaTabela1 = new javax.swing.JButton();
        btnEspelhar = new javax.swing.JButton();
        btnAplicarMap = new javax.swing.JButton();
        btnAplicarMapNovoDataset = new javax.swing.JButton();
        jMenuBar1 = new javax.swing.JMenuBar();
        jMenu2 = new javax.swing.JMenu();
        mniLoadMapping = new javax.swing.JMenuItem();
        mniSaveMapping = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JPopupMenu.Separator();
        mniLoadSimcaq13 = new javax.swing.JMenuItem();
        mniLoadSimcaq14 = new javax.swing.JMenuItem();
        mniLoadSimcaq13Javascript = new javax.swing.JMenuItem();
        jSeparator2 = new javax.swing.JPopupMenu.Separator();
        mniClose = new javax.swing.JMenuItem();

        jMenu1.setText("jMenu1");

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("View Dataset Schema");

        jPanel3.setBorder(new javax.swing.border.SoftBevelBorder(javax.swing.border.BevelBorder.RAISED));

        tblSchema.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {
                "Source Field", "Data Type", "-->", "Target Field", "Data Type", "Transformation", "UDF Name", "Script"
            }
        ) {
            boolean[] canEdit = new boolean [] {
                true, true, false, true, true, true, true, false
            };

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        tblSchema.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tblSchemaMouseClicked(evt);
            }
        });
        jScrollPane2.setViewportView(tblSchema);
        if (tblSchema.getColumnModel().getColumnCount() > 0) {
            tblSchema.getColumnModel().getColumn(2).setMinWidth(1);
            tblSchema.getColumnModel().getColumn(2).setPreferredWidth(1);
        }

        btnNovaLinhaTabela.setText("New Line");
        btnNovaLinhaTabela.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNovaLinhaTabelaActionPerformed(evt);
            }
        });

        btnNovaLinhaTabela1.setText("Remove Line");
        btnNovaLinhaTabela1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnNovaLinhaTabela1ActionPerformed(evt);
            }
        });

        btnEspelhar.setText("Mirror");
        btnEspelhar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnEspelharActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane2)
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addComponent(btnNovaLinhaTabela, javax.swing.GroupLayout.DEFAULT_SIZE, 99, Short.MAX_VALUE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnNovaLinhaTabela1, javax.swing.GroupLayout.DEFAULT_SIZE, 117, Short.MAX_VALUE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnEspelhar, javax.swing.GroupLayout.DEFAULT_SIZE, 87, Short.MAX_VALUE)
                        .addGap(688, 688, 688)))
                .addContainerGap())
        );
        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup()
                .addGap(5, 5, 5)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnNovaLinhaTabela)
                    .addComponent(btnNovaLinhaTabela1)
                    .addComponent(btnEspelhar))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 399, Short.MAX_VALUE)
                .addContainerGap())
        );

        btnAplicarMap.setText("Apply Mapping");
        btnAplicarMap.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnAplicarMapActionPerformed(evt);
            }
        });

        btnAplicarMapNovoDataset.setText("Apply Mapping to New Dataset");
        btnAplicarMapNovoDataset.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnAplicarMapNovoDatasetActionPerformed(evt);
            }
        });

        jMenu2.setText("Actions");

        mniLoadMapping.setText("Load mapping");
        mniLoadMapping.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniLoadMappingActionPerformed(evt);
            }
        });
        jMenu2.add(mniLoadMapping);

        mniSaveMapping.setText("Save mapping");
        mniSaveMapping.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniSaveMappingActionPerformed(evt);
            }
        });
        jMenu2.add(mniSaveMapping);
        jMenu2.add(jSeparator1);

        mniLoadSimcaq13.setText("Load Simcaq13");
        mniLoadSimcaq13.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniLoadSimcaq13ActionPerformed(evt);
            }
        });
        jMenu2.add(mniLoadSimcaq13);

        mniLoadSimcaq14.setText("Load Simcaq14");
        mniLoadSimcaq14.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniLoadSimcaq14ActionPerformed(evt);
            }
        });
        jMenu2.add(mniLoadSimcaq14);

        mniLoadSimcaq13Javascript.setText("Load Simcaq13 with Javascript");
        mniLoadSimcaq13Javascript.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniLoadSimcaq13JavascriptActionPerformed(evt);
            }
        });
        jMenu2.add(mniLoadSimcaq13Javascript);
        jMenu2.add(jSeparator2);

        mniClose.setText("Close");
        mniClose.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mniCloseActionPerformed(evt);
            }
        });
        jMenu2.add(mniClose);

        jMenuBar1.add(jMenu2);

        setJMenuBar(jMenuBar1);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(btnAplicarMap)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(btnAplicarMapNovoDataset)
                        .addGap(0, 0, Short.MAX_VALUE))
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                        .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addContainerGap())))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnAplicarMap, javax.swing.GroupLayout.PREFERRED_SIZE, 34, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnAplicarMapNovoDataset, javax.swing.GroupLayout.PREFERRED_SIZE, 34, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void btnAplicarMapActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnAplicarMapActionPerformed
        createEntityMapFromTable();

        this.canceled = false;
        this.dispose();
    }//GEN-LAST:event_btnAplicarMapActionPerformed

    private void btnAplicarMapNovoDatasetActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnAplicarMapNovoDatasetActionPerformed

        String entrada = JOptionPane.showInputDialog("Enter the new dataset name");
        if (entrada != null) {
            createEntityMapFromTable();

            this.newDatasetName = entrada;
            this.newDataset = true;
            this.canceled = false;
            this.dispose();
        }
    }//GEN-LAST:event_btnAplicarMapNovoDatasetActionPerformed

    private void btnNovaLinhaTabelaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNovaLinhaTabelaActionPerformed
        model.addRow(new Object[]{"", "", "-->", "", "", "", "", ""});
    }//GEN-LAST:event_btnNovaLinhaTabelaActionPerformed

    private void btnNovaLinhaTabela1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnNovaLinhaTabela1ActionPerformed
        while (tblSchema.getSelectedRows().length > 0) {
            model.removeRow(tblSchema.getSelectedRows()[0]);
        }
    }//GEN-LAST:event_btnNovaLinhaTabela1ActionPerformed

    private void btnEspelharActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnEspelharActionPerformed

        for (int i = 0; i < model.getRowCount(); i++) {
            model.setValueAt(model.getValueAt(i, 0), i, 3);
            model.setValueAt(model.getValueAt(i, 1), i, 4);
            model.setValueAt("CASTING", i, 5);
        }

    }//GEN-LAST:event_btnEspelharActionPerformed

    private void mniLoadMappingActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniLoadMappingActionPerformed

        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter filter = new FileNameExtensionFilter("JSON Files", "json");
        chooser.setCurrentDirectory(new File(this.mapCurrentDirectory));
        chooser.setFileFilter(filter);
        int returnVal = chooser.showOpenDialog(this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            this.mapPathFile = chooser.getSelectedFile().getAbsolutePath();
        } else {
            return;
        }

        // Carregando mapeamentos do disco
        this.entityMap = EntityMapJsonUtility.loadFromJSON(this.mapPathFile);

        // Removendo todas as linhas da JTable
        this.removeAllTableRows();

        // Carregando mapeamentos na JTable        
        this.loadTableFromEntityMap();
        // outra forma de carregar mapeamentos, onde somente os mapeamentos que correspondem entre tabela e EntityMaps são carregados.
        //this.matchingMapsAndLoadTableFromEntityMap();

        JOptionPane.showMessageDialog(this, "Mapping loaded from " + this.mapPathFile);

    }//GEN-LAST:event_mniLoadMappingActionPerformed

    private void mniSaveMappingActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniSaveMappingActionPerformed

        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter filter = new FileNameExtensionFilter(
                "JSON Files", "json");
        chooser.setCurrentDirectory(new File(this.mapCurrentDirectory));
        chooser.setFileFilter(filter);
        int returnVal = chooser.showSaveDialog(this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            this.mapPathFile = chooser.getSelectedFile().getAbsolutePath();
        } else {
            return;
        }
        createEntityMapFromTable();

        EntityMapJsonUtility.saveToJSON(entityMap, this.mapPathFile);

        JOptionPane.showMessageDialog(this, "Mapping saved in " + this.mapPathFile);

    }//GEN-LAST:event_mniSaveMappingActionPerformed

    private void mniCloseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniCloseActionPerformed
        this.dispose();
    }//GEN-LAST:event_mniCloseActionPerformed

    private void mniLoadSimcaq13ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniLoadSimcaq13ActionPerformed
        while (model.getRowCount() > 0) {
            model.removeRow(0);
        }

        this.entityMap = new EntityMap("SIMCAQ", new Entity("matriculas"), new Entity("CSV"), new Converter());

        try {
            FileReader arq = new FileReader("D:\\notaql-dados\\MapMatriculas2013.CSV");
            BufferedReader br = new BufferedReader(arq);

            String linha = br.readLine();
            linha = br.readLine(); // ignorando a primeira linha do .CSV
            while (linha != null) {

                String[] attributes = linha.split(";");

                if (attributes[0].equals("S")) {

                    if (attributes.length > 4) { // tem função de transformação
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", "com.kuszera.simcaq.transformations." + attributes[4], "");
                    } else {
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", "", "");
                    }

                }
                linha = br.readLine();
            }

            br.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }

        // Carregando mapeamentos na JTable        
        this.loadTableFromEntityMap();
        //this.loadMatchingMapsToTable();

        JOptionPane.showMessageDialog(this, "Mapeamento carregado de " + this.mapPathFile);
    }//GEN-LAST:event_mniLoadSimcaq13ActionPerformed

    private void mniLoadSimcaq14ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniLoadSimcaq14ActionPerformed
        while (model.getRowCount() > 0) {
            model.removeRow(0);
        }

        this.entityMap = new EntityMap("SIMCAQ", new Entity("matriculas"), new Entity("CSV"), new Converter());

        try {
            FileReader arq = new FileReader("D:\\notaql-dados\\MapMatriculas2014.CSV");
            BufferedReader br = new BufferedReader(arq);

            String linha = br.readLine();
            linha = br.readLine(); // ignorando a primeira linha do .CSV
            while (linha != null) {

                String[] attributes = linha.split(";");

                if (attributes[0].equals("S")) {

                    if (attributes.length > 5) { // tem função de transformação
                        entityMap.mapFields(attributes[1], attributes[2], attributes[4], "STRING", "com.kuszera.simcaq.transformations." + attributes[5], "");
                    } else {
                        entityMap.mapFields(attributes[1], attributes[2], attributes[4], "STRING", "", "");
                    }

                }
                linha = br.readLine();
            }

            br.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }

        // Carregando mapeamentos na JTable        
        this.loadTableFromEntityMap();

        JOptionPane.showMessageDialog(this, "Mapeamento carregado de " + this.mapPathFile);
    }//GEN-LAST:event_mniLoadSimcaq14ActionPerformed

    private void mniLoadSimcaq13JavascriptActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mniLoadSimcaq13JavascriptActionPerformed

        while (model.getRowCount() > 0) {
            model.removeRow(0);
        }

        this.entityMap = new EntityMap("SIMCAQ", new Entity("matriculas"), new Entity("CSV"), new Converter());

        try {
            FileReader arq = new FileReader("D:\\notaql-dados\\MapMatriculas2013.CSV");
            BufferedReader br = new BufferedReader(arq);

            String linha = br.readLine();
            linha = br.readLine(); // ignorando a primeira linha do .CSV
            while (linha != null) {

                String[] attributes = linha.split(";");

                if (attributes[0].equals("S")) {

                    if (attributes.length > 4) // tem função de transformação
                    {
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", attributes[4], TransformationType.CASTING, "");
                    } else {
                        entityMap.mapFields(attributes[1], attributes[2], attributes[3], "STRING", "", TransformationType.CASTING, "");
                    }

                }
                linha = br.readLine();
            }

            br.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }

        // Carregando mapeamentos na JTable        
        this.loadTableFromEntityMap();
        //this.loadMatchingMapsToTable();

        JOptionPane.showMessageDialog(this, "Mapping loaded from " + this.mapPathFile);
    }//GEN-LAST:event_mniLoadSimcaq13JavascriptActionPerformed

    private void tblSchemaMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tblSchemaMouseClicked
        if (tblSchema.getSelectedRow() != -1) {
            if (tblSchema.getColumnName(tblSchema.getSelectedColumn()).equalsIgnoreCase("Script")) {
                showScriptDialog();
            }
            
            // espelhar a linha se o usuário clicar na coluna '-->' setando transformação CASTING
            if (tblSchema.getColumnName(tblSchema.getSelectedColumn()).equalsIgnoreCase("-->")) {
                int i = tblSchema.getSelectedRow();
                model.setValueAt(model.getValueAt(i, 0), i, 3);
                model.setValueAt(model.getValueAt(i, 1), i, 4);
                model.setValueAt("CASTING", i, 5);
            }
        }
    }//GEN-LAST:event_tblSchemaMouseClicked

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
            java.util.logging.Logger.getLogger(SchemaJDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(SchemaJDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(SchemaJDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(SchemaJDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the dialog */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                SchemaJDialog dialog = new SchemaJDialog(new javax.swing.JFrame(), true);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {
                    @Override
                    public void windowClosing(java.awt.event.WindowEvent e) {
                        System.exit(0);
                    }
                });
                dialog.setVisible(true);
            }
        });
    }


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnAplicarMap;
    private javax.swing.JButton btnAplicarMapNovoDataset;
    private javax.swing.JButton btnEspelhar;
    private javax.swing.JButton btnNovaLinhaTabela;
    private javax.swing.JButton btnNovaLinhaTabela1;
    private javax.swing.JMenu jMenu1;
    private javax.swing.JMenu jMenu2;
    private javax.swing.JMenuBar jMenuBar1;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JMenuItem mniClose;
    private javax.swing.JMenuItem mniLoadMapping;
    private javax.swing.JMenuItem mniLoadSimcaq13;
    private javax.swing.JMenuItem mniLoadSimcaq13Javascript;
    private javax.swing.JMenuItem mniLoadSimcaq14;
    private javax.swing.JMenuItem mniSaveMapping;
    private javax.swing.JTable tblSchema;
    // End of variables declaration//GEN-END:variables
}
