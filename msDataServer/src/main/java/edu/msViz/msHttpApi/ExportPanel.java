package edu.msViz.msHttpApi;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class ExportPanel extends JPanel {

    private static final Logger LOGGER = Logger.getLogger(ExportPanel.class.getName());
    
    private static String ALL_DATA_TOOLTIP = "Fills range fields with entire data range of the loaded data set";

    private StartFrame frame;

    private JTextField minMZ;
    private JTextField maxMZ;
    private JTextField minRT;
    private JTextField maxRT;
    private JButton okButton;
    private JButton allDataButton;

    public ExportPanel(StartFrame frame) {
        this.frame = frame;

        setLayout(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 2, 2, 2);
        c.fill = GridBagConstraints.HORIZONTAL;

        // ROW 0: min/max headings
        c.weightx = 1;

        JLabel minLabel = new JLabel("Min");
        minLabel.setHorizontalAlignment(JLabel.CENTER);
        c.gridx = 1;
        this.add(minLabel, c);

        JLabel maxLabel = new JLabel("Max");
        maxLabel.setHorizontalAlignment(JLabel.CENTER);
        c.gridx = 2;
        this.add(maxLabel, c);

        c.weightx = 0;

        // ROW 1: mz
        c.gridy = 1;

        JLabel mzLabel = new JLabel("m/z");
        mzLabel.setHorizontalAlignment(JLabel.RIGHT);
        c.gridx = 0;
        this.add(mzLabel, c);

        minMZ = new JTextField();
        c.gridx = 1;
        this.add(minMZ, c);

        maxMZ = new JTextField();
        c.gridx = 2;
        this.add(maxMZ, c);

        // ROW 2: rt
        c.gridy = 2;

        JLabel rtLabel = new JLabel("RT");
        rtLabel.setHorizontalAlignment(JLabel.RIGHT);
        c.gridx = 0;
        this.add(rtLabel, c);

        minRT = new JTextField();
        c.gridx = 1;
        this.add(minRT, c);

        maxRT = new JTextField();
        c.gridx = 2;
        this.add(maxRT, c);

        // ROW 3:  buttons
        c.gridy = 3;
        c.fill = GridBagConstraints.NONE;

        okButton = new JButton("OK");
        okButton.setMnemonic('K');
        okButton.addActionListener(this::okClicked);
        c.gridx = 2;
        c.gridwidth = 2;
        this.add(okButton, c);
        
        allDataButton = new JButton("All Data");
        allDataButton.setMnemonic('A');
        allDataButton.addActionListener(this::allDataClicked);
        allDataButton.setToolTipText(ALL_DATA_TOOLTIP);
        c.gridx = 0;
        this.add(allDataButton,c);
        
    }

    
    
    private void allDataClicked(ActionEvent e)
    {
        double[] bounds = frame.mzTree.getDataBounds();
        if(bounds != null)
        {
            // rounded to three decimal places
            // to be inclusive, floor on mins, ceil on maxes
            float minMzRounded = (float)Math.floor(bounds[0] * 1000) / 1000f;
            float maxMzRounded = (float)Math.ceil(bounds[1] * 1000) / 1000f;
            float minRtRounded = (float)Math.floor(bounds[2] * 1000) / 1000f;
            float maxRtRounded = (float)Math.ceil(bounds[3] * 1000) / 1000f;
            
            this.minMZ.setText(String.valueOf(minMzRounded));
            this.maxMZ.setText(String.valueOf(maxMzRounded));
            this.minRT.setText(String.valueOf(minRtRounded));
            this.maxRT.setText(String.valueOf(maxRtRounded));
        }
    }
    
    private void okClicked(ActionEvent e) {
        try
        {
            String s_minMZ, s_maxMZ, s_minRT, s_maxRT;
            s_minMZ = this.minMZ.getText();
            s_maxMZ = this.maxMZ.getText();
            s_minRT = this.minRT.getText();
            s_maxRT = this.maxRT.getText();
            
            double d_minMZ, d_maxMZ, d_minRT, d_maxRT;
            d_minMZ = !s_minMZ.isEmpty() ? Double.parseDouble(s_minMZ) : Double.MIN_VALUE;
            d_maxMZ = !s_maxMZ.isEmpty() ? Double.parseDouble(s_maxMZ) : Double.MAX_VALUE;
            d_minRT = !s_minRT.isEmpty() ? Double.parseDouble(s_minRT) : Double.MIN_VALUE;
            d_maxRT = !s_maxRT.isEmpty() ? Double.parseDouble(s_maxRT) : Double.MAX_VALUE;
            
            JFileChooser outputFileChooser = new JFileChooser();
            outputFileChooser.setDialogTitle("Export");
            outputFileChooser.setFileFilter(new FileNameExtensionFilter(".csv", "csv"));
            int outputFileResult = outputFileChooser.showSaveDialog(this.frame);

            // continue only if file selected
            if (outputFileResult == JFileChooser.APPROVE_OPTION) 
            {
                String filepath = outputFileChooser.getSelectedFile().getPath();

                try {
                    int exported = this.frame.mzTree.export(filepath, d_minMZ, d_maxMZ, (float) d_minRT, (float) d_maxRT);
                    LOGGER.log(Level.INFO, "Exported " + exported + " points to CSV.");
                    JOptionPane.showMessageDialog(this.frame, "Finished CSV export", "Export Complete", JOptionPane.INFORMATION_MESSAGE);
                } catch (IOException ex) {
                    LOGGER.log(Level.WARNING, "Error when exporting CSV file", ex);
                    JOptionPane.showMessageDialog(this.frame, "Could not export to CSV file: " + ex.getMessage(), "Export Error", JOptionPane.ERROR_MESSAGE);
                }
            }
            
        }
        catch(NumberFormatException ex)
        {
            JOptionPane.showMessageDialog(this.frame, "Invalid input", "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
