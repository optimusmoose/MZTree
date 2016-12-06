package edu.msViz.msHttpApi;

import java.awt.event.ActionEvent;
import java.net.MalformedURLException;
import java.net.URL;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory.Strategy;
import java.awt.CardLayout;
import java.awt.Desktop;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;
import org.apache.commons.lang.StringUtils;

/**
 * MsDataServer GUI class
 * Swing implemented
 * 
 */
public class StartFrame extends JFrame {
    
    private JPanel cards = new JPanel(new CardLayout());
    private InitPanel initPanel;
    private final String initKey = "INIT";
    private RangePanel rangePanel;
    private final String rangeKey = "RANGE";
    
    // MsDataServer instance
    private MsDataServer dataServer = MsDataServer.getInstance();
    
    /**
     * Program start. Configures and displays StartFrame 
     * @param args command line args
     */
    public static void main(String[] args) {
        StartFrame frame = new StartFrame("MsDataServer");
    }
    
    /**
     * Default constructor
     * @param frameTitle title of JFrame
     */
    private StartFrame(String frameTitle){
        super(frameTitle);
        
        this.initPanel = new InitPanel(this);
        this.rangePanel = new RangePanel(this);
        this.cards.add(this.initPanel,this.initKey);
        this.cards.add(this.rangePanel,this.rangeKey);
        
        
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setContentPane(this.cards);
        this.pack();
        this.setLocationRelativeTo(null);
        this.setVisible(true);
    }
    
    /**
     * Override JFrame's toFront so frame is brought to
     * front on windows and linux
     */
    @Override
    public void toFront() {
        
        int sta = super.getExtendedState() & ~JFrame.ICONIFIED;

        super.setExtendedState(sta);
        super.setAlwaysOnTop(true);
        super.toFront();
        super.requestFocus();
        super.setAlwaysOnTop(false);
        
    }
    
    public void switchCard(String cardKey)
    {
        ((CardLayout)this.cards.getLayout()).show(this.cards, cardKey);
    }
    
    /**
     * Panel to be displayed on StartFrame
     */
    private static class InitPanel extends JPanel {
        
        // frame to display panel on
        private StartFrame frame;

        // Button texts
        private static final String START_TEXT = "Start Server";
        private static final String STOP_TEXT = "Stop Server";
        private static final String LAUNCH_TEXT = "Open App";
        private static final String EXPORT_TEXT = "Export";

        // flag to track if server is running
        private boolean running = false;

        // panel's GUI components
        private JTextField portEntry;
        private JButton startStopButton;
        private JButton launchButton;
        private JButton exportButton;

        /**
         * Default constructor
         * Configures panel's components
         */
        public InitPanel(StartFrame frame) {
            this.frame = frame;

            setLayout(new GridBagLayout());
            
            GridBagConstraints c = new GridBagConstraints();
            c.insets = new Insets(2, 2, 2, 2);
            c.fill = GridBagConstraints.HORIZONTAL;

            JLabel portLabel = new JLabel("Port:");
            portLabel.setHorizontalAlignment(JLabel.RIGHT);
            c.gridx = 0;
            c.gridy = 0;
            this.add(portLabel, c);
            
            portEntry = new JTextField("4567");
            c.gridx = 1;
            c.gridy = 0;
            this.add(portEntry, c);
            
            c.gridx = 0;
            c.gridwidth = 2;
            c.fill = GridBagConstraints.NONE;
            
            startStopButton = new JButton(START_TEXT);
            startStopButton.addActionListener(e -> startStopClicked(e));
            c.gridy = 1;
            this.add(startStopButton, c);

            launchButton = new JButton(LAUNCH_TEXT);
            launchButton.setEnabled(false);
            launchButton.addActionListener(e -> launchClicked(e));
            c.gridy = 2;
            this.add(launchButton, c);
            
            exportButton = new JButton(EXPORT_TEXT);
            exportButton.setEnabled(false);
            exportButton.addActionListener(e -> exportClicked(e));
            c.gridy = 3;
            this.add(exportButton, c);
        }

        private void openWebPage(String url) {
            URI uri = java.net.URI.create(url);
            try {
                if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                    Desktop.getDesktop().browse(uri);
                } else {
                    Runtime.getRuntime().exec("xdg-open " + uri.toString());
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        private void startStopClicked(ActionEvent e) {
            if (!running) {
                try {
                    int port = Integer.parseInt(portEntry.getText());
                    this.frame.dataServer.startServer(port, frame, Strategy.WeightedStriding);
                    this.frame.dataServer.waitUntilStarted();
                    launchClicked(null);
                    exportButton.setEnabled(true);
                } catch (NumberFormatException ex) {
                    portEntry.setText("4567");
                }
            } else {
                this.frame.dataServer.stopServer();
                exportButton.setEnabled(false);
            }
            running = !running;
            startStopButton.setText(running ? STOP_TEXT : START_TEXT);
            launchButton.setEnabled(running);
        }

        private void launchClicked(ActionEvent e) {
            URL dest;
            try {
                dest = new URL("http", "localhost", this.frame.dataServer.getPort(), "");
                openWebPage(dest.toString());
            } catch (MalformedURLException ex) {
                JOptionPane.showMessageDialog(this, "Could not create URL to server.");
            }
        }
        
        private void exportClicked(ActionEvent e)
        {
           if(!(this.frame.dataServer.fileStatus == MsDataServer.FileStatus.FILE_READY))
               JOptionPane.showMessageDialog(this.frame, "No file currently loaded", "Error", JOptionPane.ERROR_MESSAGE);
           else
               this.frame.switchCard(this.frame.rangeKey);
        }
    }
    
    private static class RangePanel extends JPanel
    {
        private StartFrame frame;
        
        private JTextField minMZ;
        private JTextField maxMZ;
        private JTextField minRT;
        private JTextField maxRT;
        private JButton OK;
        private JButton CANCEL;
        
        public RangePanel(StartFrame frame)
        {
            this.frame = frame;

            setLayout(new GridBagLayout());
            
            GridBagConstraints c = new GridBagConstraints();
            c.insets = new Insets(2, 2, 2, 2);
            c.fill = GridBagConstraints.HORIZONTAL;
            
            // min label
            c.gridx = 1;
            c.gridy = 0;
            this.add(new JLabel("Min"), c);
            
            // max label
            c.gridx = 2;
            c.gridy = 0;
            this.add(new JLabel("Max"), c);
            
            // mz label
            c.gridx = 0;
            c.gridy = 1;
            this.add(new JLabel("m/z"), c);
            
            // rt label
            c.gridx = 0;
            c.gridy = 2;
            this.add(new JLabel("RT"), c);
            
            // mz min
            minMZ = new JTextField();
            c.gridx = 1;
            c.gridy = 1;
            this.add(minMZ,c);
            
            // mz max
            maxMZ = new JTextField();
            c.gridx = 2;
            c.gridy = 1;
            this.add(maxMZ,c);
            
            // rt min
            minRT = new JTextField();
            c.gridx = 1;
            c.gridy = 2;
            this.add(minRT,c);
            
            // rt max
            maxRT = new JTextField();
            c.gridx = 2;
            c.gridy = 2;
            this.add(maxRT,c);
            
            // OK button            
            OK = new JButton("OK");
            OK.addActionListener(e -> okClicked(e));
            c.gridx = 2;
            c.gridy = 3;
            this.add(OK,c);
            
            // CANCEL button
            CANCEL = new JButton("Cancel");
            CANCEL.addActionListener(e -> cancelClicked(e));
            c.gridx = 1;
            c.gridy = 3;
            this.add(CANCEL,c);
        }
        
        private void okClicked(ActionEvent e)
        {
            String s_minMZ, s_maxMZ, s_minRT, s_maxRT;
            s_minMZ = this.minMZ.getText();
            s_maxMZ = this.maxMZ.getText();
            s_minRT = this.minRT.getText();
            s_maxRT = this.maxRT.getText();
            
            // validate input
            if(StringUtils.isNumeric(s_minMZ) && StringUtils.isNumeric(s_maxMZ) && StringUtils.isNumeric(s_minRT) && StringUtils.isNumeric(s_maxRT))
            {
                JFileChooser outputFileChooser = new JFileChooser();
                outputFileChooser.setDialogTitle("Export");
                outputFileChooser.setFileFilter(new FileNameExtensionFilter(".csv", "csv"));
                int outputFileResult = outputFileChooser.showSaveDialog(this.frame);
                
                // continue only if file selected
                if(outputFileResult == JFileChooser.APPROVE_OPTION)
                {
                    double d_minMZ, d_maxMZ, d_minRT, d_maxRT;
                    d_minMZ = Double.parseDouble(s_minMZ);
                    d_maxMZ = Double.parseDouble(s_maxMZ);
                    d_minRT = Double.parseDouble(s_minRT);
                    d_maxRT = Double.parseDouble(s_maxRT);
                    
                    String filepath = outputFileChooser.getSelectedFile().getPath();
                    
                    try 
                    {
                        this.frame.dataServer.exportCSV(filepath, d_minMZ, d_maxMZ, (float)d_minRT, (float)d_maxRT);
                    } 
                    catch (IOException ex) 
                    {
                        JOptionPane.showMessageDialog(this.frame, "Could not read the selected CSV file", "Error", JOptionPane.ERROR_MESSAGE);
                    }
                    finally
                    {
                        this.frame.switchCard(this.frame.initKey);
                    }
                }
            }
        }
        
        private void cancelClicked(ActionEvent e)
        {
            this.frame.switchCard(this.frame.initKey);
        }
    }
}
