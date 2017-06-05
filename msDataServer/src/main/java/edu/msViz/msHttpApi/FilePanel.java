package edu.msViz.msHttpApi;

import edu.msViz.mzTree.ImportState;
import edu.msViz.mzTree.MzTree;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Panel to be displayed on StartFrame
 */
class FilePanel extends JPanel {

    private static final Logger LOGGER = Logger.getLogger(FilePanel.class.getName());
    private static final String NO_FILE_TEXT = "No file open";

    // frame to display panel on
    private StartFrame frame;

    // Button texts
    private static final String OPEN_TEXT = "Open...";
    private static final String SAVE_TEXT = "Save As...";
    private static final String CLOSE_TEXT = "Close";

    // panel's GUI components
    private JButton openButton;
    private JButton saveButton;
    private JButton closeButton;
    private JLabel fileLabel;

    private final JFileChooser fileChooser;
    FileNameExtensionFilter openFilter = new FileNameExtensionFilter("Mass Spectrometry Data File", "mzML", "mzTree", "csv");
    FileNameExtensionFilter saveFilter = new FileNameExtensionFilter("mzTree file", "mzTree");

    /**
     * Default constructor
     * Configures panel's components
     */
    public FilePanel(StartFrame frame) {
        this.frame = frame;

        setLayout(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 2, 2, 2);
        c.weightx = 1;

        // add the open, save, and close buttons
        c.gridy = 0;
        c.gridwidth = 1;
        c.fill = GridBagConstraints.HORIZONTAL;

        openButton = new JButton(OPEN_TEXT);
        openButton.setMnemonic('O');
        openButton.addActionListener(this::openClicked);
        c.gridx = 0;
        this.add(openButton, c);

        saveButton = new JButton(SAVE_TEXT);
        saveButton.setMnemonic('A');
        saveButton.addActionListener(this::saveClicked);
        c.gridx = 1;
        this.add(saveButton, c);

        closeButton = new JButton(CLOSE_TEXT);
        closeButton.setMnemonic('C');
        closeButton.addActionListener(this::closeClicked);
        c.gridx = 2;
        this.add(closeButton, c);

        // add the file name display
        fileLabel = new JLabel(NO_FILE_TEXT);
        fileLabel.setPreferredSize(new Dimension(100, 30));
        c.gridy = 1;
        c.gridx = 0;
        c.gridwidth = 4;
        this.add(fileLabel, c);

        // create the file chooser and set its filter to supported mass spec file types
        fileChooser = new JFileChooser();
    }

    public void setFileOpenState(boolean state) {
        saveButton.setEnabled(state);
        closeButton.setEnabled(state);
    }

    private void openClicked(ActionEvent e) {
        // if user clicks "OK" (chose a file using the dialog):
        fileChooser.resetChoosableFileFilters();
        fileChooser.setFileFilter(openFilter);
        int fileChooserResult = fileChooser.showOpenDialog(frame);
        if(fileChooserResult == JFileChooser.APPROVE_OPTION) {
            String filePath = fileChooser.getSelectedFile().getPath();

            // disconnect and drop the previous mzTree
            closeClicked(e);
            frame.mzTree = new MzTree();

            // when the mzTree wants a conversion destination (when opening an mzML file for example):
            // try to get a save-as destination from the user. Fall back to the suggested default path
            // if there is an error or the user cancels.
            frame.mzTree.setConvertDestinationProvider(suggested -> {
                final Path[] userPath = new Path[1];
                try {
                    SwingUtilities.invokeAndWait(() -> userPath[0] = requestUserSavePath(suggested));
                } catch (InterruptedException|InvocationTargetException ex) {
                    userPath[0] = null;
                }
                if (userPath[0] == null) {
                    throw new Exception("User canceled file conversion.");
                }
                return userPath[0];
            });

            // process mzTree on new thread so that UI thread remains responsive
            Thread mzTreeThread = new Thread(() -> {
                long start = System.currentTimeMillis();
                try
                {
                    // attempt to create mzTree
                    frame.dataServer.setMzTree(frame.mzTree);
                    frame.mzTree.load(filePath, SummarizationStrategyFactory.Strategy.WeightedStriding);

                    LOGGER.log(Level.INFO, "MzTree load time: " + (System.currentTimeMillis() - start));

                    SwingUtilities.invokeLater(this::updateFileState);
                }
                catch (Exception ex)
                {
                    frame.mzTree = null;

                    LOGGER.log(Level.WARNING, "Could not open requested file", ex);
                }
            });

            // whenever the import state changes, update the status label
            // the <html> tag and escapeHtml are used so it word-wraps in the JLabel
            frame.mzTree.getImportState().addObserver((o, arg) -> {
                final String status = ((ImportState)o).getStatusString();
                SwingUtilities.invokeLater(() -> frame.setStatusText(status));
            });

            mzTreeThread.start();
        }
    }

    private void updateFileState() {
        if (frame.mzTree == null || frame.mzTree.getLoadStatus() != ImportState.ImportStatus.READY) {
            frame.setFileOpenState(false);
            fileLabel.setText(NO_FILE_TEXT);
            fileLabel.setToolTipText(null);
        } else {
            frame.setFileOpenState(true);
            String resultFilePath = frame.mzTree.dataStorage.getFilePath();
            fileLabel.setText("File: " + Paths.get(resultFilePath).getFileName());
            fileLabel.setToolTipText(resultFilePath);
        }

    }

    private void saveClicked(ActionEvent e) {
        // ensure model ready to save
        if(frame.mzTree == null || frame.mzTree.getLoadStatus() != ImportState.ImportStatus.READY){
            return;
        }

        // suggest a filename based on the date and time
        String suggestedFilename = new SimpleDateFormat("MM-dd-yyyy_HH-mm-ss").format(new Date());
        Path suggestedFilePath = Paths.get(frame.mzTree.dataStorage.getFilePath()).resolveSibling(suggestedFilename);

        Path targetFilepath = requestUserSavePath(suggestedFilePath);
        if (targetFilepath == null) {
            return;
        }

        try {
            // disconnect HTTP server while saving, reconnect after copied
            frame.dataServer.setMzTree(null);
            frame.mzTree.saveAs(targetFilepath);
            frame.dataServer.setMzTree(frame.mzTree);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(frame, ex.getMessage());
            LOGGER.log(Level.WARNING, "Could not copy mzTree file", ex);
        } finally {
            updateFileState();
        }
    }

    // shows a save dialog to ask for a path. null indicates error or cancellation
    private Path requestUserSavePath(Path suggestedFilePath) {
        // set up the file dialog
        fileChooser.resetChoosableFileFilters();
        fileChooser.setFileFilter(saveFilter);
        fileChooser.setSelectedFile(suggestedFilePath.toFile());

        // prompt to choose a file
        if (fileChooser.showSaveDialog(frame) != JFileChooser.APPROVE_OPTION) {
            return null;
        }

        // make sure it doesn't already exist and has the mzTree extension
        File targetFile = fileChooser.getSelectedFile();
        if (targetFile.exists()) {
            JOptionPane.showMessageDialog(frame, "The file already exists.");
            return null;
        }
        if (!targetFile.getName().endsWith(".mzTree")) {
            targetFile = new File(targetFile.getAbsolutePath() + ".mzTree");
        }

        // return normalized version of the user's chosen path
        return targetFile.toPath().normalize();
    }

    private void closeClicked(ActionEvent e) {
        frame.dataServer.setMzTree(null);
        if (frame.mzTree != null) {
            frame.mzTree.close();
            frame.mzTree = null;
        }

        updateFileState();
    }
}
