package edu.msViz.msHttpApi;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

class ServerPanel extends JPanel {
    private static final Logger LOGGER = Logger.getLogger(ServerPanel.class.getName());

    private static final String START_TEXT = "Start Server";
    private static final String STOP_TEXT = "Stop Server";

    private MsDataServer dataServer;

    // UI items
    private JTextField portEntry;
    private JButton startStopButton;

    // flag to track if server is running
    private boolean running = false;

    public ServerPanel(MsDataServer controlledServer) {
        this.dataServer = controlledServer;

        this.setLayout(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 2, 2, 2);

        // ROW 0: port field
        c.gridy = 0;
        c.fill = GridBagConstraints.HORIZONTAL;

        JLabel portLabel = new JLabel("Port:");
        portLabel.setHorizontalAlignment(JLabel.RIGHT);
        this.add(portLabel, c);

        portEntry = new JTextField("4567");
        c.gridx = 1;
        this.add(portEntry, c);

        // ROW 1: start button
        c.gridy = 1;

        startStopButton = new JButton(START_TEXT);
        startStopButton.setMnemonic('S');
        startStopButton.addActionListener(this::startStopClicked);
        c.gridx = 0;
        c.gridwidth = 2;
        this.add(startStopButton, c);
    }

    private void startStopClicked(ActionEvent e) {
        if (!running) {
            try {
                int port = Integer.parseInt(portEntry.getText());
                this.dataServer.startServer(port);
                this.dataServer.waitUntilStarted();
            } catch (NumberFormatException ex) {
                portEntry.setText("4567");
            }
        } else {
            this.dataServer.stopServer();
        }
        running = !running;
        startStopButton.setText(running ? STOP_TEXT : START_TEXT);
    }
}
