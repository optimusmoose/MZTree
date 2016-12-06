/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.msHttpApi;

import java.util.Observable;
import java.util.Observer;

/**
 * Observer implementation for tracking the progress of an mzml import
 * or MzTree load
 * @author kyle
 */
public class ImportMonitor implements Observer
{   
    // status enumeration
    // PARSING, BUILDING, FINISED belong to importing an mzml
    // LOADING_MZTREE is an all-cases flag for loading MZTREE
    public static enum ImportStatus { PARSING, BUILDING, FINISHED, LOADING_MZTREE };

    // current import status
    private ImportStatus importStatus;
    
    // total work to be done (float for percentage calculation)
    private float totalWork;
    
    // amount of work currently completed
    private int workDone = 0;
    
    // path to source file (source)
    private String sourceFilePath;
    
    // path to mzTree file (destination)
    private String mzTreeFilePath;

    // work interval upon which to receive updates
    public int updateInterval = 1000;
    
    @Override
    public void update(Observable o, Object arg) 
    {
        
        // allow the updating of various tracked values
        // depending on incoming type
        
        // String --> sourceFilePath
        if (arg instanceof String)
            this.sourceFilePath = (String)arg;
        
        // int --> workDone
        else if (arg instanceof Integer)
            this.workDone = (int)arg;
        
        // float --> totalWork
        else if (arg instanceof Float)
            this.totalWork = (float)arg;
        
        // ImportStatus --> importStatus
        else if (arg instanceof ImportStatus)
            this.importStatus = (ImportStatus)arg;
    }
    
    /**
     * Setter for mzTreeFilePath
     * @param path filepath to mzTree file being created or loaded from
     */
    public void setMzTreeFilePath(String path){
        this.mzTreeFilePath = path;
    }
    
    /**
     * Reports the progress of an mzml import / mzTree load
     * @return 
     */
    public String getStatus()
    {
        // switch on the status of the import
        switch(this.importStatus){
            
            // parsing mzml
            case PARSING:
                return "Parsing " + this.sourceFilePath;
            
            // building mzTree from parsed mzml
            case BUILDING:
                if (this.mzTreeFilePath == null)
                    return "Creating mzTree file";
                else
                {
                    int percentDone = (int)((this.workDone / this.totalWork) * 100);
                    return "Importing to " + this.mzTreeFilePath + " || " + percentDone + "%";
                }
                    
            // finished
            case FINISHED:
                return "Import complete: " + this.mzTreeFilePath;
            
            // loading MzTree from .mztree
            case LOADING_MZTREE:
                if (this.mzTreeFilePath == null)
                    return "Connecting to MzTree";
                else
                    return "Loading MzTree: " + this.mzTreeFilePath;
                
            // default implies error
            default:
                return "Server Error - Import status unknown";
        }
    }
}
