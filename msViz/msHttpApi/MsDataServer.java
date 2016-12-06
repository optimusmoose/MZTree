package edu.msViz.msHttpApi;

import net.sf.json.JSONObject;
import javax.swing.JFileChooser;
import javax.swing.SwingUtilities;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.servlet.http.HttpServletResponse;
import edu.msViz.mzTree.*;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory.Strategy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.DataFormatException;
import javax.swing.JFrame;
import net.sf.json.JSONArray;
import spark.Request;
import spark.Response;
import spark.Spark;

/**
 * Http server uzing Spark web server library
 */
public final class MsDataServer {

    // tracks the most recently used port
    private int lastPort;
    
    // path to API root
    private static final String API_ROOT = "/api/v1";
    
    // location to output saved files
    public static final String OUTPUT_LOCATION = "../output/";
    
    // Enumeration of file selection statuses
    public static enum FileStatus { NO_FILE_SELECTED, SELECTING_FILE, PROCESSING_FILE, WRONG_TYPE_SELECTED, FILE_READY };
    
    // File selection status
    public FileStatus fileStatus = FileStatus.NO_FILE_SELECTED;
    
    // Swing file chooser 
    private final JFileChooser fileChooser = new JFileChooser();
    
    // MzTree data model
    private MzTree mzTree;
    
    // Server's launch window
    private JFrame serverFrame;
    
    // Worker thread for building mzTree
    private Thread mzTreeThread;
    
    // Strategy selected by the user in the GUI
    private Strategy selectedStrategy;
    
    // mutual exclusion lock (fair) for updating/saving
    private final ReentrantLock updateSaveLock = new ReentrantLock(true);

    // progress monitor for importing mzml
    private ImportMonitor importMonitor;
    
    private CommandStack commandStack;
    
    public int getPort() {
        return lastPort;
    }
    
    private MsDataServer() {}
    
    // Singleton pattern for MsDataServer
    private static final MsDataServer instance = new MsDataServer();
    public static MsDataServer getInstance() {
        return instance;
    }

    /**
     * Starts the Spark web server listening on specified port 
     * Defines the http methods of the server
     * @param port Port number to listen on
     * @param frame server launch window's frame
     * @param strategy summarization strategy selected from the gui
     */
    public void startServer(int port, JFrame frame, Strategy strategy) {
        serverFrame = frame;
        selectedStrategy = strategy;
        lastPort = port;
        
        Spark.port(port);

        // set the fileChooser to accept only mass spec data file types
        fileChooser.setFileFilter(new FileNameExtensionFilter("Mass Spectrometry Data File", "mzML", "db", "mzTree","csv"));
        
        if (new File("../client").exists()) {
            // development mode: use the local "client" folder
            // allows developer to run from and IDE and modify js/html files without needing to rebuild
            Spark.externalStaticFileLocation("../client");
        } else {
            // production mode: use the files packaged into the jar at build time
            Spark.staticFileLocation("/edu/msViz/client");
        }

        /*       Initialize Web API endpoints       */
        
        Spark.get(API_ROOT + "/getpoints", this::getPoints);

        Spark.get(API_ROOT + "/openfile", this::openFile); 
        
        Spark.get(API_ROOT + "/filestatus", this::fileStatus);
        
        Spark.get(API_ROOT + "/savemodel", this::saveModel);
        
        Spark.post(API_ROOT + "/updatesegmentation", this::updateSegmentation);
        
    } // END startServer
    
    //****************************************************
    //                 WEB API FUNCTIONS                ||
    //***************************************************/
    
    /**
     * Processes a query to the data model for data points
     * 
     * API ENDPOINT: GET /getpoints
     * HTTP GET PARAMETERS: 
     *      mzmin -> double : lower mz query bound
     *      mzmax -> double : upper mz query bound
     *      rtmin -> float : lower rt query bound
     *      rtmax -> float : upper rt query bound
     *      numpoints -> int : number of points to return
     * 
     * @param request Spark request object containing HTTP request components
     * @param response Spark response object returned to requester
     * @return Server message
     */
    private Object getPoints(Request request, Response response){
        // respond according to fileStatus
        switch(fileStatus){
            case NO_FILE_SELECTED:

                // respond with HTTP 204 NO_CONTENT
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "No file has been selected.";

            case SELECTING_FILE:
            case PROCESSING_FILE:

                // respond with HTTP 409 CONFLICT
                response.status(HttpServletResponse.SC_CONFLICT);
                return "The server is selecting or processing a file.";

            case WRONG_TYPE_SELECTED:

                // respond with HTTP 406 NOT_ACCEPTABLE
                response.status(HttpServletResponse.SC_NOT_ACCEPTABLE);
                return "The previously selected file is of the wrong file format.";

            case FILE_READY:

                // get request parameters (query bounds)
                double mzmin, mzmax;
                float rtmin, rtmax;
                double intmin = 0;
                int numPoints;
                try{
                    // parse paramaters from request url
                    mzmin = Double.parseDouble(request.queryParams("mzmin"));
                    mzmax = Double.parseDouble(request.queryParams("mzmax"));
                    rtmin = Float.parseFloat(request.queryParams("rtmin"));
                    rtmax = Float.parseFloat(request.queryParams("rtmax"));
                    numPoints = Integer.parseInt(request.queryParams("numpoints"));
                    String intminS = request.queryParams("intmin");
                    if (intminS != null && !intminS.isEmpty()) {
                        intmin = Double.parseDouble(intminS);
                    }
                }
                // catch cases where parameter not included
                catch (NullPointerException ex)
                {
                    response.status(HttpServletResponse.SC_BAD_REQUEST);
                    return "One or more URL parameters missing.";
                }

                // ensure a valid range
                if(mzmax < mzmin || rtmax < rtmin)
                {
                    response.status(HttpServletResponse.SC_BAD_REQUEST);
                    return "Invalid data range requested.";
                }

                // query the mzTree for points within the bounds
                //long start = System.currentTimeMillis();

                List<MsDataPoint> queryResults = mzTree.query(mzmin, mzmax, rtmin, rtmax, numPoints, intmin, true);
                //System.out.println("Query exec time (ms): " + String.valueOf(System.currentTimeMillis() - start));

                // serialize query results as JSON
                
                StringBuilder queryResultsJSON = JSONify(queryResults,numPoints,true);
                //System.out.println("JSON exec time (ms): " + String.valueOf(System.currentTimeMillis() - start));

                // serialize traceMap
                JSONObject traceMapJSON = new JSONObject();
                for(Map.Entry<Short,Short> trace : mzTree.traceMap.entrySet()){
                    if(trace.getValue() == null)
                        traceMapJSON.put(trace.getKey(), 0);
                    else
                        traceMapJSON.put(trace.getKey(), trace.getValue());
                }
                String traceMapJSONString = traceMapJSON.toString();
                
                // respond with HTTP 200 OK
                response.status(HttpServletResponse.SC_OK);

                // format points and traceMap as single JSON object
                queryResultsJSON.insert(0,"{\"points\":").append(",\"traceMap\": ").append(traceMapJSONString).append("}");
                return queryResultsJSON.toString();

            default:

                // default to HTTP 204 NO_CONTENT
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "";
        }
    } // END getPoints
    
    /**
     * Launches a file chooser
     * Upon file selection loads the mzTree data model from the file
     * 
     * API ENDPOINT: GET /openfile
     * 
     * @param request Spark request object containing HTTP request components
     * @param response Spark response object returned to requester
     * @return Server message
     */
    private Object openFile(Request request, Response response){
        
        // continue only if not already selecting file
        if (fileStatus != FileStatus.SELECTING_FILE) {
            FileStatus prevFileStatus = fileStatus;

            // set selecting file flag to true
            fileStatus = FileStatus.SELECTING_FILE;

            // invoke file chooser on ui thread when ready
            // allows server to respond without waiting on file operations
            SwingUtilities.invokeLater(() -> {

                // shows an open file dialog, blocks until dialog addressed
                // returns result code of open file dialog
                serverFrame.toFront();
                
                int fileChooserResult = fileChooser.showOpenDialog(serverFrame);
                
                // branch on file chooser result code
                switch(fileChooserResult){

                    //case: file selected (APPROVE_OPTION = 0)
                    case JFileChooser.APPROVE_OPTION:
                        
                        // get user choice on load options -> 0 | -1: load entire dataset; 1: partition dataset
                        //int loadOptionResult = JOptionPane.showConfirmDialog(this.serverFrame, "Conserve memory on load?", "Memory Conservation", JOptionPane.YES_NO_OPTION);
                        
                        // begin processing file
                        fileStatus = FileStatus.PROCESSING_FILE;

                        // get file metaData from file chooser
                        String filePath = fileChooser.getSelectedFile().getPath();

                        // process mzTree on new thread so that server thread can respond to request
                        mzTreeThread = new Thread(){

                            @Override
                            public void run(){
                                long start = System.currentTimeMillis();
                                importMonitor = new ImportMonitor();
                                
                                // attempt opening file and constructing mzTree
                                try 
                                {
                                    // create mzTree
                                    mzTree = new MzTree(importMonitor);
                                    mzTree.load(filePath, selectedStrategy, true);
                                    //mzTree.load(filePath, importMonitor, selectedStrategy, loadOptionResult == 0);
                                    
                                    
                                    // indicate success
                                    fileStatus = FileStatus.FILE_READY;
                                    
                                    // initialize new command stack on succesful file load
                                    commandStack = new CommandStack();
                                } 

                                // file processing exceptions
                                catch (Exception ex) 
                                {
                                    if(ex instanceof DataFormatException)
                                        // could not process file type
                                        fileStatus = FileStatus.WRONG_TYPE_SELECTED;
                                    else{
                                        // revert back to no file
                                        fileStatus = FileStatus.NO_FILE_SELECTED;
                                        System.err.println(ex.getMessage());
                                        ex.printStackTrace();
                                    }
                                }
                                System.out.println("MzTree load time: " + (System.currentTimeMillis() - start));
                            }
                        };
                        mzTreeThread.start();

                        break;

                    //case: dialog cancelled (CANCEL_OPTION = 1)
                    case JFileChooser.CANCEL_OPTION:
                        fileStatus = prevFileStatus;
                        break;

                    default:
                        fileStatus = FileStatus.NO_FILE_SELECTED;
                        break;
                }
            });

            // respond with HTTP 200 OK
            response.status(HttpServletResponse.SC_OK);
            return "Server has begun choosing a file.";
        } 
        else {
            // respond with HTTP 403 Forbidden
            response.status(HttpServletResponse.SC_FORBIDDEN);
            return "Server is already choosing a file.";
        }
    } // END openFile
    
    /**
     * Query on the status of the data model
     * 
     * API ENDPOINT: GET /filestatus
     * 
     * @param request Spark request object containing HTTP request components
     * @param response Spark response object returned to requester
     * @return Server message
     */
    private Object fileStatus(Request request, Response response){
        // switch on fileStatus
        switch (fileStatus) {

            // case: FILE_READY
            case FILE_READY:
                // respond with HTTP 200 OK
                response.status(HttpServletResponse.SC_OK);

                // serialize payload as JSON
                JSONObject payload = new JSONObject();

                // mz min and max
                payload.put("mzmin",mzTree.head.mzMin); payload.put("mzmax",mzTree.head.mzMax);

                // rt min and max
                payload.put("rtmin",mzTree.head.rtMin); payload.put("rtmax",mzTree.head.rtMax);

                // rt min and max
                payload.put("intmin",mzTree.head.intMin); payload.put("intmax",mzTree.head.intMax);
                
                return payload;

            // case: NO_FILE_SELECTED
            case NO_FILE_SELECTED:
                // respond with HTTP 204 NO_CONTENT
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "No file has been selected. Select a file before continuing.";

            // case: WRONG_TYPE_SELECTED
            case WRONG_TYPE_SELECTED:
                // respond with HTTP 406 NOT_ACCEPTABLE
                response.status(HttpServletResponse.SC_NOT_ACCEPTABLE);
                return "The previously selected file was of the wrong type. Select a new file before continuing.";

            // case: SELECTING_FILE
            case SELECTING_FILE:
                // respond with HTTP 403 FORBIDDEN
                response.status(HttpServletResponse.SC_FORBIDDEN);
                return "A file is currently being selected on the server.";

            //case: PROCESSING_FILE
            case PROCESSING_FILE:
                // respond with HTTP 409 CONFLICT
                response.status(HttpServletResponse.SC_CONFLICT);
                return importMonitor == null ? "Launching import" : importMonitor.getStatus();
        }
        return "";
    } // END fileStatus
    
    /**
     * Saves the current data model to the output location with the specified file name
     * 
     * API ENDPOINT: GET /savemodel
     * HTTP GET PARAMETERS: 
     *      filename -> string : name to save file under
     * 
     * @param request Spark request object containing HTTP request components
     * @param response Spark response object returned to requester
     * @return Server message
     * @throws IOException 
     */
    private Object saveModel(Request request, Response response) throws IOException {

        // ensure model ready to save
        if(mzTree == null || fileStatus != FileStatus.FILE_READY){
            response.status(HttpServletResponse.SC_NO_CONTENT);
            return "No data model exists";
        }

        // source file path
        Path sourceFilepath = Paths.get(mzTree.dataStorage.getFilePath());
        
        // retrieve filename
        String filename = request.queryParams("filename");

        // use datetime as filename if none provided
        if(filename == null)
            filename = new SimpleDateFormat("MM-dd-yyyy HH-mm-ss").format(new Date()) + ".mzTree";

        // ensure the output location exists
        File outputDirectory = new File(OUTPUT_LOCATION);
        if(!outputDirectory.exists()) outputDirectory.mkdir();

        //long start = System.currentTimeMillis();
        
        // location of new file
        Path targetFilepath = Paths.get(outputDirectory.getCanonicalPath().replace("\\", "/") + "/" + filename);
            
        // if the intended save destination already exists...
        if(new File(targetFilepath.toString()).exists()){
            // ... then this is an invalid request, return HTTP 409 CONFLICT
            response.status(HttpServletResponse.SC_CONFLICT);
            return "A file " + targetFilepath + " already exists";
        }

        try{
            
            // synchronize updating and saving
            updateSaveLock.lock();
            
            // close current storage connection
            mzTree.dataStorage.close();

            // copy current output location to new output location
            mzTree.dataStorage.copy(targetFilepath);

            // init connection to new database
            mzTree.dataStorage.init(targetFilepath.toString(), null);
            
        }
        catch(Exception e){
            
            System.err.println("Could not create copy at " + targetFilepath.toString() + " || " + e.getMessage());
            e.printStackTrace();
            
            try{
                // revert back to previous connection
                mzTree.dataStorage.init(sourceFilepath.toString(), null);
            }
            catch(Exception ex){
                System.err.println("After failed copy, could not revert back to " + sourceFilepath.toString() + " || " + ex.getMessage());
                e.printStackTrace();
            }
            
            response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return e.getMessage();
        }
        finally {
            updateSaveLock.unlock();
        }
        
        //System.out.println("Savemodel took " + (System.currentTimeMillis() - start) + " ms");

        // return results
        response.status(HttpServletResponse.SC_OK);
        JSONObject payload = new JSONObject().element("filepath", mzTree.dataStorage.getFilePath());
        return payload;

    } // END saveModel
    
    /**
     * Updates segmentation data in the mzTree and data storage implementation
     * 
     * API ENDPOINT: POST /updatesegmentation
     * HTTP POST PARAMETERS:
     *      [ <action>, <action>, ... ]
     *      <action> (one of the following):
     *          { "type": "undo" }
     *          { "type": "redo" }
     *          { "type": "set-trace", "trace": <traceid>, "points": [<pointid>, ...] }
     *          { "type": "set-envelope", "envelope": <envelopeid>, "traces": [<traceid>, ...] }
     *          { "type": "rectangle", "bounds": [<lower_mz>, <upper_mz>, <lower_rt>, <upper_rt>], "id": <traceID>, "isAdd": <boolean> }
     * @param request Spark request object containing HTTP request components
     * @param response Spark response object returned to requester
     * @return Server message
     * @return 
     */
    private Object updateSegmentation(Request request, Response response) {

        try {
            updateSaveLock.lock();
            //System.out.println("-------------------------------------");
            //System.out.println("trace map before update: " + mzTree.traceMap.toString());

            // respond according to fileStatus
            if (mzTree == null || fileStatus != FileStatus.FILE_READY) {
                // respond with HTTP 204 NO_CONTENT
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "No data loaded or not ready.";
            }

            // parse root level JSONArray of actions
            JSONArray actions = JSONArray.fromObject(request.body());

            // iterate through each received action
            for (int i = 0; i < actions.size(); i++) {

                // parse action
                JSONObject action = actions.getJSONObject(i);

                // get action type
                String actionType = action.getString("type");

                // command to perform
                Command cmd = null;

                // switch on action type
                switch (actionType) {

                    // undo action
                    case "undo":
                        this.commandStack.undoCommand();
                        break;

                    // redo action
                    case "redo":
                        this.commandStack.redoCommand();
                        break;

                    // set-trace action (brush tracing)
                    case "set-trace":

                        // parse traceID
                        short traceID = (short) action.getInt("trace");

                        // parse point IDs
                        Integer[] pointIDs = (Integer[]) action.getJSONArray("points").toArray(new Integer[0]);

                        // construct trace command
                        cmd = new TraceCommand(mzTree, traceID, pointIDs);
                        break;

                    // set-envelope action
                    case "set-envelope":

                        // parse envelopeID
                        short envelopeID = (short) action.getInt("envelope");

                        // parse traceIDs
                        Integer[] traceIDs = (Integer[]) action.getJSONArray("traces").toArray(new Integer[0]);

                        // construct envelope command
                        cmd = new EnvelopeCommand(mzTree, envelopeID, traceIDs);
                        break;

                    // rectangular trace action
                    case "rectangle":

                        // parse traceID
                        short _traceID = (short) action.getInt("id");

                        // parse rectangle bounds
                        Double[] bounds = (Double[]) action.getJSONArray("bounds").toArray(new Double[0]);

                        // parse add flag
                        boolean isAdd = action.getBoolean("isAdd");

                        // construct rectangular trace command
                        cmd = new RectangularTraceCommand(mzTree, mzTree.dataStorage, _traceID, bounds, isAdd);
                        break;
                }

                // if there is a new command to perform
                // then perform it via the command stack
                if (cmd != null) {
                    commandStack.doCommand(cmd);
                }
            }

            //System.out.println("trace map after update: " + mzTree.traceMap.toString());
        } 
        catch(Exception e)
        {
            e.printStackTrace();
            response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return e.getMessage();
        }
        finally {
            updateSaveLock.unlock();
        }

        // respond with HTTP 200 OK
        response.status(HttpServletResponse.SC_OK);

        return "";

    } // END updateSegmentation

    /*****************************************************
    ||                      HELPERS                     ||
    *****************************************************/
    
    /**
     * Serializes a portio of an array of MsDataPoint objects into JSON format
     * @param msData Mass spec dataset
     * @param numPoints Number of points to return
     * @param includeIDs If True traceID and envelopeID are included in serialization
     */
    private static StringBuilder JSONify(List<MsDataPoint> msData, int numPoints, boolean includeIDs)
    {
        // numPoints == 0 implies no limit
        if(numPoints == 0)
            numPoints = Integer.MAX_VALUE;
        
        StringBuilder JSON = new StringBuilder("[");
        if(!includeIDs)
            for(int i = 0; i < numPoints && i < msData.size(); i++)
            {
                MsDataPoint curPoint = msData.get(i);
                JSON.append("[").append(curPoint.mz).append(",")
                        .append(curPoint.rt).append(",")
                        .append(curPoint.intensity).append("],");
            }
                   
        else
            for(int i = 0; i < numPoints && i < msData.size(); i++)
            {
                MsDataPoint curPoint = msData.get(i);
                JSON.append("[")
                        .append(curPoint.pointID).append(",")
                        .append(curPoint.traceID).append(",")
                        .append(curPoint.mz).append(",")
                        .append(curPoint.rt).append(",")
                        .append(curPoint.intensity).append("],");
            }
        
        if(msData.size() > 0)
            JSON.replace(JSON.length()-1, JSON.length(), "]");
        else 
            JSON.append("]");
        
        return JSON;
    }
    
    /**
     * Exports a specified data range to a CSV file at the given path
     * @param filepath
     * @param minMZ
     * @param maxMZ
     * @param minRT
     * @param maxRT
     * @throws IOException 
     */
    public void exportCSV(String filepath, double minMZ, double maxMZ, float minRT, float maxRT) throws IOException
    {
        this.mzTree.export(filepath, minMZ, maxMZ, minRT, maxRT);
    }
    
    /**
     * Stops the Spark web server
     */
    public void stopServer() {
        
        try{
            if (mzTree != null && mzTree.dataStorage != null) {
                mzTree.dataStorage.close();
            }
        }
        catch(Exception e){
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
        finally{
            mzTree = null;
            Spark.stop();
        }
        

    }

    /**
     * Blocks until the spark server is initialized
     */
    public void waitUntilStarted() {
        Spark.awaitInitialization();
    }
    
    public long processFileOpen(String path, boolean shouldDelete) throws Exception
    {
        long start = System.currentTimeMillis();
        MzTree tree = new MzTree(new ImportMonitor());
        tree.load(path, SummarizationStrategyFactory.Strategy.WeightedStriding, false);
        long exectime = System.currentTimeMillis() - start;
        
        if(shouldDelete)
            tree.close();
        
        return exectime;
    }
    
}