package edu.msViz.msHttpApi;

import net.sf.json.JSONObject;
import javax.servlet.http.HttpServletResponse;
import edu.msViz.mzTree.*;
import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.json.JSONArray;
import spark.Request;
import spark.Response;
import spark.Service;

/**
 * Http server using Spark web server library
 */
public final class MsDataServer {

    private static final Logger LOGGER = Logger.getLogger(MsDataServer.class.getName());
    
    // path to API root
    private static final String API_ROOT = "/api/v2";

    // MzTree data model
    private MzTree mzTree;
    
    // mutual exclusion lock (fair) for updating/saving
    private final ReentrantLock updateSaveLock = new ReentrantLock(true);
    
    public int getPort() {
        return spark.port();
    }

    private Service spark;

    public MsDataServer() { }

    /**
     * Starts the Spark web server listening on specified port 
     * Defines the http methods of the server
     * @param port Port number to listen on
     * @param frame server launch window's frame
     * @param strategy summarization strategy selected from the gui
     */
    public void startServer(int port) {
        spark = Service.ignite();
        spark.initExceptionHandler((e) -> LOGGER.log(Level.WARNING, "Failed to start Spark server", e));
        spark.port(port);

        if (new File("../client").exists()) {
            // development mode: use the local "client" folder
            // allows developer to run from and IDE and modify js/html files without needing to rebuild
            spark.externalStaticFileLocation("../client");
        } else {
            // production mode: use the files packaged into the jar at build time
            spark.staticFileLocation("/edu/msViz/client");
        }

        /*       Initialize Web API endpoints       */
        
        spark.get(API_ROOT + "/getpoints", this::getPoints);        
        
        spark.get(API_ROOT + "/filestatus", this::fileStatus);
                
    } // END startServer

    public void setMzTree(MzTree newTree) {
        try {
            updateSaveLock.lock();
            this.mzTree = newTree;
        }
        finally {
            updateSaveLock.unlock();
        }
    }

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

        // no mzTree assigned or not open yet
        if (mzTree == null || mzTree.getLoadStatus() == ImportState.ImportStatus.NONE) {
            response.status(HttpServletResponse.SC_NO_CONTENT);
            return "No file has been selected.";
        }

        switch (mzTree.getLoadStatus()) {

            // "loading" status types
            case PARSING:
            case CONVERTING:
            case LOADING_MZTREE:
                response.status(HttpServletResponse.SC_CONFLICT);
                return "The server is selecting or processing a file.";

            // "error" status types
            case ERROR:
                response.status(HttpServletResponse.SC_NOT_ACCEPTABLE);
                return "There was a problem opening the file.";

            case READY:
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

                    // numPoints == 0 means no limit
                    numPoints = numPoints == 0 ? Integer.MAX_VALUE : numPoints;
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

                List<MsDataPoint> queryResults = mzTree.query(mzmin, mzmax, rtmin, rtmax, numPoints);

                // serialize query results as JSON

                StringBuilder queryResultsJSON = JSONify(queryResults,numPoints,true);

                // respond with HTTP 200 OK
                response.status(HttpServletResponse.SC_OK);

                // format points as JSON
                return queryResultsJSON.toString();

            // unknown status type
            default:
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "";
        }
    } // END getPoints

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
        // no mzTree assigned or not open yet
        if (mzTree == null || mzTree.getLoadStatus() == ImportState.ImportStatus.NONE) {
            response.status(HttpServletResponse.SC_NO_CONTENT);
            return "No file has been selected.";
        }

        switch (mzTree.getLoadStatus()) {

            // "loading" status types
            case PARSING:
            case CONVERTING:
            case LOADING_MZTREE:
                response.status(HttpServletResponse.SC_CONFLICT);
                return mzTree.getLoadStatusString();

            // "error" status types
            case ERROR:
                response.status(HttpServletResponse.SC_NOT_ACCEPTABLE);
                return "There was a problem opening the file.";

            case READY:
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

                // point count
                try {
                    payload.put("pointcount", mzTree.dataStorage.getPointCount());
                } catch (Exception ex) {
                    payload.put("pointcount", null);
                }

                return payload;

            default:
                // default to HTTP 204 NO_CONTENT
                response.status(HttpServletResponse.SC_NO_CONTENT);
                return "";
        }
    } // END fileStatus

    /*****************************************************
    ||                      HELPERS                     ||
    *****************************************************/
    
    /**
     * Serializes a portio of an array of MsDataPoint objects into JSON format
     * @param msData Mass spec dataset
     * @param numPoints Number of points to return
     * @param includeExtras If True pointID and meta1 are included in serialization
     */
    private static StringBuilder JSONify(List<MsDataPoint> msData, int numPoints, boolean includeExtras)
    {
        // numPoints == 0 implies no limit
        if(numPoints == 0)
            numPoints = Integer.MAX_VALUE;
        
        StringBuilder JSON = new StringBuilder("[");
        if(!includeExtras)
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
                        .append(curPoint.meta1).append(",")
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
     * Stops the Spark web server
     */
    public void stopServer() {
        spark.stop();
    }

    /**
     * Blocks until the spark server is initialized
     */
    public void waitUntilStarted() {
        spark.awaitInitialization();
    }
    
}