/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.mzTree.storage;

import edu.msViz.msHttpApi.ImportMonitor;
import edu.msViz.msHttpApi.MsDataServer;
import edu.msViz.mzTree.MsDataPoint;
import edu.msViz.mzTree.MzTree;
import edu.msViz.mzTree.MzTreeNode;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * StorageFacade implementation that utilized SQLite for persistance
 * @author Kyle
 */
public class HybridStorage implements StorageFacade
{
    // connection to the SQLite database
    private Connection dbConnection;
    
    // SQL statement preparation and execution
    private SQLEngine dbEngine;
    
    // point value access
    private PointEngine pointEngine;
    
    // path to the database and point files
    private String filePath;
    private String pointFilePath;
    
    // work done counter (number of points || nodepoints saved)
    private int workDone = 0;
    
    //**********************************************//
    //                    INIT                      //
    //**********************************************//
    
    @Override
    public void init(String filePath, Integer numPoints) throws Exception
    {
        
        // generate output file location if none passed
        if(filePath == null)
        {
            // date time file name
            String filename = new SimpleDateFormat("MM-dd-yyyy_HH-mm-ss").format(new java.util.Date()) + ".mzTree";

            // ensure the output location exists
            File outputDirectory = new File(MsDataServer.OUTPUT_LOCATION);
            if(!outputDirectory.exists()) outputDirectory.mkdir();
            
            filePath = outputDirectory.getCanonicalPath().replace("\\", "/") + "/" + filename;
            
        }
        
        this.filePath = filePath;
        this.pointFilePath = filePath + "-points";
        
        try{
            // link the JDBC-sqlite class
            Class.forName("org.sqlite.JDBC");
            
            // connect to sqlite database at specified location
            // if doesn't exist then new database will be created
            this.dbConnection = DriverManager.getConnection("jdbc:sqlite:" + this.filePath);
            
            // enable foreign keys
            this.dbConnection.createStatement().execute("PRAGMA foreign_keys=ON;");
            this.dbConnection.createStatement().execute("PRAGMA page_size = 4096;");
            
            // disable auto commit (enables user defined transactions)
            this.dbConnection.setAutoCommit(false);
            
            // construct SQL Engine and Point Engine
            this.dbEngine = new SQLEngine();
            this.pointEngine = new PointEngine();
            
            // reserve space for the number of incoming points
            if(numPoints != null)
                this.pointEngine.reserveSpace(numPoints);
            
        }
        catch(ClassNotFoundException | SQLException e)
        {
            System.err.println("Unable to initialize database connection at " + this.filePath + " || " + e.getMessage());
            
            // SQLExceptions contain leading tags that describe what ACTUALLY happened
            // looking for case when the file chosen was not a database
            if(e.getMessage().contains("[SQLITE_NOTADB]"))
                throw new DataFormatException(e.getMessage());
            
            else throw e;
        }
    }
    
    //**********************************************//
    //                 LOAD MODEL                   //
    //**********************************************//
    
    @Override
    public void loadModel(MzTree mzTree) throws Exception
    {
        // tree's root node
        MzTreeNode rootNode = new MzTreeNode();

        // read all traces from the trace table, insert into traceMap
        this.loadTraceMap(mzTree);

        // query for root node
        ResultSet rootNodeResult = this.dbConnection.createStatement().executeQuery(this.dbEngine.selectRootNodeStatement);

        // move cursor to first (and only) result 
        rootNodeResult.next();

        // assign node values to root node
        this.assignNodeValues(rootNode, rootNodeResult);

        // recursively build tree
        this.recursiveTreeBuilder(rootNode,mzTree,0);

        // finish constructing the tree
        mzTree.head = rootNode;
    }
    
    /**
     * Recursively builds an MzTree starting with the root node.
     * Retrieves and constructs the MzTreeNode specified by the 
     * nodeID (and all descendents) from the database
     * @param node Node from which to start recursive MzTree construction
     * @return number of points in the tree
     */
    private void recursiveTreeBuilder(MzTreeNode node, MzTree mzTree, int curDepth) throws SQLException
    {
        // branching conditions:
        
        // if true then node will collect min/max intensities
        boolean isLeaf;
        
        // query database for all child nodes
        ArrayList<MzTreeNode> childNodes = this.dbEngine.selectNode(node.nodeID, true);
        
        // isLeaf == has children
        isLeaf = childNodes.isEmpty();
        
        // leaf nodes update tree height (results in largest height)
        if(isLeaf)
            mzTree.treeHeight = (curDepth > mzTree.treeHeight) ? (short)curDepth : mzTree.treeHeight;
        
        // recurse on each child node if there are any
        for(MzTreeNode childNode : childNodes)
        {
            // recursive call at +1 depth
            this.recursiveTreeBuilder(childNode, mzTree, curDepth+1);

            // add reference to child and keep min/max mz/rt/int
            node.addChildGetBounds(childNode);
        }
    }
    
    
    //**********************************************//
    //                  SAVE POINTS                 //
    //**********************************************//

    @Override
    public void savePoints(SavePointsTask task, ImportMonitor importMonitor) throws IOException
    {   
        
        // inform the MzTreeNode of its position in the file and number of points
        task.node.fileIndex = this.pointEngine.getCurrentFileLocation();
        task.node.numSavedPoints = task.dataset.size();
        
        // iterate through all points
        for(MsDataPoint point : task.dataset)
        {
            // insert point into database
            this.pointEngine.insert(point);

            // a point is a single unit of work
            this.workDone++;

            // if update interval reached then send update to importMonitor
            if(this.workDone % importMonitor.updateInterval == 0)
                importMonitor.update(null, this.workDone);
        }
    }
    
    @Override
    public ArrayList<Integer> getNodePointIDs(int nodeID) throws Exception {
        return this.dbEngine.selectPointIDsByNode(nodeID);
    }
    
    /**
     * Assigns to the node the values at the current result set cursor position
     * @param node MzTreeNode to assign values to
     * @param rs ResultSet containing Node results, expects cursor to be in correct position
     * @throws SQLException 
     */
    private void assignNodeValues(MzTreeNode node, ResultSet rs) throws SQLException
    {
        node.nodeID = rs.getInt(1);
        
        // the jdbc way to determine nulls. good stuff
        node.fileIndex = rs.getLong(2);
        if(rs.wasNull())
            node.fileIndex = null;
        
        node.numSavedPoints = rs.getInt(3);
        if(rs.wasNull())
            node.numSavedPoints = null;
        
        node.mzMin = rs.getDouble(4);
        node.mzMax = rs.getDouble(5);
        node.rtMin = rs.getFloat(6);
        node.rtMax = rs.getFloat(7);
        node.intMin = rs.getDouble(8);
        node.intMax = rs.getDouble(9);
    }
    
    /**
     * Reads all traces from the database and inserts them into the traceMap
     * @param mzTree
     * @throws SQLException 
     */
    private void loadTraceMap(MzTree mzTree) throws SQLException 
    {
        mzTree.traceMap = dbEngine.loadTraceMap();
    }
    
    //**********************************************//
    //               UPDATE TRACES                  //
    //**********************************************//
    
    @Override
    public void updateTraces(short traceID, Integer[] targets) throws SQLException, IOException
    {
        // iterate through all pointID/traceID pairs
        for (Integer pointID : targets) {
            this.pointEngine.updatePointTrace(pointID, traceID);
        }
        
        this.dbConnection.commit();
    }
    
    @Override
    public void insertTrace(short traceID, short envelopeID) throws SQLException
    {
        this.dbEngine.insertTrace(traceID, envelopeID);
        
        this.dbConnection.commit();
    }
    
    //**********************************************//
    //               DELETE TRACE                  //
    //**********************************************//
    
    @Override
    public void deleteTrace(short traceID) throws SQLException, IOException
    {
        // update point's trace
        this.dbEngine.deleteTrace(traceID);
        
        this.dbConnection.commit();
    }
    
    //**********************************************//
    //               UPDATE ENVELOPES               //
    //**********************************************//
    
    @Override
    public void updateEnvelopes(short envelopeID, Integer[] targets) throws SQLException
    {
        // iterate through all traceID,envelopeID pairs
        for (Integer traceID : targets) {
            this.dbEngine.updateTrace(traceID.shortValue(), envelopeID);
        }
        
        this.dbConnection.commit();
    }
    
    //**********************************************//
    //                  SAVE NODE                   //
    //**********************************************//
    
    @Override
    public int saveNode(MzTreeNode childNode, int parentNodeID) throws SQLException
    {
        return this.dbEngine.insert(childNode, parentNodeID);
    }
    
    //**********************************************//
    //               SAVE NODE POINTS               //
    //**********************************************//
    
    @Override
    public void saveNodePoints(MzTreeNode node, ImportMonitor importMonitor) throws SQLException
    {
        // iterate through all pointIDs
        for(int pointID : node.pointIDs)
        {
            // insert nodepoint relationship entity
            this.dbEngine.insert(node.nodeID, pointID);
            
            // a nodepoint entity is a single unit of work
            this.workDone++;
            
            // if update interval reached send update to importmonitor
            if(this.workDone % importMonitor.updateInterval == 0)
                importMonitor.update(null, this.workDone);
        }
    }
    
    //**********************************************//
    //                  LOAD POINTS                 //
    //**********************************************//
    
    @Override
    public List<MsDataPoint> loadPoints(List<Integer> pointIDs) throws IOException
    {
        return this.pointEngine.selectPoints(pointIDs);
    }
    
    @Override
    public List<MsDataPoint> loadLeavesPointsInBounds(List<MzTreeNode> leaves, double mzmin, double mzmax, float rtmin, float rtmax) throws IOException
    {
        List<MsDataPoint> results = new ArrayList<>();
        
        for(MzTreeNode leaf : leaves)
        {
            results.addAll(this.pointEngine.selectLeafPointsInBounds(leaf, mzmin, mzmax, rtmin, rtmax));
        }
        
        return results;
    }
    
    
    @Override
    public List<MsDataPoint> loadLeavesPointsInBoundsClumped(List<MzTreeNode> leaves, double mzmin, double mzmax, float rtmin, float rtmax) throws IOException
    {
        List<MsDataPoint> results = new ArrayList<>();
        List<Long> blockStarts = new ArrayList<>();
        List<Long> blockLengths = new ArrayList<>();
        
        // ensure that the leaf nodes are sorted by fileIndex
        leaves.sort(Comparator.comparing((MzTreeNode node) -> node.fileIndex));
        
        // currently accumulating block start and length
        long accumulatingBlockStart = leaves.get(0).fileIndex;
        long accumulatingBlockLength = leaves.get(0).numSavedPoints * MsDataPoint.DISK_NUM_BYTES_PER_POINT;
        
        // iterate through the remaining nodes, conjoining adjacent node blocks
        int i = 1;
        while(i < leaves.size())
        {
            MzTreeNode curNode = leaves.get(i);
            
            // if the current node's block is adjacent to the accumulating block then accumulate it
            if(curNode.fileIndex == accumulatingBlockStart + accumulatingBlockLength)
                accumulatingBlockLength += curNode.numSavedPoints * MsDataPoint.DISK_NUM_BYTES_PER_POINT;
            
            // else collect the accumulated block and begin a new one
            else
            {
                // collect accumulated block
                blockStarts.add(accumulatingBlockStart);
                blockLengths.add(accumulatingBlockLength);
                
                // begin new accumulating block
                accumulatingBlockStart = curNode.fileIndex;
                accumulatingBlockLength = curNode.numSavedPoints * MsDataPoint.DISK_NUM_BYTES_PER_POINT;
            }
            
            i++;
        }
        
        // collect final accumulated block
        blockStarts.add(accumulatingBlockStart);
        blockLengths.add(accumulatingBlockLength);
        
        // collect points within bounds from each of the blocks
        for(i = 0; i < blockStarts.size(); i++)
        {
            results.addAll(this.pointEngine.selectBlockPointsInBounds(blockStarts.get(i), blockLengths.get(i), mzmin, mzmax, rtmin, rtmax));
        }
        
        return results;
        
    }
    
    
    //**********************************************//
    //                    FLUSH                     //
    //**********************************************//
    
    @Override
    public void flush() throws SQLException, IOException
    {
        this.dbConnection.commit();
        this.pointEngine.flush();
    }
    
    //**********************************************//
    //                 GET FILE PATH                //
    //**********************************************//
    
    @Override
    public String getFilePath()
    {
        return this.filePath;
    }
    
    //**********************************************//
    //                    CLOSE                     //
    //**********************************************//
    
    @Override
    public void close() throws Exception
    {
        this.flush();
        this.dbConnection.close();
        this.pointEngine.pointFile.close();
    }

    @Override
    public void copy(Path targetFilepath) throws IOException {
        Path targetPointFilepath = Paths.get(targetFilepath.toString() + "-points");
        Files.copy(Paths.get(filePath), targetFilepath);
        Files.copy(Paths.get(pointFilePath), targetPointFilepath);
    }
    
    //**********************************************//
    //                 SQL ENGINE                   //
    //**********************************************//
    
    /**
     * Inner class for preparing and executing SQL statements
     */
    private class SQLEngine{
        
        // SQL statement for retrieiving root node
        public final String selectRootNodeStatement = "SELECT * FROM Node WHERE parentId IS NULL;";
        
        // ordered create table statements
        public final String[] orderedCreateTableStatements = {
            "CREATE TABLE IF NOT EXISTS Node (nodeId INTEGER PRIMARY KEY, fileIndex INTEGER, numPoints INTEGER, mzMin DOUBLE NOT NULL, mzMax DOUBLE NOT NULL, rtMin FLOAT NOT NULL, rtMax FLOAT NOT NULL, intMin DOUBLE, intMax DOUBLE, parentId INTEGER, FOREIGN KEY(parentId) REFERENCES Node(nodeId));",
            "CREATE INDEX IF NOT EXISTS Node_parentId ON Node (parentId);",
            "CREATE TABLE IF NOT EXISTS Trace (traceId INTEGER PRIMARY KEY, envelopeID INTEGER);",
            "CREATE TABLE IF NOT EXISTS NodePoints (nodeId INTEGER NOT NULL, pointId INTEGER NOT NULL, FOREIGN KEY(nodeId) REFERENCES Node(nodeId));",
            "CREATE INDEX IF NOT EXISTS NodePoints_nodeId ON NodePoints (nodeId);"
        };
        
        // insert statements 
        private final PreparedStatement insertNodeStatement; 
        private final PreparedStatement insertTraceStatement; 
        private final PreparedStatement insertNodePointStatement;
        
        // select statements
        private final String selectPointIDsByNodeSQL = "SELECT pointId FROM NodePoints WHERE nodeId=?;";
        private final String selectNodeByParentSQL = "SELECT * FROM Node WHERE parentId=?;";
        private final String selectNodeByIdSQL = "SELECT * FROM Node WHERE nodeId=?;";
        public final String selectAllTracesSQL = "SELECT * FROM Trace;";
        
        // update statements
        private final PreparedStatement updateTraceStatement;
        
        // delete statements
        private final PreparedStatement deleteTraceStatement;
        
        
        /**
         * Default constructor
         * Ensures that tables exist within database and creates prepared statements
         */
        public SQLEngine() throws SQLException
        {
            // create tables if don't already exist
            Statement statement = dbConnection.createStatement();
            for(String createTableStatement : this.orderedCreateTableStatements)
                statement.execute(createTableStatement);
            dbConnection.commit();
          
            // init insert statements 
            this.insertNodeStatement = dbConnection.prepareStatement("INSERT INTO Node VALUES (?,?,?,?,?,?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
            this.insertTraceStatement = dbConnection.prepareStatement("INSERT INTO Trace VALUES (?,?);"); 
            this.insertNodePointStatement = dbConnection.prepareStatement("INSERT INTO NodePoints VALUES (?,?);"); 
            
            // init update statements 
            this.updateTraceStatement = dbConnection.prepareStatement("UPDATE Trace SET envelopeId=? WHERE traceId=?;");
            
            // init delete statements 
            this.deleteTraceStatement = dbConnection.prepareStatement("DELETE FROM Trace WHERE traceId=?;");
            
        }
        
        /**
         * Inserts an MzTreeNode into the database
         * @param node MzTreeNode to insert
         * @param parentNodeID node's parentNodeID, 0 signals null parentNodeID (root node only)
         * @return ID of the node in the database
         * @throws SQLException 
         */
        public int insert(MzTreeNode node, int parentNodeID) throws SQLException
        {
            // set values in prepared statement
            
            // set null for primary key, db autoincrements
            this.insertNodeStatement.setNull(1, Types.INTEGER);
            
            // file index
            if(node.fileIndex != null)
                this.insertNodeStatement.setLong(2, node.fileIndex);
            else
                this.insertNodeStatement.setNull(2,Types.BIGINT);
            
            // num points in file
            if(node.numSavedPoints != null)
                this.insertNodeStatement.setInt(3, node.numSavedPoints);
            else
                this.insertNodeStatement.setNull(3, Types.INTEGER);
            
            // mz, rt and intensity bounds
            this.insertNodeStatement.setDouble(4, node.mzMin);
            this.insertNodeStatement.setDouble(5, node.mzMax);
            this.insertNodeStatement.setDouble(6, node.rtMin);
            this.insertNodeStatement.setDouble(7, node.rtMax);
            this.insertNodeStatement.setDouble(8, node.intMin);
            this.insertNodeStatement.setDouble(9, node.intMax);

            // set parent node ID, 0 implies null parent node ID
            if (parentNodeID != 0)
                this.insertNodeStatement.setInt(10, parentNodeID);
            else
                this.insertNodeStatement.setNull(10, Types.SMALLINT);

            // execute insert
            this.insertNodeStatement.executeUpdate();

            // retrieve generated key and return
            ResultSet results = this.insertNodeStatement.getGeneratedKeys();
            results.next();
            return results.getInt(1);
        }

        
        /**
         * Creates a trace entity in the database
         * @param traceID ID of the trace entity (given by the client)
         * @param envelopeID trace's envelope ID (given by the client)
         *      0 implies null envelope
         * @throws SQLException 
         */
        public void insertTrace(short traceID, Short envelopeID) throws SQLException
        {
            // set traceID
            this.insertTraceStatement.setInt(1, traceID);
            
            // set envelopeID, 0 implies null envelopeID
            if(envelopeID != null)
                this.insertTraceStatement.setInt(2, envelopeID);
            else
                this.insertTraceStatement.setNull(2,Types.SMALLINT);
            
            // execute insert
            this.insertTraceStatement.executeUpdate();
        }
        
        /**
         * Inserts a nodepoint relationship entity into the database
         * @param nodeID ID of the node in the nodepoint relationship
         * @param pointID ID of the point in the nodepoint relationship
         * @throws SQLException 
         */
        public void insert(int nodeID, int pointID) throws SQLException
        {
            this.insertNodePointStatement.setInt(1, nodeID);
            this.insertNodePointStatement.setInt(2, pointID);
            this.insertNodePointStatement.executeUpdate();   
        }
        
        /**
         * Performs a query on the NODE table, querying for a single node by ID
         * or for a collection of nodes by parentIDs
         * @param id ID to place in the WHERE clause
         * @param byParent if true selects by the parentId column,
         *  else selects by the primary key nodeId
         * @return Node(s) retrieved by query
         */
        public ArrayList<MzTreeNode> selectNode(int id, boolean byParent) throws SQLException
        {
            ResultSet results;
            PreparedStatement selectNodePrepStatement;
            
            if(byParent)
            {
                selectNodePrepStatement = dbConnection.prepareStatement(this.selectNodeByParentSQL);
                selectNodePrepStatement.setInt(1, id);
                results = selectNodePrepStatement.executeQuery();
            }
            else
            {
                selectNodePrepStatement = dbConnection.prepareStatement(this.selectNodeByIdSQL);
                selectNodePrepStatement.setInt(1,id);
                results = selectNodePrepStatement.executeQuery();
            }
            
            // flush result set to list of child nodes
            ArrayList<MzTreeNode> nodes = new ArrayList<>();
            while(results.next())
            {
                // create new node and assign values
                MzTreeNode childNode = new MzTreeNode();
                HybridStorage.this.assignNodeValues(childNode,results);

                // collect node
                nodes.add(childNode);
            }
                
            // close prepared statement (also closes resultset)
            selectNodePrepStatement.close();
            
            return nodes;
        }
        
        /**
         * Queries for pointIDs of points belonging to the node specified by nodeID
         * @param nodeID node whose points' IDs are to be collected
         * @return List of IDs of points belonging to the specified node
         */
        public ArrayList<Integer> selectPointIDsByNode(int nodeID) throws SQLException
        {
            try(PreparedStatement selectPointIDsByNodeStatement = dbConnection.prepareStatement(this.selectPointIDsByNodeSQL))
            {
                // query for pointIDs by nodeID
                selectPointIDsByNodeStatement.setInt(1, nodeID);
                ResultSet results = selectPointIDsByNodeStatement.executeQuery();

                // transform resultset into list of pointIDs
                ArrayList<Integer> pointIDs = new ArrayList<>();
                while(results.next())
                    pointIDs.add(results.getInt(1));

                return pointIDs;
            }
        }
        
       
        /**
         * Loads the Trace table into a HashMap (traceID -> envelopeID) and returns the HashMap
         * @return Trace table as a traceID->envelopeID HashMap
         * @throws SQLException 
         */
        public Map<Short,Short> loadTraceMap() throws SQLException
        {
            try(PreparedStatement selectAllTracesPrepStatement = dbConnection.prepareStatement(this.selectAllTracesSQL))
            {
                
                Map<Short,Short> traceMap = new HashMap<>();

                // query for all traces
                ResultSet traces = selectAllTracesPrepStatement.executeQuery();

                // iterate through all results collecting trace entities into trace map
                while(traces.next())
                {
                    short traceID = (short)traces.getInt(1);
                    short envelopeID = (short)traces.getInt(2);
                    traceMap.put(traceID, envelopeID);
                }

                return traceMap;
            }   
        }
        
        /**
         * Updates a trace to have the given envelopeID (could be null)
         * @param traceID ID of the trace to be updated
         * @param envelopeID new envelopeID value (can be null)
         */
        public void updateTrace(short traceID, Short envelopeID) throws SQLException
        {            
            // set envelopeID value (null allowed)
            if(envelopeID == null)
                this.updateTraceStatement.setNull(1, Types.SMALLINT);
            else
                this.updateTraceStatement.setShort(1, envelopeID);
            
            // set traceID value
            this.updateTraceStatement.setShort(2, traceID);
            
            // execute update
            this.updateTraceStatement.executeUpdate();
        }
        
        /**
         * Deletes the specified trace from the database, first taking the time
         * update any points that reference the trace to have a null trace reference
         * @param traceID ID of the trace to delete
         * @throws SQLException 
         */
        public void deleteTrace(short traceID) throws SQLException, IOException
        {
            pointEngine.clearTrace(traceID);

            // now that references are cleared, delete the trace
            this.deleteTraceStatement.setShort(1,traceID);

            // delete that sucka
            this.deleteTraceStatement.executeUpdate();
        }
    }
    
    //**********************************************//
    //                POINT ENGINE                  //
    //**********************************************//
    
    // TODO: "synchronized" keyword is brute-force thread safety;
    // RandomAccessFile is not thread-safe and should be replaced by something else
    private class PointEngine {
        
        /* POINT FORMAT
         * NOTE: Java uses big-endian in RandomAccessFile and in ByteBuffer
         * NOTE: pointIDs are 1-indexed for some reason. Be aware of that
         *
         * MZ   : 8 [DOUBLE]
         * RT   : 4 [FLOAT]
         * INTEN: 8 [DOUBLE]
         * TRACE: 2 [SHORT]
         */
        
        private final RandomAccessFile pointFile;
        
        // location within a point entry where the trace is located
        private static final int TRACE_OFFSET = 8 + 4 + 8;
        
        // number of points in the file
        private int pointCount;
        
        /**
         * Creates or opens the point storage file
         * @throws IOException 
         */
        public PointEngine() throws IOException {
            pointFile = new RandomAccessFile(pointFilePath, "rw");
            this.pointCount = 0;
            
            if (pointFile.length() > 0) {
                // existing file
                pointCount = (int) (pointFile.length() / MsDataPoint.DISK_NUM_BYTES_PER_POINT) - 1;
            }
            
        }

        /* Converts a byte array to point data */
        private MsDataPoint pointFromBytes(int id, byte[] data) 
        {
            ByteBuffer buf = ByteBuffer.wrap(data);
            double mz = buf.getDouble();
            float rt = buf.getFloat();
            double intensity = buf.getDouble();
            short traceID = buf.getShort();
            
            MsDataPoint pt = new MsDataPoint(id, mz, rt, intensity);
            pt.traceID = traceID;
            return pt;
        }
        
        /* Converts point data to a byte array */
        private byte[] pointToBytes(MsDataPoint point) {
            ByteBuffer buf = ByteBuffer.allocate(MsDataPoint.DISK_NUM_BYTES_PER_POINT);
            buf.putDouble(point.mz);
            buf.putFloat(point.rt);
            buf.putDouble(point.intensity);
            buf.putShort(point.traceID);
            return buf.array();
        }
        
        /* Reserves space in the file for the necessary number of points */
        public synchronized void reserveSpace(int numPoints) throws IOException {
            pointFile.setLength((long)(numPoints+1) * (long)MsDataPoint.DISK_NUM_BYTES_PER_POINT);
        }
        
        /**
         * Inserts a MsDataPoint into the point file at the current point file pointer location
         * @param point MsDataPoint to insert into database
         * @throws SQLException 
         */
        public synchronized void insert(MsDataPoint point) throws IOException
        {
            // write the point to the point file
            byte[] data = pointToBytes(point);
            pointFile.write(data);
           
            // assign the point's ID
            point.pointID = this.pointCount;
            this.pointCount++;
        }
        
         /**
         * Selects a point entity from the database, returns as MsDataPoint object
         * @param pointID ID of point to select
         * @return MsDataPoint selected from database
         * @throws SQLException 
         */
        public synchronized MsDataPoint selectPoint(int pointID) throws IOException
        {
            // convert to long to avoid integer overflow
            long pointLocation = (long)pointID * (long)MsDataPoint.DISK_NUM_BYTES_PER_POINT;
            this.pointFile.seek(pointLocation);
            byte[] data = new byte[MsDataPoint.DISK_NUM_BYTES_PER_POINT];
            this.pointFile.read(data);
            return pointFromBytes(pointID, data);
        }
        
        /**
         * Queries for points specified in pointIDs
         * @param pointIDs IDs of points to select
         * @return MsDataPoints selected from storage
         * @throws SQLException 
         */
        public synchronized List<MsDataPoint> selectPoints(List<Integer> pointIDs) throws IOException
        {
            // remember where we were
            long filePosBefore = this.pointFile.getFilePointer();
            
            // return list
            ArrayList<MsDataPoint> points = new ArrayList<>(pointIDs.size());
            for (Integer id : pointIDs) {
                points.add(this.selectPoint(id));
            }
            
            // reset to position before file loads
            this.pointFile.seek(filePosBefore);
            
            return points;
        }
        
        /**
         * Selects a leaf node's points from the point file by loading its entire block of points.
         * Allows for accessing an entire leaf node's data points with only one file seek.
         * Additionally trims the resulting list according to the given bounds
         * @param leaf leaf node that will have data block loaded
         * @param mzmin lower mz bound
         * @param mzmax upper mz bound
         * @param rtmin lower rt bound
         * @param rtmax upper rt bound
         * @return list of MsDataPoints loaded from leaf node's data block, trimmed according to data bounds
         * @throws IOException 
         */
        public synchronized List<MsDataPoint> selectLeafPointsInBounds(MzTreeNode leaf, double mzmin, double mzmax, float rtmin, float rtmax) throws IOException
        {
            // results list
            List<MsDataPoint> results = new ArrayList<>();
            
            // start location of node in point file
            long nodeStartLocation = leaf.fileIndex;
            
            // seek to start of node in point file
            this.pointFile.seek(nodeStartLocation);
            
            // allocated space for the node block
            byte[] data = new byte[leaf.numSavedPoints * MsDataPoint.DISK_NUM_BYTES_PER_POINT];
            
            // read node block
            this.pointFile.read(data);
            
            // parse points from retrieved binary
            for(int i = 0; i < leaf.numSavedPoints; i++)
            {
                // current point's sub array
                byte[] pointData = Arrays.copyOfRange(data, i * MsDataPoint.DISK_NUM_BYTES_PER_POINT, (i+1) * MsDataPoint.DISK_NUM_BYTES_PER_POINT);
                
                // parse into MsDataPoint
                MsDataPoint point = this.pointFromBytes(leaf.pointIDs[i], pointData);
                
                //assert point.mz >= leaf.mzMin && point.mz <= leaf.mzMax && point.rt >= leaf.rtMin && point.rt <= leaf.rtMax : "A loaded point was outside of its node's bounds!!!";
                
                // include in result set if within bounds
                if(point.isInBounds(mzmin, mzmax, rtmin, rtmax))
                    results.add(point);
            }
            return results;
        }
        
        /**
         * Updates a point in the DB to have the given dbTraceID
         * @param pointID ID of the point to be updated
         * @param traceID new dbTraceID value. If 0, DB entry will have dbTraceID set to null
         */
        
        public synchronized void updatePointTrace(int pointID, short traceID) throws IOException
        {
            long pointLocation = pointID * MsDataPoint.DISK_NUM_BYTES_PER_POINT;
            
            pointFile.seek(pointLocation + TRACE_OFFSET);
            pointFile.writeShort(traceID);
        }

        /**
         * Deletes a trace by setting every reference to it to no-trace
         * @param traceID trace ID to erase
         * @throws IOException 
         */
        private synchronized void clearTrace(short traceID) throws IOException {
            for (int i = 1; i <= pointCount; i++) {
                pointFile.seek(i * MsDataPoint.DISK_NUM_BYTES_PER_POINT + TRACE_OFFSET);
                if (pointFile.readShort() == traceID) {
                    pointFile.writeShort(0);
                }
            }
        }
        
        /* Ensures changes have been saved to the underlying storage medium */
        private synchronized void flush() throws IOException {
            pointFile.getFD().sync();
        }

        private Long getCurrentFileLocation() throws IOException {
            return this.pointFile.getFilePointer();
        }

        private synchronized List<MsDataPoint> selectBlockPointsInBounds(Long blockStart, Long blockLength, double mzmin, double mzmax, float rtmin, float rtmax) throws IOException 
        {
            // results list
            List<MsDataPoint> results = new ArrayList<>();
            
            // seek to start of node in point file
            this.pointFile.seek(blockStart);
            
            // if blockLength longer than allowed byte array length
            if(blockLength > Integer.MAX_VALUE)
            {
                // resolve block's ending location
                long blockEnd = blockStart + blockLength;
                
                // cursors for 
                long currentSubBlockStart = blockStart;
                int currentSubBlockLength = Integer.MAX_VALUE;
                
                while(currentSubBlockStart < blockLength)
                {
                    // allocated space for the node block
                    byte[] data = new byte[currentSubBlockLength];

                    // read node block
                    this.pointFile.read(data);

                    // parse points from retrieved binary
                    for(int i = 0; i * MsDataPoint.DISK_NUM_BYTES_PER_POINT < currentSubBlockLength; i++)
                    {
                        // current point's sub array
                        byte[] pointData = Arrays.copyOfRange(data, i * MsDataPoint.DISK_NUM_BYTES_PER_POINT, (i+1) * MsDataPoint.DISK_NUM_BYTES_PER_POINT);

                        // parse into MsDataPoint
                        MsDataPoint point = this.pointFromBytes(0, pointData);

                        // include in result set if within bounds
                        if(point.isInBounds(mzmin, mzmax, rtmin, rtmax))
                            results.add(point);
                    }
                    
                    // move to next block start
                    currentSubBlockStart += currentSubBlockLength;

                    // next block length is minimum of MAX_INT, distance from next block start to end of overall block
                    long distanceToEnd = blockEnd - currentSubBlockStart;
                    currentSubBlockLength = Integer.MAX_VALUE < distanceToEnd ? Integer.MAX_VALUE : (int)distanceToEnd;
                }
            }
            
            // else proceed with integer block length
            else
            {
                // allocated space for the node block
                byte[] data = new byte[blockLength.intValue()];

                // read node block
                this.pointFile.read(data);
                
                // parse points from retrieved binary
                for(int i = 0; i * MsDataPoint.DISK_NUM_BYTES_PER_POINT < blockLength; i++)
                {
                    // current point's sub array
                    byte[] pointData = Arrays.copyOfRange(data, i * MsDataPoint.DISK_NUM_BYTES_PER_POINT, (i+1) * MsDataPoint.DISK_NUM_BYTES_PER_POINT);

                    // parse into MsDataPoint
                    MsDataPoint point = this.pointFromBytes(0, pointData);

                    // include in result set if within bounds
                    if(point.isInBounds(mzmin, mzmax, rtmin, rtmax))
                        results.add(point);
                }
            }
            
            return results;
        }
    }
}
