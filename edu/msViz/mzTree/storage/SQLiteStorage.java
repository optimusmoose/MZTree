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
import edu.msViz.mzTree.PointCache;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * StorageFacade implementation that utilized SQLite for persistance
 * @author Kyle
 */
public class SQLiteStorage implements StorageFacade
{
    // connection to the SQLite database
    private Connection dbConnection;
    
    // SQL statement preparation and execution
    private SQLEngine dbEngine;
    
    // path to the database file
    private String filePath;
    
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
            
            // construct SQL Engine
            this.dbEngine = new SQLEngine();
            
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
    
    /**
     * Recursively inserts all nodes and datapoints in a tree in a sqlite database
     * @param node node to save
     * @param parentNode node's parent node (possibly null) for inserting parentNodeId
     * @param pointCache hashmap containing all points
     * @throws SQLException 
     */
    private void recursiveSave(MzTreeNode node, MzTreeNode parentNode, PointCache pointCache) throws SQLException
    {
        /**
         * So that DB foreign key relationships aren't violated perform saving
         * with the following recursive scheme:
         *  1) insert node entity
         *  2) recurse on all children
         *  3) (leaf nodes only) insert points
         *  4) insert nodepoints (node/point join entities)
         *      - interleaved with inserting points
         */
        
        // first insert the node into the database
        if(parentNode != null)
            node.nodeID = dbEngine.insert(node,parentNode.nodeID);
        else
            node.nodeID = dbEngine.insert(node,0);
        
        // recurse on all children
        for(MzTreeNode childNode : node.children)
            this.recursiveSave(childNode, node, pointCache);
        
        // leaf nodes save their data points to the database
        boolean isLeaf = node.children.isEmpty();
        
        // iterate through all pointIDs in node
        for(int pointID : node.pointIDs)
        {
            // fetch the datapoint from the point cache
            MsDataPoint dataPoint = pointCache.get(pointID);
            
            // leaves insert point into database
            if(isLeaf)    
                dbEngine.insert(dataPoint);
            
            // all points insert nodepoint relationship entity
            dbEngine.insert(node.nodeID, dataPoint.pointID);
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
        
        // if true then node will cache points
        boolean shouldAddToCache = curDepth <= 1;
        
        // if true then node will collect min/max intensities
        boolean isLeaf;
        
        // query database for all child nodes
        List<MzTreeNode> childNodes = this.dbEngine.selectNode(node.nodeID, true);
        
        // isLeaf == has children
        isLeaf = childNodes.isEmpty();
        
        // leaf nodes update tree height (results in largest height)
        if(isLeaf)
            mzTree.treeHeight = (curDepth > mzTree.treeHeight) ? (short)curDepth : mzTree.treeHeight;
        
        // nodes that should cache need entire points
        if(shouldAddToCache)
        {
            // query db for points belonging to current mpde
            List<MsDataPoint> points = this.dbEngine.selectPointsByNode(node.nodeID);
            
            // collect points and IDs from result set
            for(MsDataPoint point : points){
                mzTree.pointCache.put(point);
            }
            
            // assign pointIDs to node
            node.pointIDs = points.stream().mapToInt(x -> x.pointID).toArray();
        }
        
        // recurse on each child node if there are any
        for(MzTreeNode childNode : childNodes)
        {
            // recursive call at +1 depth
            this.recursiveTreeBuilder(childNode, mzTree, curDepth+1);

            // add reference to child and keep min/max mz/rt/int
            node.addChildGetBounds(childNode);
        }
    }
    
    @Override
    public List<Integer> getNodePointIDs(int nodeID) throws Exception {
        return this.dbEngine.selectPointIDsByNode(nodeID);
    }
    
    /**
     * Reads entry from points result set and converts to MsDataPoint
     * Expects result set to have cursor in correct position
     * @param rs points result set
     * @return MsDataPoint constructed by result set entry
     * @throws SQLException 
     */
    private MsDataPoint resultSetToMsDataPoint(ResultSet rs) throws SQLException
    {
        MsDataPoint dataPoint = new MsDataPoint(rs.getInt(1), rs.getDouble(2), rs.getFloat(3), rs.getDouble(4));
        dataPoint.traceID = rs.getShort(5);
        return dataPoint;
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
        node.mzMin = rs.getDouble(2);
        node.mzMax = rs.getDouble(3);
        node.rtMin = rs.getFloat(4);
        node.rtMax = rs.getFloat(5);
        node.intMin = rs.getDouble(6);
        node.intMax = rs.getDouble(7);
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
    public void updateTraces(short traceID, Integer[] targets) throws SQLException
    {
        // iterate through all pointID/traceID pairs
        for(int i = 0; i < targets.length; i++)
        {
            // extract pointID
            int pointID = targets[i];
            
            // update point's trace
            this.dbEngine.updatePointTrace(pointID, traceID);
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
    public void deleteTrace(short traceID) throws SQLException
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
        for(int i = 0; i < targets.length; i++)
        {
            // get traceID and envelopeID from tracesToEnvelope
            short traceID = targets[i].shortValue();
            
            // insert trace if doesn't exist, update if does exist
            this.dbEngine.updateTrace(traceID, envelopeID);
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
    //                  SAVE POINTS                 //
    //**********************************************//
    
    @Override
    public void savePoints(SavePointsTask saveTask, ImportMonitor importMonitor) throws SQLException
    {
        // iterate through all points
        for(MsDataPoint point : saveTask.dataset)
        {
            // insert point into database
            this.dbEngine.insert(point);
            
            // a point is a single unit of work
            this.workDone++;
            
            // if update interval reached then send update to importMonitor
            if(this.workDone % importMonitor.updateInterval == 0)
                importMonitor.update(null, this.workDone);
        }
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
    public List<MsDataPoint> loadPoints(List<Integer> pointIDs) throws SQLException
    {
        long start = System.currentTimeMillis();
        
        List<MsDataPoint> points = this.dbEngine.selectPoints(pointIDs);
        
        System.out.println("Loaded " + pointIDs.size() + " from db in " + (System.currentTimeMillis() - start));
        
        return points;
    }
    
    @Override
    public List<MsDataPoint> loadLeavesPointsInBoundsClumped(List<MzTreeNode> leaves, double mzmin, double mzmax, float rtmin, float rtmax) 
    {
        throw new UnsupportedOperationException("TODO implement the leaf load function for the SQLite SF implementation");
    }
    
    
    //**********************************************//
    //                    FLUSH                     //
    //**********************************************//
    
    @Override
    public void flush() throws SQLException
    {
        this.dbConnection.commit();
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
    }

    @Override
    public void copy(Path targetFilepath) throws Exception {
        Files.copy(Paths.get(filePath), targetFilepath);
    }

    @Override
    public List<MsDataPoint> loadLeavesPointsInBounds(List<MzTreeNode> leaves, double mzmin, double mzmax, float rtmin, float rtmax) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
            "CREATE TABLE IF NOT EXISTS Node (nodeId INTEGER PRIMARY KEY, mzMin DOUBLE NOT NULL, mzMax DOUBLE NOT NULL, rtMin FLOAT NOT NULL, rtMax FLOAT NOT NULL, intMin DOUBLE, intMax DOUBLE, parentId INTEGER, FOREIGN KEY(parentId) REFERENCES Node(nodeId));",
            "CREATE INDEX IF NOT EXISTS Node_parentId ON Node (parentId);",
            "CREATE TABLE IF NOT EXISTS Trace (traceId INTEGER PRIMARY KEY, envelopeID INTEGER);",
            "CREATE TABLE IF NOT EXISTS Point (pointId INTEGER PRIMARY KEY, mz DOUBLE NOT NULL, rt FLOAT NOT NULL, intensity DOUBLE NOT NULL, traceId INTEGER, FOREIGN KEY(traceId) REFERENCES Trace(traceId));",
            "CREATE TABLE IF NOT EXISTS NodePoints (nodeId INTEGER NOT NULL, pointId INTEGER NOT NULL, FOREIGN KEY(nodeId) REFERENCES Node(nodeId), FOREIGN KEY(pointId) REFERENCES Point(pointId));",
            "CREATE INDEX IF NOT EXISTS NodePoints_nodeId ON NodePoints (nodeId);"
        };
        
        // insert statements 
        private final PreparedStatement insertNodeStatement; 
        private final PreparedStatement insertPointStatement; 
        private final PreparedStatement insertTraceStatement; 
        private final PreparedStatement insertNodePointStatement;
        
        // select statements
        private final String selectPointsByNodeSQL = "SELECT Point.* FROM NodePoints NATURAL JOIN Point WHERE nodeId=?";
        private final String selectPointIDsByNodeSQL = "SELECT pointId FROM NodePoints WHERE nodeId=?;";
        private final String selectPointIDsByTraceSQL = "SELECT pointId FROM Point WHERE traceId=?;";
        private final String selectPointSQL = "SELECT * FROM Point WHERE pointId=?;";
        private final String selectNodeByParentSQL = "SELECT * FROM Node WHERE parentId=?;";
        private final String selectNodeByIdSQL = "SELECT * FROM Node WHERE nodeId=?;";
        public final String selectAllTracesSQL = "SELECT * FROM Trace;";
        
        // update statements 
        private final PreparedStatement updatePointTraceStatement; 
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
            this.insertNodeStatement = dbConnection.prepareStatement("INSERT INTO Node VALUES (?,?,?,?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS); 
            this.insertPointStatement = dbConnection.prepareStatement("INSERT INTO Point VALUES (?,?,?,?,?);"); 
            this.insertTraceStatement = dbConnection.prepareStatement("INSERT INTO Trace VALUES (?,?);"); 
            this.insertNodePointStatement = dbConnection.prepareStatement("INSERT INTO NodePoints VALUES (?,?);"); 
            
            // init update statements 
            this.updatePointTraceStatement = dbConnection.prepareStatement("UPDATE Point SET traceId=? WHERE PointId=?;"); 
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
            this.insertNodeStatement.setNull(1, Types.INTEGER);
            this.insertNodeStatement.setDouble(2, node.mzMin);
            this.insertNodeStatement.setDouble(3, node.mzMax);
            this.insertNodeStatement.setDouble(4, node.rtMin);
            this.insertNodeStatement.setDouble(5, node.rtMax);
            this.insertNodeStatement.setDouble(6, node.intMin);
            this.insertNodeStatement.setDouble(7, node.intMax);

            // set parent node ID, 0 implies null parent node ID
            if (parentNodeID != 0)
                this.insertNodeStatement.setInt(8, parentNodeID);
            else
                this.insertNodeStatement.setNull(8, Types.SMALLINT);

            // execute insert
            this.insertNodeStatement.executeUpdate();

            // retrieve generated key and return
            ResultSet results = this.insertNodeStatement.getGeneratedKeys();
            results.next();
            return results.getInt(1);
            
            
        }
        
        /**
         * Inserts a MsDataPoint into the database
         * @param point MsDataPoint to insert into database
         * @throws SQLException 
         */
        public void insert(MsDataPoint point) throws SQLException
        {   
                
            // set prepared statement values
            this.insertPointStatement.setInt(1, point.pointID);
            this.insertPointStatement.setDouble(2, point.mz);
            this.insertPointStatement.setFloat(3, point.rt);
            this.insertPointStatement.setDouble(4, point.intensity);

            // set traceID, 0 implies null traceID
            if (point.traceID != 0)
                this.insertPointStatement.setInt(5,point.traceID);
            else
                this.insertPointStatement.setNull(5, Types.INTEGER);

            // execute insert
            this.insertPointStatement.executeUpdate();

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
        public List<MzTreeNode> selectNode(int id, boolean byParent) throws SQLException
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
                SQLiteStorage.this.assignNodeValues(childNode,results);

                // collect node
                nodes.add(childNode);
            }
                
            // close prepared statement (also closes resultset)
            selectNodePrepStatement.close();
            
            return nodes;
        }
        
        /**
         * Queries for MsDataPoints belonging to a node specified by nodeID
         * @param nodeID node whose points are to be collected
         * @return List of MsDataPoints retrieved by query
         */
        public List<MsDataPoint> selectPointsByNode(int nodeID) throws SQLException
        {
            try(PreparedStatement selectPointsByNodePrepStatement = dbConnection.prepareStatement(this.selectPointsByNodeSQL))
            {
                // execute points by node query
                selectPointsByNodePrepStatement.setInt(1, nodeID);
                ResultSet pointResults = selectPointsByNodePrepStatement.executeQuery();

                // transform resultset into list of MsDataPoint
                ArrayList<MsDataPoint> points = new ArrayList<>();
                while(pointResults.next())
                    points.add(SQLiteStorage.this.resultSetToMsDataPoint(pointResults));

                return points;
            }
            
        }
        
        /**
         * Queries for pointIDs of points belonging to the node specified by nodeID
         * @param nodeID node whose points' IDs are to be collected
         * @return List of IDs of points belonging to the specified node
         */
        public List<Integer> selectPointIDsByNode(int nodeID) throws SQLException
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
         * Selects a point entity from the database, returns as MsDataPoint object
         * @param pointID ID of point to select
         * @return MsDataPoint selected from database
         * @throws SQLException 
         */
        public MsDataPoint selectPoint(int pointID) throws SQLException
        {
            try(PreparedStatement selectPointPrepStatement = dbConnection.prepareStatement(this.selectPointSQL))
            {
                // query for point by pointID
                selectPointPrepStatement.setInt(1,pointID);
                ResultSet pointResults = selectPointPrepStatement.executeQuery();

                // thanks to primary key constraints there will only be one result
                pointResults.next();

                // convert that one result to MsDataPoint
                MsDataPoint thePoint = new MsDataPoint(pointID, pointResults.getDouble(2), pointResults.getFloat(3), pointResults.getDouble(4));
                thePoint.traceID = pointResults.getShort(5);

                return thePoint;
            }
            
        }
        
        /**
         * Queries for points specified in pointIDs
         * @param pointIDs IDs of points to select
         * @return MsDataPoints selected from storage
         * @throws SQLException 
         */
        public List<MsDataPoint> selectPoints(List<Integer> pointIDs) throws SQLException
        {
            // return list
            ArrayList<MsDataPoint> points = new ArrayList<>();
            
            try (PreparedStatement selectStatement = dbConnection.prepareStatement(this.selectPointSQL)) {
                for (Integer id : pointIDs) {
                    selectStatement.setInt(1, id);

                    // execute query
                    ResultSet pointsResults = selectStatement.executeQuery();

                    // transform resultset into list of MsDataPoints
                    while(pointsResults.next())
                        points.add(resultSetToMsDataPoint(pointsResults));
                }
                return points;
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
         * Updates a point in the DB to have the given dbTraceID
         * @param pointID ID of the point to be updated
         * @param traceID new dbTraceID value. If 0, DB entry will have dbTraceID set to null
         */
        public void updatePointTrace(int pointID, short traceID) throws SQLException
        {
            // set traceID value, 0 implies null traceID
            if(traceID == 0)
                this.updatePointTraceStatement.setNull(1, Types.SMALLINT);
            else
                this.updatePointTraceStatement.setShort(1, traceID);
            
            // set pointID value
            this.updatePointTraceStatement.setInt(2,pointID);
            
            // execute update
            this.updatePointTraceStatement.executeUpdate();
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
        }
        
        /**
         * Deletes the specified trace from the database, first taking the time
         * update any points that reference the trace to have a null trace reference
         * @param traceID ID of the trace to delete
         * @throws SQLException 
         */
        public void deleteTrace(short traceID) throws SQLException
        {
            try(PreparedStatement selectPointIDsByTracePrepStatement = dbConnection.prepareStatement(this.selectPointIDsByTraceSQL))
            {
                // get points that reference the to-be-deleted trace
                selectPointIDsByTracePrepStatement.setInt(1, traceID);
                ResultSet pointIDResults = selectPointIDsByTracePrepStatement.executeQuery();

                // update each of the retrieved points to have a null traceId
                while(pointIDResults.next())
                    this.updatePointTrace(pointIDResults.getInt(1),(short)0); // 0 translates to null

                // now that references are cleared, delete the trace
                this.deleteTraceStatement.setShort(1,traceID);

                // delete that sucka
                this.deleteTraceStatement.executeUpdate();
            }
        }
    }
}
