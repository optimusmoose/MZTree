/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.mzTree.storage;

import edu.msViz.mzTree.ImportState;
import edu.msViz.mzTree.MsDataPoint;
import edu.msViz.mzTree.MzTreeNode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

/**
 * StorageFacade implementation that utilized SQLite for persistance
 * @author Kyle
 */
public class HybridStorage implements StorageFacade
{
    private static final Logger LOGGER = Logger.getLogger(HybridStorage.class.getName());

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
            throw new IllegalArgumentException("filePath must be specified");
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
            Statement setupStatement = this.dbConnection.createStatement();
            setupStatement.execute("PRAGMA foreign_keys=ON;");
            setupStatement.close();
            
            // disable auto commit (enables user defined transactions)
            this.dbConnection.setAutoCommit(false);
            
            // construct SQL Engine and Point Engine
            this.dbEngine = new SQLEngine();
            this.pointEngine = new PointEngine(pointFilePath);
            
            // reserve space for the number of incoming points
            if(numPoints != null)
                this.pointEngine.reserveSpace(numPoints);
            
        }
        catch(Exception e)
        {
            LOGGER.log(Level.WARNING, "Unable to initialize database connection at " + this.filePath, e);
            
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
    public MzTreeNode loadRootNode() throws Exception {
        MzTreeNode rootNode = new MzTreeNode();

        // query for and retrieve root node
        try(ResultSet rootNodeResult = this.dbConnection.createStatement().executeQuery(this.dbEngine.selectRootNodeStatement)) {
            rootNodeResult.next();

            this.assignNodeValues(rootNode, rootNodeResult);
        }
        return rootNode;
    }

    @Override
    public List<MzTreeNode> loadChildNodes(MzTreeNode parent) throws Exception {
        return this.dbEngine.selectNode(parent.nodeID, true);
    }
    
    //**********************************************//
    //                  SAVE POINTS                 //
    //**********************************************//

    @Override
    public void savePoints(SavePointsTask task, ImportState importState) throws IOException
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

            importState.setWorkDone(this.workDone);
        }
    }

    @Override
    public Integer getPointCount() throws Exception {
        return this.pointEngine.pointCount;
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
    private void assignNodeValues(MzTreeNode node, ResultSet rs) throws SQLException {
        node.nodeID = rs.getInt(1);

        // the jdbc way to determine nulls. good stuff
        node.fileIndex = rs.getLong(2);
        if (rs.wasNull())
            node.fileIndex = null;

        node.numSavedPoints = rs.getInt(3);
        if (rs.wasNull())
            node.numSavedPoints = null;

        node.mzMin = rs.getDouble(4);
        node.mzMax = rs.getDouble(5);
        node.rtMin = rs.getFloat(6);
        node.rtMax = rs.getFloat(7);
        node.intMin = rs.getDouble(8);
        node.intMax = rs.getDouble(9);
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
    public void saveNodePoints(MzTreeNode node, ImportState importState) throws SQLException
    {
        // set the node points array in DB
        this.dbEngine.updateNodePoints(node.nodeID, node.pointIDs.stream().mapToInt(x -> x).toArray());
        
        // node points are not significant work done
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
    public void close()
    {
        try {
            this.flush();
            this.dbConnection.close();
            this.pointEngine.pointFile.close();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not cleanly close storage", e);
        } finally {
            this.dbConnection = null;
            this.pointEngine = null;
        }
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
        
        private static final int APPLICATION_ID = 223764262;
        private static final int USER_VERSION = 5;
        
        // SQL statement for retrieiving root node
        public final String selectRootNodeStatement = "SELECT nodeId, fileIndex, numPoints, mzMin, mzMax, rtMin, rtMax, intMin, intMax, parentId, points FROM Node WHERE parentId IS NULL;";
        
        // ordered create table statements
        public final String[] orderedCreateTableStatements = {
            "CREATE TABLE IF NOT EXISTS Node (nodeId INTEGER PRIMARY KEY, fileIndex INTEGER, numPoints INTEGER, mzMin DOUBLE NOT NULL, mzMax DOUBLE NOT NULL, rtMin FLOAT NOT NULL, rtMax FLOAT NOT NULL, intMin DOUBLE, intMax DOUBLE, parentId INTEGER, points BLOB, FOREIGN KEY(parentId) REFERENCES Node(nodeId));",
            "CREATE INDEX IF NOT EXISTS Node_parentId ON Node (parentId);",
        };
        
        // insert statements 
        private final PreparedStatement insertNodeStatement; 

        // select statements
        private final String selectPointIDsByNodeSQL = "SELECT points FROM Node WHERE nodeId=?;";
        private final String selectNodeByParentSQL = "SELECT nodeId, fileIndex, numPoints, mzMin, mzMax, rtMin, rtMax, intMin, intMax, parentId, points FROM Node WHERE parentId=?;";
        private final String selectNodeByIdSQL = "SELECT nodeId, fileIndex, numPoints, mzMin, mzMax, rtMin, rtMax, intMin, intMax, parentId, points FROM Node WHERE nodeId=?;";
        
        // update statements
        private final PreparedStatement updateNodePointsStatement;
        
        /**
         * Default constructor
         * Ensures that tables exist within database and creates prepared statements
         */
        public SQLEngine() throws Exception
        {
            int appId;

            // check the application ID
            try (Statement checkAppIdStatement = dbConnection.createStatement()) {
                ResultSet appIdResult = checkAppIdStatement.executeQuery("PRAGMA application_id;");
                appIdResult.next();
                appId = appIdResult.getInt(1);
            }

            if (appId == 0) {
                // appId == 0 means it's not an mzTree or it's empty

                try (PreparedStatement checkEmpty = dbConnection.prepareStatement("SELECT count(*) FROM sqlite_master;")) {
                    ResultSet ers = checkEmpty.executeQuery();
                    ers.next();
                    int tables = ers.getInt(1);
                    if (tables != 0) {
                        throw new Exception("Not an mzTree file");
                    }
                }

                LOGGER.log(Level.INFO, "Creating a new mzTree file, version " + USER_VERSION);

                // initializing a new database with the current version
                try (Statement updateAppIdStatement = dbConnection.createStatement()) {
                    updateAppIdStatement.execute("PRAGMA application_id = " + APPLICATION_ID + ";");
                    updateAppIdStatement.execute("PRAGMA user_version = " + USER_VERSION + ";");
                }

                Statement statement = dbConnection.createStatement();
                for(String createTableStatement : this.orderedCreateTableStatements)
                    statement.execute(createTableStatement);
                statement.close();
                dbConnection.commit();

            } else if (appId != APPLICATION_ID) {
                throw new SQLException("Not an mzTree file.");
            }

            int userVersion;
            // check the user version for upgrades
            try (Statement userVersionStatement = dbConnection.createStatement()) {
                ResultSet userVersionResult = userVersionStatement.executeQuery("PRAGMA user_version;");
                userVersionResult.next();
                userVersion = userVersionResult.getInt(1);
            }

            // process version upgrades
            if (userVersion != USER_VERSION)
            {
                LOGGER.log(Level.INFO, "Converting mzTree file from version " + userVersion);

                // use switch fall-through (no "break" statement) to run multiple migrations
                // commented-out examples below
                switch(userVersion) {
                    case 5:
                        //convert_v5_v6();
                    case 6:
                        //convert_v6_v7();
                        break;
                    default:
                        throw new SQLException("Unsupported mzTree file version.");
                }

                try(Statement updateUserVersionStatement = dbConnection.createStatement()) {
                    updateUserVersionStatement.execute("PRAGMA user_version = " + USER_VERSION + ";");
                }

                LOGGER.log(Level.INFO, "mzTree file converted to version " + USER_VERSION);
            }
          
            // init insert statements 
            this.insertNodeStatement = dbConnection.prepareStatement("INSERT INTO Node (nodeId, fileIndex, numPoints, mzMin, mzMax, rtMin, rtMax, intMin, intMax, parentId, points) VALUES (?,?,?,?,?,?,?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS);

            // init update statements
            this.updateNodePointsStatement = dbConnection.prepareStatement("UPDATE Node SET points=? WHERE nodeId=?");
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
            
            this.insertNodeStatement.setNull(11, Types.BLOB);

            // execute insert
            this.insertNodeStatement.executeUpdate();

            // retrieve generated key and return
            ResultSet results = this.insertNodeStatement.getGeneratedKeys();
            results.next();
            return results.getInt(1);
        }

        /**
         * Inserts a nodepoint relationship entity into the database
         * @param nodeID ID of the node in the nodepoint relationship
         * @param pointID ID of the point in the nodepoint relationship
         * @throws SQLException
         */
        public void updateNodePoints(int nodeID, int[] pointIDs) throws SQLException
        {
            // convert pointIDs into a byte array
            ByteBuffer bytes = ByteBuffer.allocate(pointIDs.length * 4).order(ByteOrder.BIG_ENDIAN);
            IntBuffer wrapper = bytes.asIntBuffer();
            wrapper.put(pointIDs);
            bytes.rewind();
            
            // update the Node.points field in the database
            this.updateNodePointsStatement.setBytes(1, bytes.array());
            this.updateNodePointsStatement.setInt(2, nodeID);
            this.updateNodePointsStatement.executeUpdate();
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
                if(results.next()) {
                    // convert the byte array to an integer array
                    byte[] bytes = results.getBytes(1);
                    int[] ints = new int[bytes.length / 4];
                    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer().get(ints);
                    
                    // add the IDs to the result list
                    pointIDs.ensureCapacity(ints.length);
                    for (int i = 0; i < ints.length; i++) {
                        pointIDs.add(ints[i]);
                    }
                }

                return pointIDs;
            }
        }
    }

    //**********************************************//
    //                POINT ENGINE                  //
    //**********************************************//
    private static class PointEngine {
        
        /* POINT FORMAT
         * NOTE: Java uses big-endian in RandomAccessFile and in ByteBuffer
         *
         * MZ   : 8 [DOUBLE]
         * RT   : 4 [FLOAT]
         * INTEN: 8 [DOUBLE]
         * META1: 4 [INTEGER]
         */
        
        private final RandomAccessFile pointFile;

        // number of points in the file
        private int pointCount;
        
        /**
         * Creates or opens the point storage file
         * @throws IOException 
         */
        public PointEngine(String pointFilePath) throws IOException {
            pointFile = new RandomAccessFile(pointFilePath, "rw");
            this.pointCount = 0;
            
            if (pointFile.length() > 0) {
                // existing file
                pointCount = (int) (pointFile.length() / MsDataPoint.DISK_NUM_BYTES_PER_POINT);
            }
            
        }

        /* Converts a byte array to point data */
        private MsDataPoint pointFromBytes(int id, byte[] data) 
        {
            ByteBuffer buf = ByteBuffer.wrap(data);
            double mz = buf.getDouble();
            float rt = buf.getFloat();
            double intensity = buf.getDouble();
            int meta1 = buf.getInt();
            
            MsDataPoint pt = new MsDataPoint(id, mz, rt, intensity);
            pt.meta1 = meta1;
            return pt;
        }
        
        /* Converts point data to a byte array */
        private byte[] pointToBytes(MsDataPoint point) {
            ByteBuffer buf = ByteBuffer.allocate(MsDataPoint.DISK_NUM_BYTES_PER_POINT);
            buf.putDouble(point.mz);
            buf.putFloat(point.rt);
            buf.putDouble(point.intensity);
            buf.putInt(point.meta1);
            return buf.array();
        }
        
        /* Reserves space in the file for the necessary number of points */
        public synchronized void reserveSpace(int numPoints) throws IOException {
            pointFile.setLength((long)(numPoints) * (long)MsDataPoint.DISK_NUM_BYTES_PER_POINT);
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
            if (pointID < 0) {
                throw new IndexOutOfBoundsException("pointID");
            }

            if (pointID >= this.pointCount) {
                throw new IndexOutOfBoundsException("pointID");
            }

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
            
            // reset to position of cursor before load
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

            if (leaf.fileIndex == null) {
                // not a leaf node, or upgraded from a version without this optimization
                return selectPoints(leaf.pointIDs)
                        .stream().filter(p -> p.isInBounds(mzmin, mzmax, rtmin, rtmax)).collect(Collectors.toList());
            }

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
                MsDataPoint point = this.pointFromBytes(leaf.pointIDs.get(i), pointData);
                
                //assert point.mz >= leaf.mzMin && point.mz <= leaf.mzMax && point.rt >= leaf.rtMin && point.rt <= leaf.rtMax : "A loaded point was outside of its node's bounds!!!";
                
                // include in result set if within bounds
                if(point.isInBounds(mzmin, mzmax, rtmin, rtmax))
                    results.add(point);
            }
            return results;
        }

        /* Ensures changes have been saved to the underlying storage medium */
        private synchronized void flush() throws IOException {
            pointFile.getFD().sync();
        }

        private synchronized long getCurrentFileLocation() throws IOException {
            return this.pointFile.getFilePointer();
        }
    }
}
