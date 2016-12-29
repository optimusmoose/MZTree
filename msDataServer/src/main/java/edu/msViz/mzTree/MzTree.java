/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.mzTree;

import com.opencsv.CSVReader;
import edu.msViz.msHttpApi.ImportMonitor;
import edu.msViz.msHttpApi.ImportMonitor.ImportStatus;
import edu.msViz.mzTree.storage.StorageFacade;
import edu.msViz.mzTree.storage.StorageFacadeFactory;
import edu.msViz.mzTree.summarization.SummarizationStrategy;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory.Strategy;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import javax.xml.stream.XMLStreamException;
import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * R-Tree implementation for storing and accessing MS data
 * @author rob
 */
public final class MzTree
{
    // there is really no good reason for this...
    public static final int DEFAULT_TREE_HEIGHT = 4;
    
    // number of points per node should assume an whole number of disk blocks 
    // some disks ship with blocks of size 4096B, some with blocks of 512B. luckily 512 is a multiple of 4096
    // an MsDataPoint requires 22B, so we need the LCM of 4096 and 22, which is 45056
    // 45056B is 11 4096B disk blocks, 88 512B disk blocks, and 2048 points.
    public static final int NUM_POINTS_PER_NODE = 45056;

    // minimum branching factor for a partitioned load
    // ensures that we don't have a tiny branching factor that produces
    // a very tall tree (such as 2...)
    private static final int MINIMUM_BRANCHING_FACTOR = 4;
    
    // fraction of heap alloted for MsDataPoints
    private static final float HEAP_FRACTION = .3f;
    
    // the branching factor of the tree (number of children per root/hidden node)
    public int branchingFactor;

    // the height of the tree
    public short treeHeight;
    
    // the tree's head node
    public MzTreeNode head;
    
    // the summarizer used to form the summarized collections of data
    // at the root/intermediate nodes
    public SummarizationStrategy summarizer;
    
    // point map, keyed by pointID for unified point storage
    public PointCache pointCache;
    
    // mapping of traceIDs to envelopeIDs
    public Map<Short,Short> traceMap;
    
    // disk storage implementation
    public StorageFacade dataStorage;
    
    // static storage interface choice
    private static final StorageFacadeFactory.Facades STORAGE_INTERFACE_CHOICE = StorageFacadeFactory.Facades.Hybrid; 

    // timing variables
    private long overallTreeBuildTime;
    
    // import progress monitor
    private final ImportMonitor importMonitor;
        
    /**
     * No argument constructor for basic initialization
     * @param importMonitor import progress monitor
     */
    public MzTree(ImportMonitor importMonitor) {
        this.importMonitor = importMonitor;
        this.pointCache = new PointCache(null, false);
        this.traceMap = new HashMap<>();
    }

    //***********************************************//
    //                     LOAD                      //
    //***********************************************//
    
    /**
     * 
     * Loads an mzTree either by building from mzML or by reconnecting to an mzTree
     * @param filePath The path to the mzML or mzTree file
     * @param summarizationStrategy strategy to be used for summarization of points
     * @param isPartitionedLoad flag signaling the user wishes to perform a memory-conservative load
     * @throws Exception 
     */
    public void load(String filePath, Strategy summarizationStrategy, boolean isPartitionedLoad) throws Exception 
    {
        // construct the summarizer corresponding to user's choice
        this.summarizer = SummarizationStrategyFactory.create(summarizationStrategy);        
        
        // inform monitor of source file location
        importMonitor.update(null, filePath);
        
        try 
        {
            long start = System.currentTimeMillis();
            
            // csv load
            if(filePath.endsWith(".csv"))
            {
                this.csvLoad(filePath);
            }
            
            // mzml load
            else
            {
                // initialize mzmlParser
                MzmlParser mzmlParser = new MzmlParser(filePath);
                
                // if the user specified a memory-conservative load
                // and the partitioned load is necessary then perform a partitioned load
                if(isPartitionedLoad && this.partitionedLoadConfiguration(mzmlParser))
                {
                    
                    this.partitionedLoad(mzmlParser);
                }

                // else perform standard, memory-apathetic load
                else
                {
                    // initialize mzmlParser
                    this.standardLoad(mzmlParser);
                }
            }

            importMonitor.update(null, ImportStatus.FINISHED);
            
            this.overallTreeBuildTime = System.currentTimeMillis() - start;
            this.displayExecutionTimes();
            
        } 
        catch (DataFormatException | XMLStreamException ex) 
        {
            // try as an mzTree file instead of XML+build in the 
            // occurence of DataFormatException or XMLStreamException
                        
            this.pointCache = new PointCache(null, false);
            this.traceMap = new HashMap<>();

            // initialize data storage on mzTree file
            this.initDataStorage(STORAGE_INTERFACE_CHOICE, filePath, null);

            // inform the importMonitor of an .mzTree load
            this.importMonitor.update(null, ImportStatus.LOADING_MZTREE);
            
            // create mzTree from existing file
            dataStorage.loadModel(this);

            // inform importMonitor that mzTree load has finished
            this.importMonitor.update(null, ImportStatus.FINISHED);
        }
    }

    /**
     * Performs a partitioned load of the data set, resulting in conservative memory consumption
     * @param mzmlParser input mzml file parser
     * @param importMonitor import progress monitor
     * @throws XMLStreamException
     * @throws DataFormatException
     * @throws IOException 
     */
    private void partitionedLoad(MzmlParser mzmlParser) throws XMLStreamException, DataFormatException, IOException
    {
        
        System.out.println("Partitioned load w/ " + this.branchingFactor + " partitions");
        
        // signal to the import monitor that tree building has begun
        this.importMonitor.update(null, ImportStatus.BUILDING);
        
        // init head node
        this.head = new MzTreeNode(this.branchingFactor);
        
        // iterate through each level 1 node, loading partition and
        // constructing separately
        for(int i = 0; i < this.branchingFactor; i++)
        {            
            // current level 1 node
            MzTreeNode curL1Node = new MzTreeNode(this.branchingFactor);
            
            // load level 1 node's partition
            List<MsDataPoint> curPartition = mzmlParser.readPartition();
            
            // recursively construct level 1 node
            this.divide(true, curPartition, curL1Node , 1);
            
            // add the level 1 node to the root node
            this.head.addChildGetBounds(curL1Node);
            
            System.out.println("Completed partition " + i);
        }
        
        // root node summarization!!!!!!
        this.head.summarizeFromChildren(MzTree.NUM_POINTS_PER_NODE, this.summarizer, this.dataStorage);
        
        // recursively save node information (only points are saved during construction)
        this.recursiveNodeSave(this.head, 0);
        
        try {
            // commit all entries
            this.dataStorage.flush();
        } catch (Exception ex) {
            System.err.println("Could not flush entries to storage || " + ex.getMessage());
            ex.printStackTrace();
            return;
        }   
    }
    
    /**
     * Performs a standard, memory-apathetic load
     * @param mzmlParser input file parser initialized w/ target file
     * @param importMonitor import progress monitor
     * @throws IOException
     * @throws XMLStreamException
     * @throws DataFormatException 
     */
    private void standardLoad(MzmlParser mzmlParser) throws IOException, XMLStreamException, DataFormatException, Exception
    {   
        importMonitor.update(null, ImportStatus.PARSING);

        List<MsDataPoint> dataset = mzmlParser.readAllData();

        this.buildTreeFromRoot(dataset);
    }
    
    /**
     * Loads MS data in csv format (mz, rt, intensity, traceID, envelopeID)
     * @param filePath path to csv file
     * @param importMonitor
     * @throws FileNotFoundException
     * @throws IOException
     * @throws Exception 
     */
    private void csvLoad(String filePath) throws FileNotFoundException, IOException, Exception
    {
        this.importMonitor.update(null, ImportStatus.PARSING);
        
        ArrayList<MsDataPoint> points = new ArrayList<>();
        
        // open csv reader on targetted csv file
        CSVReader reader = new CSVReader(new FileReader(filePath));
        
        // point ID iterator
        int id = 1;
        
        // first line might be a header
        String[] line = reader.readNext();
        
        // if the first line is not a header
        if(line != null && StringUtils.isNumeric(line[0]))
        {
            // convert to msdatapoint, collect
            MsDataPoint point = this.csvRowToMsDataPoint(line, id);
            points.add(point);
            id++;
            
            // add the trace if it doesn't already exist and isn't zero
            if(point.traceID != 0 && !this.traceMap.containsKey(point.traceID))
                this.insertTrace(point.traceID, Short.parseShort(line[4]));
        }
        
        // read the remaining lines (now guaranteed no header)
        while((line = reader.readNext()) != null)
        {
            // convert to msdatapoint, collect
            MsDataPoint point = this.csvRowToMsDataPoint(line, id);
            points.add(point);
            id++;
            
            // add the trace if it doesn't already exist
            if(point.traceID != 0 && !this.traceMap.containsKey(point.traceID))
                this.insertTrace(point.traceID, Short.parseShort(line[4]));
        }
        
        // build that tree!
        this.buildTreeFromRoot(points);
    }
    
    /**
     * Constructs an MzTree from the dataset, starting at the root node (so no partitioned load)
     * @param dataset
     * @param importMonitor 
     */
    private void buildTreeFromRoot(List<MsDataPoint> dataset) throws Exception
    {
        this.initDataStorage(STORAGE_INTERFACE_CHOICE, null, dataset.size());
        
        // **************** STEP 1: CONFIGURE TREE ****************

        // number of leafNodes = globalNumPoints / hdBlockTupleCapacity 
        int numLeafNodes = (int) Math.ceil((float) dataset.size() / (float) MzTree.NUM_POINTS_PER_NODE);

        this.treeHeight = MzTree.DEFAULT_TREE_HEIGHT; 

        // branching factor = leafnodes ^ (1/treeDepth) 
        this.branchingFactor = (int) Math.ceil(Math.pow(numLeafNodes, 1.0 / (double) this.treeHeight));

        // init head node
        this.head = new MzTreeNode(this.branchingFactor);

        // inform importMonitor of anticipated amount of work
        int numPointsToSave = dataset.size(); // save points: dataset.length
        int numNodePointsToSave = this.calculateNodePoints(numPointsToSave);
        this.importMonitor.update(null, (float) (numPointsToSave + numNodePointsToSave));
        
        // **************** STEP 2: BUILD ****************
        
        this.importMonitor.update(null, ImportStatus.BUILDING);
        
        // divide the head node, do not sort at start (null), mzML data already sorted by RT
        this.divide(null, dataset, this.head, 0);

        // recursively save node information (only points are saved during construction)
        this.recursiveNodeSave(this.head, 0);

        try {
            // commit all entries
            this.dataStorage.flush();
        } catch (Exception ex) {
            System.err.println("Could not flush entries to storage || " + ex.getMessage());
            ex.printStackTrace();
            return;
        }
        
    }
    
    /**
     * Recursively divides the dataset into the mzTree, a depth first construction
     * starting with the head node. 
     * @param sort_by_rt sorting flag, rt or mz
     * @param dataset The recursive call's data partition
     * @param head The recursive call's top level node
     * @param curHeight current height in three (root is 0)
     * @param threadPool Pool to use when dividing sub-nodes, or null to run in same thread
     */
    private void divide(Boolean sort_by_rt, List<MsDataPoint> dataset, MzTreeNode head, int curHeight)
    {
        // leaf flag
        boolean isLeaf = dataset.size() <= MzTree.NUM_POINTS_PER_NODE;
        
        // LEAF: save points, get mins/maxes
        if (isLeaf)
        {
            // leaf node submits its dataset to be written to data store
            try{
                this.dataStorage.savePoints(new StorageFacade.SavePointsTask(head,dataset), this.importMonitor);
            }
            catch(Exception e)
            {
                System.err.println("Could not save points to datastorage for leaf node: " + head.toString() + " | " + e.getMessage());
                e.printStackTrace();
            }
            
            // collect point IDs, mz/rt/intensity min/max
            head.initLeaf(dataset);
            
            dataset = null; // garbage collect away   
        }
        
        // ROOT/INTERMEDIATE: summarize, partition and recurse
        else
        {
            
            // if sort_by_rt is null then don't sort, implies initial partition
            // on mzml sourced data which is already sorted by RT
            if(sort_by_rt != null)
            {
                if (sort_by_rt)
                    Collections.sort(dataset, Comparator.comparing((MsDataPoint dataPoint) -> dataPoint.rt));
                else
                    Collections.sort(dataset, Comparator.comparing((MsDataPoint dataPoint) -> dataPoint.mz));
            }

            // the partition size is the subset length divided by the numChildrenPerNode
            int partitionSize = (int) Math.ceil((double) dataset.size() / (double) this.branchingFactor);

            // split the dataset into partitions
            List<List<MsDataPoint>> partitions = new ArrayList<>();
            int i = 0;
            while(i < dataset.size())
            {
                // populate partition
                final List<MsDataPoint> partition = dataset.subList(i, Math.min(i + partitionSize, dataset.size()));
                i += partition.size();

                // collect partition
                partitions.add(partition);
            }

            // free dataset for GC
            dataset = null;
            
            // distribute the partitions to child nodes
            for(List<MsDataPoint> partition : partitions)
            {
                // instantiate child node
                MzTreeNode child = new MzTreeNode(this.branchingFactor);

                // resolve sort_by_rt
                Boolean my_sort_by_rt = sort_by_rt;

                // if null (initial call mzML) set to true
                if(my_sort_by_rt == null)
                    my_sort_by_rt = true;

                // recursively divide child node (depth first)
                this.divide(!my_sort_by_rt, partition, child, curHeight + 1);

                // collect child node
                head.addChildGetBounds(child);
            }
            
            // collect summary of points from child nodes (additionally saves pointIDs)
            head.summarizeFromChildren(MzTree.NUM_POINTS_PER_NODE, this.summarizer, this.dataStorage);
            
        } // END ROOT/INTERMEDIATE NODE
        
    }
    
    /**
     * Upon the user selecting a memory-conservative load, configures the tree for
     * a partitioned load according to available memory.
     *      - A minimum branching factor must be reached by the partitioned configuration,
     *        a branching factor smaller than the minimum creates a very tall tree. 
     *        Increasing the branching factor decreases partition size, 
     *        thus not endangering memory consumption. 
     *      - If the partitioned configuration returns a branching factor of 1 
     *        then the entire dataset will fit into memory. Revert to default configuration.
     * @param mzmlParser input mzml file parser
     * @return false if partitioned load is unnecessary (entire dataset will fit in RAM), otherwise true 
     * @throws XMLStreamException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws DataFormatException 
     */
    private boolean partitionedLoadConfiguration(MzmlParser mzmlParser) throws XMLStreamException, FileNotFoundException, IOException, DataFormatException, Exception
    {
        
        this.importMonitor.update(null, ImportStatus.PARSING);
        
        // count the number of points in the mzML file
        int numPoints = mzmlParser.countPoints();

        // number of available bytes in java heap
        long numBytesInHeap = Runtime.getRuntime().maxMemory();

        // max allowed points to hold in memory at a time
        // = (heap size * FRACTION) / bytes per point
        int maxPointsInRam = (int) Math.floor((numBytesInHeap * MzTree.HEAP_FRACTION) / (float)MsDataPoint.MEM_NUM_BYTES_PER_POINT);
        
        // number of leafNodes = globalNumPoints / hdBlockTupleCapacity
        int numLeafNodes = (int) Math.ceil((float) numPoints / (float) MzTree.NUM_POINTS_PER_NODE);

        // branching factor determined by the max number of points allowed in RAM
        // if branchingFactor != 1 each level 1 subtree will be processed one at a time
        this.branchingFactor = (short)Math.ceil((float)numPoints / (float)maxPointsInRam);

        // branchingFactor != 1 implies the entire dataset won't fit into the heap,
        // branchingFactor determines treeHeight
        if(this.branchingFactor != 1)
        {
            // ensure we use at least the minimum branching factor
            this.branchingFactor = Math.max(this.branchingFactor, MzTree.MINIMUM_BRANCHING_FACTOR);
            
            // treeHeight = log_branchingFactor(numLeafNodes)
            // cool logarithmic identity: logb(n) = log(n) / log(b)
            this.treeHeight = (short)(Math.ceil(Math.log(numLeafNodes) / Math.log(this.branchingFactor)));

            // if branchingFactor unchanged by max call partitionSize == maxPointsInRam
            // else partitionSize < maxPointsInRam -> SAFE
            int partitionSize = (int) Math.floor( (float) numPoints / (float) this.branchingFactor);
            
            // prepare parser for partitioned read
            // recalculate partition size
            mzmlParser.initPartitionedRead(partitionSize);
            
            this.initDataStorage(STORAGE_INTERFACE_CHOICE, null, numPoints);
            
            // inform importMonitor of the amount of work to do
            int numPointsToSave = numPoints; // save points: dataset.length
            int numNodePointsToSave = this.calculateNodePoints(numPointsToSave);
            this.importMonitor.update(null, (float) (numPointsToSave + numNodePointsToSave));
            
            return true;
        }
        // else branching factor is 1, implying the entire dataset can fit into heap
        // return false to signal a regular load should ensue
        else    
            return false;
        
    }
    
    /**
     * Recursively saves an mzTree starting at curNode
     * @param curNode node to recursively save
     * @param parentNodeID ID of the node's parent (0 if no parent)
     */
    private void recursiveNodeSave(MzTreeNode curNode, int parentNodeID)
    {
        try{
            // save node to db
            curNode.nodeID = this.dataStorage.saveNode(curNode, parentNodeID);
            
            // save node points to db
            this.dataStorage.saveNodePoints(curNode, this.importMonitor);
            
            // recurse on chilren
            for(MzTreeNode childNode : curNode.children)
                this.recursiveNodeSave(childNode, curNode.nodeID);
                
        }
        catch(Exception e){
            System.err.println("Could not save node || " + e.getMessage());
            e.printStackTrace();
        }
        
    }
    
    /**
     * inits the data storage module
     * @param storageChoice Storage interface selection
     * @param filePath (optional) location to create storage file
     * @param numPoints (Hybrid only) number of points that will be saved in file
     * @param importMonitor Import progress monitor
     * @throws Exception 
     */
    private void initDataStorage(StorageFacadeFactory.Facades storageChoice, String filePath, Integer numPoints) throws Exception
    {
        // init data storage module
        this.dataStorage = StorageFacadeFactory.create(storageChoice);
        this.dataStorage.init(filePath, numPoints);
        this.pointCache.dataStorage = this.dataStorage;

        this.importMonitor.setMzTreeFilePath(this.dataStorage.getFilePath());
    }
    
    //***********************************************//
    //                    QUERY                      //
    //***********************************************//
    
    /**
     * Queries the MzTree for points contained with the mz, rt bounds
     *
     * @param mzMin query mz lower bound
     * @param mzMax query mz upper bound
     * @param rtMin query rt lower bound
     * @param rtMax query rt upper bound
     * @param numPoints number of points to be returned
     * @param intmin minimum intensity threshold to return a point
     * @param shouldUseSummary
     * @return 2-dimensional double array
     */
    public List<MsDataPoint> query(double mzMin, double mzMax,
            float rtMin, float rtMax, int numPoints, double intmin, boolean shouldUseSummary)
    {
        // if zero passed for any query bound use relative min/max
        mzMin = (mzMin == 0) ? this.head.mzMin : mzMin;
        mzMax = (mzMax == 0) ? this.head.mzMax : mzMax;
        rtMin = (rtMin == 0) ? this.head.rtMin : rtMin;
        rtMax = (rtMax == 0) ? this.head.rtMax : rtMax;
                
        // current level in tree
        int curLevel = 0;
        
        // all nodes in current level of tree whose bounds are within 
        // the query's bounds
        ArrayList<MzTreeNode> curLevelNodesInBounds = new ArrayList<>();
        
        // IDs the points in the current level that are within the query bounds
        ArrayList<MsDataPoint> curLevelPointsInBounds = new ArrayList<>();
        
        if(shouldUseSummary){
        
            // search for the level in the tree that has curLevelPointsInBounds >= numPoints
            // base case: curLevel is the leaf level
            while(curLevelPointsInBounds.size() < numPoints && curLevel != this.treeHeight + 1){

                // populates curLevelNodesInBounds with the children of the current curLevelNodesInBounds
                // that are within the query's bounds
                curLevelNodesInBounds = this.collectNextLevelNodesInBounds(curLevelNodesInBounds,mzMin,mzMax,rtMin,rtMax);

                // find all curLevel's points within the query's bounds
                curLevelPointsInBounds = this.collectPointsWithinBounds(curLevelNodesInBounds,mzMin,mzMax,rtMin,rtMax, intmin);
                curLevel++;
            }

            // case to handle: the bounds were small enough to make it to the
            // leaf nodes (curLevel == this.treeHeight)
            // without surpassing numPoints (curLevelPointsInBounds.size() < numPoints)
            // simply return all points
            if(curLevelPointsInBounds.size() <= numPoints)
                return curLevelPointsInBounds;

            // else we made it to a level that has more points in bounds than
            // numPoints (curLevelPointsInBounds.size() >= numPoints)
            // (could be the leaf level)
            // return sample from current points in bounds using the summarizer
            else
                return this.summarizer.summarize(curLevelPointsInBounds, numPoints);
            
        }
        
        // else the leaf nodes must satisfy this query 
        else
        {
            // search for the level in the tree that has curLevelPointsInBounds >= numPoints
            // base case: curLevel is the leaf level
            while(curLevel != this.treeHeight + 1){

                // populates curLevelNodesInBounds with the children of the current curLevelNodesInBounds
                // that are within the query's bounds
                curLevelNodesInBounds = this.collectNextLevelNodesInBounds(curLevelNodesInBounds,mzMin,mzMax,rtMin,rtMax);

                curLevel++;
            }
            
            // all points belonging to the pertinent leaf nodes
            try
            {
                // populate each node's pointID array
                for(MzTreeNode node : curLevelNodesInBounds)
                    if (node.pointIDs == null) 
                    {
                        // node.pointIDs is lazy loaded on first access, not on file open
                        try {
                            List<Integer> pointIDs = this.dataStorage.getNodePointIDs(node.nodeID);
                            node.pointIDs = pointIDs.stream().mapToInt(x -> x).toArray();
                        } 
                        catch (Exception e) 
                        {
                            System.err.println("Failed to retrieve points for node. Future query results may be incomplete.");
                            e.printStackTrace();
                        }
                    }
                
                return this.dataStorage.loadLeavesPointsInBounds(curLevelNodesInBounds, mzMin, mzMax, rtMin, rtMax);
            }
            catch(Exception e)
            {
                System.err.println("Failed to load points from the leaf level | " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * Collects all child nodes of all nodes in curLevelNodesInBounds that overlap
     * with the bounds of the query
     * @param curLevelNodesInBounds nodes in the current level that overlap with the query bounds
     * @param mzMin mz lower bound
     * @param mzMax mz upper bound
     * @param rtMin rt lower bound
     * @param rtMax rt upper bound
     * @return ArrayList containing nodes of the next level that overlap with the query bounds
     */
    private ArrayList<MzTreeNode> collectNextLevelNodesInBounds(ArrayList<MzTreeNode> curLevelNodesInBounds, double mzMin, double mzMax, float rtMin, float rtMax){
        
        // nodes in the next level that overlap the bounds of the query
        ArrayList<MzTreeNode> nextLevelNodesInBounds = new ArrayList<>();
        
        // base case: curLevelNodesInBounds is empty
        // return ArrayList with the head node of the tree (guaranteed to overlap  bounds)
        if(curLevelNodesInBounds.isEmpty()){
            nextLevelNodesInBounds.add(this.head);
            return nextLevelNodesInBounds;
        }
         
        // iterate through all current level nodes
        for(MzTreeNode curLevelNode : curLevelNodesInBounds)
        {
            // if there are children check each child for overlapping bounds
            if (!curLevelNode.children.isEmpty()) 
            {
                // iterate through current level node's children
                for(MzTreeNode nextLevelNode : curLevelNode.children)
                {
                    // collecting if overlaps with the bounds of the query
                    if(this.doesOverlap(nextLevelNode, mzMin, mzMax, rtMin, rtMax))
                        nextLevelNodesInBounds.add(nextLevelNode);
                }
            } 
            
            // else this is a leaf node not at the expected leaf level,
            // include in next level
            else
                nextLevelNodesInBounds.add(curLevelNode);
        }
        return nextLevelNodesInBounds;
    }
    
    /**
     * Collects the points within a collection of MzTreeNodes that fall within
     * the given mz/rt bounds
     * @param nodes nodes whose points are to be checked against bounds of query
     * @param mzMin mz lower bound
     * @param mzMax mz upper bound
     * @param rtMin rt lower bound
     * @param rtMax rt upper bound
     * @return List of MsDataPoints belonging to the given nodes that are within the given bounds
     */
    private ArrayList<MsDataPoint> collectPointsWithinBounds(ArrayList<MzTreeNode> nodes,
            double mzMin, double mzMax, float rtMin, float rtMax, double intmin){
        
        // collect all point IDs from all nodes
        ArrayList<Integer> allNodesPointIDs = new ArrayList<>();
        for(MzTreeNode node : nodes) {
            if (node.pointIDs == null) {
                // node.pointIDs is lazy loaded on first access, not on file open
                try {
                    List<Integer> pointIDs = this.dataStorage.getNodePointIDs(node.nodeID);
                    node.pointIDs = pointIDs.stream().mapToInt(x -> x).toArray();
                } catch (Exception e) {
                    System.err.println("Failed to retrieve points for node. Future query results may be incomplete.");
                    e.printStackTrace();
                }
            }
            
            for(int pointID : node.pointIDs)
                allNodesPointIDs.add(pointID);
        }
        
        // retrieve all points from pointCache
        ArrayList<MsDataPoint> allNodesPoints = this.pointCache.retrievePoints(allNodesPointIDs);
        
        // arraylist for collecting points that fall within bounds
        ArrayList<MsDataPoint> pointsWithinBounds = new ArrayList<>();
        
        // iterate through all nodes, checking to see if they fall within the bounds
        for(MsDataPoint pointToCheck : allNodesPoints){
            
            // if in bounds then collect
            if(pointToCheck.isInBounds(mzMin, mzMax, rtMin, rtMax) && pointToCheck.intensity >= intmin)
                pointsWithinBounds.add(pointToCheck);
        }
        
        return pointsWithinBounds;
    }
   
    /**
     * Checks if the search bounds overlap with a node's data bounds
     * @param node node to check
     * @param mzMin mz lower bound
     * @param mzMax mz upper bound
     * @param rtMin rt lower bound
     * @param rtMax rt upper bound
     * @return True if node's bounds overlap search bounds, false otherwise
     */
    private boolean doesOverlap(MzTreeNode node, double mzMin, double mzMax, float rtMin, float rtMax)
    {
        return // node's rtmin within search
                (((node.rtMin >= rtMin && node.rtMin <= rtMax)
                || // node's rtmax within search
                (node.rtMax <= rtMax && node.rtMax >= rtMin)
                || // node's rt range encapsulates search
                (node.rtMin <= rtMin && node.rtMax >= rtMax))
                && // node's mz min within search
                ((node.mzMin >= mzMin && node.mzMin <= mzMax)
                || // node's mz max within search
                (node.mzMax <= mzMax && node.mzMax >= mzMin)
                || // node's mz range encapsulated by search
                (node.mzMin <= mzMin && node.mzMax >= mzMax)));
    }
    
    //***********************************************//
    //                 SEGMENTATION                  //
    //***********************************************//
    
    /**
     * Updates the points specified by pointIDs to have the given traceID
     * @param traceID updated traceID value
     * @param pointIDs IDs of points to update
     * @throws java.lang.Exception
     */
    public void updateTraces(short traceID, Integer[] pointIDs) throws Exception
    {
        // update the trace of each point specified in pointIDs
        for(int i = 0; i < pointIDs.length; i++)
            this.pointCache.shallowTraceUpdate(pointIDs[i], traceID);
            

        this.dataStorage.updateTraces(traceID, pointIDs);
      
    }
    
    /**
     * Creates a trace in the traceMap and storage
     * @param traceID ID of the trace to add
     * @param envelopeID initial envelope for the trace
     * @throws java.lang.Exception
     * 
     */
    public void insertTrace(short traceID, short envelopeID) throws Exception
    {
        this.traceMap.put(traceID, envelopeID);
        
        this.dataStorage.insertTrace(traceID, envelopeID);
    }
    
    /**
     * Deletes a trace from the traceMap and storage
     * @param traceID ID of the trace to delete
     * @throws java.lang.Exception
     */
    public void deleteTrace(short traceID) throws Exception
    {
        // delete trace from traceMap
        this.traceMap.remove(traceID);
        
        // delete trace from storage
        this.dataStorage.deleteTrace(traceID);        
    }
    
    /**
     * Updates the envelopeID of the specified traces
     * @param envelopeID new envelope ID value
     * @param traceIDs IDs of the traces to update
     * @throws java.lang.Exception
     */
    public void updateEnvelopes(short envelopeID, Integer[] traceIDs) throws Exception
    {
        
        // update each trace in the trace map
        for(int i = 0; i < traceIDs.length; i++)
            this.traceMap.put(traceIDs[i].shortValue(), envelopeID);
        
        // update traces in storage
        this.dataStorage.updateEnvelopes(envelopeID, traceIDs);
    }
    
    //***********************************************//
    //                    CSV EXPORT                 //
    //***********************************************//
    
    /**
     * Exports the given data range into a csv at filepath
     * @param filepath out location
     * @param minMZ lower mz bound
     * @param maxMZ upper mz bound
     * @param minRT lower rt bound
     * @param maxRT upper rt bound
     * @throws java.io.IOException
     */
    public void export(String filepath, double minMZ, double maxMZ, float minRT, float maxRT) throws IOException
    {
        //append csv extension if not already there
        if(!filepath.endsWith(".csv"))
            filepath = filepath + ".csv";
        
        try ( CSVWriter writer = new CSVWriter(new FileWriter(filepath)) ) 
        {
            writer.writeNext(new String[] {"m/z","RT","intensity","traceID","envelopeID"});
            
            // get the points of the data range
            // THIS IS WHERE THE OPTIMIZATION PROBLEM STARTS
            // currently loads all pertinent points into memory (could be the whole file)
            List<MsDataPoint> points = this.query(minMZ, maxMZ, minRT, maxRT, Integer.MAX_VALUE, 0, false);
            
            // write away!
            for (MsDataPoint p : points)
            {
                int envelopeID = this.traceMap.containsKey(p.traceID) ? this.traceMap.get(p.traceID) : 0;
                writer.writeNext( new String[] {Double.toString(p.mz), Float.toString(p.rt), Double.toString(p.intensity), Integer.toString(p.traceID), Integer.toString(envelopeID) });
            }
                
        }
    }
    
    
    //***********************************************//
    //                   HELPERS                     //
    //***********************************************//
    
    /**
     * Adds a list of points to the point cache
     * @param points 
     */
    private void addToPointCache(List<MsDataPoint> points)
    {
        for(MsDataPoint point : points)
            this.pointCache.put(point);
    }
    
    /**
     * Set's the storage facade object for both the MzTree and the point cache
     * @param dataStorage 
     */
    public void setDataStorage(StorageFacade dataStorage)
    {
        this.dataStorage = dataStorage;
        this.pointCache.dataStorage = this.dataStorage;
    }
    
    /**
     * Calculates the number of NodePoint entities to be processed
     * in writing the MzTree to disk
     * @return # NodePoint entities to be processed
     */
    private int calculateNodePoints(int totalPoints)
    {
        // number of points belong to all non-leaf nodes
        // = # non-leaf nodes * # pts per node
        int nonLeafNodeCount = this.geometricSeriesSum(this.branchingFactor, this.treeHeight - 1);
        int nonLeafNodePointCount = nonLeafNodeCount * this.NUM_POINTS_PER_NODE;
        
        // number of points belonging to all leaf nodes
        // its the entire dataset
        int leafNodePointCount = totalPoints;
        
        return nonLeafNodePointCount + leafNodePointCount;
    }
    
    /**
     * Calculates a geometric series sum
     * @param r geometric ratio
     * @param n summation limit
     * @return Geometric series sum
     */
    private int geometricSeriesSum(int r, int n){
        if (r == 1) {
            return r*n;
        }
        return (1 - (int)Math.pow(r, n + 1)) / (1 - r);
    }
    
    /**
     * Converts a csv row MsDataPoint to MsDataPoint object
     * @param line
     * @param id
     * @param envelopeID
     * @return 
     */
    private MsDataPoint csvRowToMsDataPoint(String[] line, int id)
    {
        double mz = Double.parseDouble(line[0]);
        float rt = Float.parseFloat(line[1]);
        double intensity = Double.parseDouble(line[2]);
        short traceID = Short.parseShort(line[3]);
        MsDataPoint point = new MsDataPoint(id, mz, rt, intensity);
        point.traceID = traceID;
        return point;
    }
    
    private void displayExecutionTimes()
    {   
        System.out.println("Tree Build Real Time: " + this.overallTreeBuildTime);
    }

    public void close() 
    {
        String filepath = this.dataStorage.getFilePath();
        try{
            this.dataStorage.close();
        }
        catch(Exception e)
        {
            System.err.println("failed to close the datastorage");
        }
        
        new File(filepath).delete();
        new File(filepath+"-points").delete();
        
        
    }
}
