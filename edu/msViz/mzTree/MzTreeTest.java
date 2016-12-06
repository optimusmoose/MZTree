/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.msViz.mzTree;

import edu.msViz.msHttpApi.ImportMonitor;
import edu.msViz.mzTree.summarization.SummarizationStrategyFactory.Strategy;
import java.util.List;


/**
 *
 * @author rob
 */
public class MzTreeTest {
  /**
   * @param args the command line arguments
   */
  public static MzTree mzTree;
  
  public static void main(String[] args) throws Exception {
    ////////////////////// BUILD ///////////////////////////////////////////////
    // get .mzML data
    //String filepath = "../../data/mzml/sample3.mzML";
    String filepath = "../../data/mzml/18185_REP2_4pmol_UPS2_IDA_1.mzML";
    //msData = new MzmlParser(filepath).readAllData();

    Strategy selectedStrategy = Strategy.IntensityCutoff;
    
    ImportMonitor monitor = new ImportMonitor();
    
    mzTree = new MzTree(monitor);
    mzTree.load(filepath, selectedStrategy, true);
    
    List<MsDataPoint> results = mzTree.query(mzTree.head.mzMin, mzTree.head.mzMin, mzTree.head.rtMin, mzTree.head.rtMin+100, 15000, 0, true);
    
    System.out.println("Retrieved results w/ length " + results.size());
    /*
    queryAndTest(0,10000,0,10000);
    queryAndTest(100,500,0,5);
    queryAndTest(500,1000,0,3);
    queryAndTest(5,1000,1,5);
    queryAndTest(20,507,6,10);
    queryAndTest(21,356,0,12);
    queryAndTest(456,10000,0,20);
    queryAndTest(500,1100,5,6);
    queryAndTest(20,459,0,900);
    queryAndTest(0,500,2,900);
    */
  }
  
  /*
  public static void queryAndTest(double mzMin, double mzMax, float rtMin, float rtMax){
  	//QUERY
  	List<MsDataPoint> response = mzTree.query(mzMin, mzMax, rtMin, rtMax, Integer.MAX_VALUE, 0);
  	//CHECK
  	LinkedList<MsDataPoint> bruteForceAnswer = new LinkedList<>();
  	for (MsDataPoint dataPoint: msData){
  		if(dataPoint.rt >= rtMin && dataPoint.rt <= rtMax && dataPoint.mz <= mzMax && dataPoint.mz >= mzMin){
  			bruteForceAnswer.add(dataPoint);
  		}
  	}
	if(response.size() == bruteForceAnswer.size()){
  		System.out.println("PASSED! " + mzMin + "," + mzMax + "," + rtMin+ "," + rtMax);
	} else{
		System.out.println("FAILED! " + mzMin + "," + mzMax + "," + rtMin+ "," + rtMax);
	}
  }*/
}
