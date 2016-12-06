/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.mzTree;

import edu.msViz.msHttpApi.MsDataServer;
import java.io.File;
import java.io.PrintWriter;

/**
 * Performs numIterations conversions on each file in filenames, recording conversion
 * times in the output file
 * @author kyle
 */
public class MzTreeConversion {
    public static void main(String[] args){
        
        MsDataServer dataServer = MsDataServer.getInstance();
        
        // parameters
        String relPath = "../../data/mzml/";
        File output = new File("../comparison/results/conversion_timesOEMMA.txt");
        //String[] filenames = {"sample1.mzML","sample2.mzML", "sample3.mzML", "humantenpercentone.mzML", "64bit_uncompressed.mzML","18185_REP2_4pmol_UPS2_IDA_1.mzML", "OEMMA121101_61b.mzML" };         
        
        // for those of us without enough disk space for 18185 or OEMMA 10 times...
        //String[] filenames = {"sample1.mzML","sample2.mzML", "sample3.mzML", "humantenpercentone.mzML", "64bit_uncompressed.mzML" ,"18185_REP2_4pmol_UPS2_IDA_1.mzML"};
        //String[] filenames = {"humantenpercentone.mzML"};
        String[] filenames = {"OEMMA121101_61b.mzML"};
        int numIterations = 2;
        
        try (PrintWriter writer = new PrintWriter(output.getAbsolutePath())) {
            for(String file : filenames){
                writer.println(file);
                for(int i = 0; i < numIterations; i++)
                    writer.println(dataServer.processFileOpen(relPath+file, false));
                writer.flush();

            }
        }
        catch(Exception ex){
            System.err.println(ex.getMessage());
        }
    }
}
