/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msViz.mzTree.summarization;

import edu.msViz.mzTree.MsDataPoint;
import java.util.ArrayList;
import java.util.List;

/**
 * Summarization strategy that accumulates the intensities of an intensity sorted
 * MS dataset, collecting points that send the accumulation over a threshold
 * @author Kyle
 */
public class WeightedStridingStrategy extends SummarizationStrategy
{
    // length that stride jumps between array elements
    final int STRIDE_LENGTH = 43;

    @Override
    public List<MsDataPoint> summarize(List<MsDataPoint> dataset, int numPoints)
    {
        // selected points
        List<MsDataPoint> selection = new ArrayList<>(numPoints);
        
        // intensity accumulator
        // collects intensities of a sequence of points
        double intensityAccumulation = 0;
        
        // accumulation threshold
        // when accumulating intensities, exceeding this threshold collects the point
        // that overtook the threshold and resets the accumulator
        double accumulationThreshold = this.sumIntensity(dataset) / numPoints;

        // accumulate the intensities of each point, collecting points that
        // send the intensity accumulation over the accumulation threshold.
        // Intensity accumulation resets after exceeding accumulation threshold
        int i = 0; // index for current stride position
        while (true) {
            intensityAccumulation += dataset.get(i).intensity;
            if (intensityAccumulation > accumulationThreshold) {
                selection.add(dataset.get(i));
                intensityAccumulation -= accumulationThreshold;
                if (selection.size() >= numPoints) {
                    break;
                }
            }
            i += STRIDE_LENGTH;
            if (i >= dataset.size()) {
                // stride through data again, starting with a new index
                i = (i%STRIDE_LENGTH) + 1;
                if (i == 0) {
                    // in this case, we start revisiting data points from the start of the set.
                    System.err.println("Got " + dataset.size() + ". Wanted " + numPoints + ". Returned " + selection.size() + ".");
                    break;   // break and have less than numPoints instead of including some points twice.
                }
            }
            
        }
        
        return selection;
    }
}
