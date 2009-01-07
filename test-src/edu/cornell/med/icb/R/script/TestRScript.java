/*
 * Copyright (C) 2008-2009 Institute for Computational Biomedicine,
 *                         Weill Medical College of Cornell University
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.cornell.med.icb.R.script;

import static junit.framework.Assert.assertEquals;
import org.junit.Test;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RserveException;

import java.io.IOException;

/**
 * Test the RScript class.
 * @author Kevin Dorff
 */
public class TestRScript {
    /**
     * Normal case of testing the RScriptHelper.
     * @throws IOException error reading script file
     * @throws RserveException r server error
     * @throws REXPMismatchException r server error
     * @throws REngineException r server error
     */
    @Test
    public void testRHelper()
            throws IOException, RserveException, REXPMismatchException, REngineException {

        double[][] srcData = new double[][] {
                {1.0, 2.0, 3.0, 4.0},
                {5.0, 6.0, 7.0, 8.0},
                {9.0, 10.0, 11.0, 12.0}};

        final RScript rscript = RScript.createFromResource("rscripts/test_sum_prod.R");
        rscript.setInput("base", 2.0d);
        rscript.setInput("values", new double[] {1.0, 2.0, 3.0, 4.0, 5.0});
        // Place a 2d array of doubles in twodvalues
        rscript.setInput("twodvalues", srcData);

        rscript.setOutput("sum", RDataObjectType.Double);
        rscript.setOutput("prod", RDataObjectType.Double);
        rscript.setOutput("comb", RDataObjectType.DoubleArray);

        // Retrieve the 2d array of doubles from twodoutput.
        rscript.setOutput("twodoutput", RDataObjectType.Double2DArray);

        // Execute the script with the given outputs and obtain the outputs
        rscript.execute();

        // Assert that we have the proper values in outputs
        assertEquals(17.0d, rscript.getOutputDouble("sum"));
        assertEquals(122.0d, rscript.getOutputDouble("prod"));
        TestRDataObject.assertDoubleArrayEquals(new double[] {17.0d, 122.0d},
                rscript.getOutputDoubleArray("comb"));

        TestRDataObject.assertDouble2DArrayEquals(srcData,
                rscript.getOutputDouble2DArray("twodoutput"));

        // Update the inputs and rerun
        rscript.setInput("base", 3.0d);
        rscript.setInput("values", new double[] {2.0, 3.0, 4.0, 5.0, 6.0});
        rscript.execute();
        assertEquals(23.0d, rscript.getOutputDouble("sum"));
        assertEquals(723.0d, rscript.getOutputDouble("prod"));
        TestRDataObject.assertDoubleArrayEquals(new double[] {23.0d, 723.0d},
                rscript.getOutputDoubleArray("comb"));
    }

    /**
     * This is the code that is the example in the ICB wiki. Pasted here to verify
     * that it works correctly.
     * @throws REngineException rserve exception
     * @throws REXPMismatchException rserve exception
     * @throws RserveException rserve exception
     */
    @Test
    public void testCodeFromWiki() throws REngineException, REXPMismatchException, RserveException {
        final String ksTest =
           "q <- ks.test(x,y)" + "\n"
           + "p_value <- q$p.value" + "\n"
           + "test_statistic <- q$statistic[[1]]";
        final RScript rscript = RScript.createFromScriptString(ksTest);

        final double[] xValues = new double[] {0.1, 0.2, 0.3, 0.4, 0.5};
        final double[] yValues = new double[] {0.6, 0.7, 0.8, 0.9, 1.0};
        // Specify the input variable names and values for the script.
        rscript.setInput("x", xValues);
        rscript.setInput("y", yValues);
        // Specify the variable names and types for the script output.
        // Outputs should be specified before we execute the script.
        rscript.setOutput("p_value", RDataObjectType.Double);
        rscript.setOutput("test_statistic", RDataObjectType.Double);

        rscript.execute();
        final double pvalue = rscript.getOutputDouble("p_value");
        final double testStat = rscript.getOutputDouble("test_statistic");

        assertEquals(0.00793d, pvalue, 0.0001d);
        assertEquals(1.0d, testStat);
    }
}
