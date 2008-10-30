/*
 * Copyright (C) 2008 Institute for Computational Biomedicine,
 *                    Weill Medical College of Cornell University
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

import org.junit.Test;
import org.rosuda.REngine.Rserve.RserveException;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

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

        final RScript rscript = new RScript("rscripts/test_sum_prod.R");
        rscript.setInput("base", 2.0d);
        rscript.setInput("values", new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

        rscript.setOutput("sum", RDataObjectType.Double);
        rscript.setOutput("prod", RDataObjectType.Double);
        rscript.setOutput("comb", RDataObjectType.DoubleArray);

        // Execute the script with the given outputs and obtain the outputs
        rscript.execute();

        // Assert that we have the proper values in outputs
        assertEquals(17.0d, rscript.getOutputDouble("sum"));
        assertEquals(122.0d, rscript.getOutputDouble("prod"));
        assertSameArray(new double[] {17.0d, 122.0d},
                rscript.getOutputDoubleArray("comb"));

        // Update the inputs and rerun
        rscript.setInput("base", 3.0d);
        rscript.setInput("values", new double[] {2.0, 3.0, 4.0, 5.0, 6.0});
        rscript.execute();
        assertEquals(23.0d, rscript.getOutputDouble("sum"));
        assertEquals(723.0d, rscript.getOutputDouble("prod"));
        assertSameArray(new double[] {23.0d, 723.0d},
                rscript.getOutputDoubleArray("comb"));
    }

    private void assertSameArray(final double[] expected, final double[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }
}
