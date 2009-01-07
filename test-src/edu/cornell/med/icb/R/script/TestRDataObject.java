/*
 * Copyright (C) 2009 Institute for Computational Biomedicine,
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

import static junit.framework.Assert.assertTrue;
import org.apache.commons.lang.ArrayUtils;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

/**
 * Describe class here.
 *
 * @author Kevin Dorff
 */
public class TestRDataObject {

    /**
     * Test flatting 2d double array.
     */
    @Test
    public void testFlatten2DArrayByRowsLong() {
        long[][] x = new long[][] {{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11}};
        long[] y = RDataObject.flatten2DArrayByRows(x);
        assertArrayEquals(new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, y);
    }

    /**
     * Test flatting 2d double array.
     */
    @Test
    public void testFlatten2DArrayByRowsDouble() {
        double[][] x = new double[][] {{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11}};
        double[] y = RDataObject.flatten2DArrayByRows(x);
        assertDoubleArrayEquals(new double[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, y);
    }

    /**
     * JUnit doesn't provide an assertArrayEquals for double[] values, so this is my version.
     * @param expected expected values
     * @param actual actual values
     */
    public static void assertDoubleArrayEquals(
            final double[] expected, final double[] actual) {
        assert expected.length == actual.length;
        String errorMessage = String.format("double[] values not equal. Expected=%s Actual=%s",
                    ArrayUtils.toString(expected), ArrayUtils.toString(actual));
        assertTrue(errorMessage, ArrayUtils.isEquals(expected, actual));
    }

    /**
     * JUnit doesn't provide an assertArrayEquals for double[][] values, so this is my version.
     * @param expected expected values
     * @param actual actual values
     */
    public static void assertDouble2DArrayEquals(
            final double[][] expected, final double[][] actual) {
        assert expected.length == actual.length;
        for (int i = 0; i < expected.length; i++) {
            assertDoubleArrayEquals(expected[i], actual[i]);
        }
    }
}
