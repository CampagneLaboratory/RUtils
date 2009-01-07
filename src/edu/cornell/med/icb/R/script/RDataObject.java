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

/**
 * Data FROM RScriptHelper R script execution.
 * Set to package private because you should be using
 * RInputOutputData to create / retrieve these objects.
 * @author Kevin Dorff
 */
public class RDataObject {

    /** The data type of this object. */
    private final RDataObjectType dataType;

    /** The field name of this object. */
    private final String fieldName;

    /** The value. */
    private Object value;

    /**
     * Used for OUTPUTs.
     * @param type the field type
     * @param field the field name
     */
    RDataObject(final RDataObjectType type, final String field) {
        this.dataType = type;
        this.fieldName = field;
    }

    /**
     * Used internally for all RInputData creations, but all creation comes in
     * through one of the other constructors to enforce acceptance of only supported types.
     * @param type the field type
     * @param field the field name
     * @param val the field value
     */
    RDataObject(final RDataObjectType type, final String field, final Object val) {
        this.dataType = type;
        this.fieldName = field;
        this.value = val;
    }

    /**
     * Get the field name.
     * @return the field name
     */
    String getFieldName() {
        return fieldName;
    }

    /**
     * Get the field data type.
     * @return the the data type
     */
    RDataObjectType getDataType() {
        return dataType;
    }

    /**
     * Get the value.
     * @return the String value.
     */
    Object getValue() {
        return value;
    }

    /**
     * Set the value.
     * @param val the value
     */
    void setValue(final Object val) {
        this.value = val;
    }

    /**
     * Take a double[][] and flatten it to a double[].
     * @param src the double[][]
     * @return the double[] containing the data from src
     */
    public static double[] flatten2DArrayByRows(final double[][] src) {
        final int numRows = src.length;
        final int rowLength = src[0].length;
        final double[] dest = new double[numRows * rowLength];
        int pos = 0;
        for (double[] srcByRow : src) {
            System.arraycopy(srcByRow, 0, dest, pos, rowLength);
            pos += rowLength;
        }
        return dest;
    }

    /**
     * Take a long[][] and flatten it to a double[].
     * @param src the long[][]
     * @return the long[] containing the data from src
     */
    public static long[] flatten2DArrayByRows(final long[][] src) {
        final int numRows = src.length;
        final int rowLength = src[0].length;
        final long[] dest = new long[numRows * rowLength];
        int pos = 0;
        for (long[] srcByRow : src) {
            System.arraycopy(srcByRow, 0, dest, pos, rowLength);
            pos += rowLength;
        }
        return dest;
    }
}
