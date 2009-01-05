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

import edu.cornell.med.icb.R.RConnectionPool;
import edu.cornell.med.icb.iterators.TextFileLineIterator;
import edu.cornell.med.icb.io.ResourceFinder;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rosuda.REngine.Rserve.RserveException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.REXP;

/**
 * An RScript where you can specify inputs and retrieve specific output
 * variables. This object is >> NOT << threadspace. If you are running in
 * multiple threads, make one of these objects for EACH thread.
 * @author Kevin Dorff
 */
public class RScript {

    /**
     * The logger for this class.
     */
    private static final Log LOG = LogFactory.getLog(RScript.class);

    private static ResourceFinder resourceFinder = new ResourceFinder();

    /** The R Connection Pool object. */
    private final RConnectionPool connectionPool = RConnectionPool.getInstance();

    /** Save the loaded scripts in a static map. */
    private static final Map<String, StringBuilder> FILENAME_TO_SCRIPT_MAP =
            new HashMap<String, StringBuilder>();

    /** List of inputs. */
    private final Map<String, RDataObject> inputMap;

    /** Map of inputs. */
    private final Map<String, RDataObject> outputMap;

    /** The script. */
    private final String script;

    /**
     * Create an RScript via a script resource name or file name.
     * @param scriptResource the script resource name or filename which contains an R script
     * @return the new RScript object
     * @throws IOException error reading the scriptResource (or file)
     */
    public static RScript createFromResource(final String scriptResource) throws IOException {
        return new RScript(readScript(scriptResource));
    }

    /**
     * Create an RScript from an R Script stored in a string (not a resource or file name).
     * @param script the R script
     * @return the new RScript object
     */
    public static RScript createFromScriptString(final String script) {
        return new RScript(script);
    }

    /**
     * Create an RScript with an R script stored in a StringBuilder.
     * @param scriptVal the R script (NOT a filename)
     */
    private RScript(final String scriptVal) {
        inputMap = new HashMap<String, RDataObject>();
        outputMap = new HashMap<String, RDataObject>();
        script = scriptVal;
    }

    /**
     * Add an output.
     * @param fieldName the field name
     * @param dataType the field type
     */
    public void setOutput(final String fieldName, final RDataObjectType dataType) {
        outputMap.put(fieldName, new RDataObject(dataType, fieldName));
    }

    /**
     * Add an input with a String value.
     * @param fieldName the field name
     * @param value the String input value
     */
    public void setInput(final String fieldName, final String value) {
        assert value != null;
        inputMap.put(fieldName, new RDataObject(
                RDataObjectType.String, fieldName, value));
    }

    /**
     * Add an input with a String[] value.
     * @param fieldName the field name
     * @param value the String[] input value
     */
    public void setInput(final String fieldName, final String[] value) {
        assert value != null;
        inputMap.put(fieldName, new RDataObject(
                RDataObjectType.StringArray, fieldName, value));

    }

    /**
     * Add an input with a double[] value.
     * @param fieldName the field name
     * @param value the double[] input value
     */
    public void setInput(final String fieldName, final Double value) {
        assert value != null;
        inputMap.put(fieldName, new RDataObject(
                RDataObjectType.Double, fieldName, value));
    }

    /**
     * Add an input with a double[] value.
     * @param fieldName the field name
     * @param value the double[] input value
     */
    public void setInput(final String fieldName, final double[] value) {
        assert value != null;
        inputMap.put(fieldName, new RDataObject(
                RDataObjectType.DoubleArray, fieldName, value));
    }

    /**
     * Get the output String value for a specific field.
     * If that field is not defined, returns null.
     * @param fieldName the field to get output data for
     * @return output String value
     */
    public String getOutputString(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return (String) field.getValue();
    }

    /**
     * Get the output String[] value for a specific field.
     * If that field is not defined, returns null.
     * @param fieldName the field to get output data for
     * @return output String[] value
     */
    public String[] getOutputStringArray(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return (String[]) field.getValue();
    }

    /**
     * Get the output double value for a specific field.
     * If that field is not defined, returns Double.NaN.
     * @param fieldName the field to get output data for
     * @return output String value
     */
    public Double getOutputDouble(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return (Double) field.getValue();
    }

    /**
     * Get the output double[] value for a specific field.
     * If that field is not defined, returns null.
     * @param fieldName the field to get output data for
     * @return output double[] value
     */
    public double[] getOutputDoubleArray(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return (double[]) field.getValue();
    }

    /**
     * Gets the output value as an Object.
     * @param fieldName the field to get output data for
     * @return output double[] value
     */
    public Object getOutput(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return field.getValue();
    }

    /**
     * Get the set of output fields names.
     * @return the set of output fields names.
     */
    public Set<String> getOutputFields() {
        return outputMap.keySet();
    }

    /**
     * The OutputDataType for the specified field.
     * If that field is not defined, returns null.
     * @param fieldName the field to get output data type for
     * @return the output data type
     */
    public RDataObjectType getOutputType(final String fieldName) {
        final RDataObject field = outputMap.get(fieldName);
        if (field == null) {
            return null;
        }
        return field.getDataType();
    }

    /**
     * Execute the rscript stored in the string script. Here I made the script
     * variable a StringBuilder so people wouldn't accidentally pass a filename
     * to this method and because when building up an R script by hand the user
     * is more likely to use a StringBuilder to do so.
     * @throws org.rosuda.REngine.Rserve.RserveException  r server error
     * @throws org.rosuda.REngine.REXPMismatchException  r server error
     * @throws org.rosuda.REngine.REngineException r server error
     */
    public void execute() throws RserveException, REXPMismatchException, REngineException {
        RConnection connection = null;
        try {
            connection = connectionPool.borrowConnection();
            setInputs(connection);
            connection.voidEval(script);
            setOutputs(connection);
        } catch (RserveException e) {
            LOG.error(errorMessage(script), e);
            throw e;
        } catch (REXPMismatchException e) {
            LOG.error(errorMessage(script), e);
            throw e;
        } catch (REngineException e) {
            LOG.error(errorMessage(script), e);
            throw e;
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection);
            }
        }
    }

    /**
     * Before the script executes, configure the input variables on the connection
     * @param connection the rconnection
     * @throws RserveException r server error
     * @throws REngineException r server error
     */
    private void setInputs(final RConnection connection)
            throws RserveException, REngineException {
        assert connection != null;
        for (RDataObject input : inputMap.values()) {
            if (input.getDataType() == RDataObjectType.String) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                            "R-input: %s <- %s%n",
                            input.getFieldName(), input.getValue()));
                }
                connection.assign(input.getFieldName(), (String) input.getValue());
            } else if (input.getDataType() == RDataObjectType.StringArray) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                            "R-input: %s <- c(%s)%n",
                            input.getFieldName(), ArrayUtils.toString(input.getValue())));
                }
                connection.assign(input.getFieldName(), (String[]) input.getValue());
            } else if (input.getDataType() == RDataObjectType.Double) {
                final String rcode = String.format("%s <- %s",
                        input.getFieldName(),
                        Double.toString((Double) input.getValue()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("R-input: %s", rcode));
                }
                // Special case for Double since connection.assign doesn't
                // directly support Double values
                connection.voidEval(rcode);
            } else if (input.getDataType() == RDataObjectType.DoubleArray) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                            "R-input: %s <- c(%s)%n",
                            input.getFieldName(),
                            ArrayUtils.toString(input.getValue())));
                }
                connection.assign(input.getFieldName(), (double[]) input.getValue());
            }
        }
    }

    /**
     * After the script executes, set the outputs.
     * @param connection the rconnection
     * @throws RserveException r server error
     * @throws REXPMismatchException r server error
     */
    private void setOutputs(final RConnection connection)
            throws RserveException, REXPMismatchException {
        assert connection != null;
        for (RDataObject output : outputMap.values()) {
            final REXP expression = connection.eval(output.getFieldName());
            if (output.getDataType() == RDataObjectType.String) {
                output.setValue(expression.asString());
            } else if (output.getDataType() == RDataObjectType.StringArray) {
                output.setValue(expression.asStrings());
            } else if (output.getDataType() == RDataObjectType.Double) {
                output.setValue(expression.asDouble());
            } else if (output.getDataType() == RDataObjectType.DoubleArray) {
                output.setValue(expression.asDoubles());
            }
        }
    }

    /**
     * The r server error message.
     * @param script the script
     * @return the error message
     */
    private String errorMessage(final String script) {
        return String.format("Error executing R script [%s]. This should look like R code, "
                + "if it looks like a filename you can called RScriptHelper incorrectly.",
                script);
    }

    /**
     * Read the script from the specified file. Note: this will first check to see if this
     * script was previously read and stored in the FILENAME_TO_SCRIPT_MAP so the file only
     * has to be read one time. This method runs synchronized so multiple objects
     * can share the FILENAME_TO_SCRIPT_MAP map.
     * NOTE: this will look for the script at scriptFilename then at data/scriptFilename
     * before giving up
     * @param scriptFilename the file to read the R script from
     * @return the String value of the R script (content of file)
     * @throws java.io.IOException error reading the file
     */
    private synchronized static String readScript(final String scriptFilename)
            throws IOException {
        StringBuilder script = FILENAME_TO_SCRIPT_MAP.get(scriptFilename);
        if (script == null) {
            URL scriptUrl = resourceFinder.findResource(scriptFilename);
            if (scriptUrl == null) {
                throw new IOException("Could not locate R script for filename " + scriptFilename);
            }
            script = new StringBuilder();
            int i = 0;
            for (String rawLine : new TextFileLineIterator(scriptUrl.openStream())) {
                final String line = rawLine.trim();
                if (StringUtils.isBlank(line) || line.startsWith("#")) {
                    continue;
                }
                if (i++ > 0) {
                    script.append('\n');
                }
                script.append(line);
            }
            FILENAME_TO_SCRIPT_MAP.put(scriptFilename, script);
        }
        return script.toString();
    }

}
