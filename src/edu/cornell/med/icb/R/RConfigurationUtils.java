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

package edu.cornell.med.icb.R;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.MalformedURLException;
import java.net.URL;

import edu.cornell.med.icb.io.ResourceFinder;

/**
 * Utilities to aid configuration of Rserve instances.
 */
final class RConfigurationUtils {
    /**
     * Default port for Rserve process if one was not specified.
     */
    static final int DEFAULT_RSERVE_PORT = 6311;

    /**
     * Name of the system environment property that will set a configuration file to use.
     * #DEFAULT_CONFIGURATION_FILE will be used if this property is not set.
     */
    private static final String DEFAULT_CONFIGURATION_KEY = "RConnectionPool.configuration";

    /**
     * Name of the default configuration file if one is not specified in the environment.
     */
    private static final String DEFAULT_XML_CONFIGURATION_FILE = "RConnectionPool.xml";

    /**
     * This class is for internal use by the {@link edu.cornell.med.icb.R.RConnectionPool}
     * and also from the command line via the {@link RUtils#main(String[])} method.  Normally,
     * other classes should not be using this directly.
     */
    private RConfigurationUtils() {
        super();
    }

    /**
     * Gets the configuration for the RServe processes.
     * @return The RConnectionPool configuration.
     * @throws ConfigurationException if the configuration cannot be built from the url
     * returned by {@link #getConfigurationURL()}
     */
    static XMLConfiguration getConfiguration() throws ConfigurationException {
        return new XMLConfiguration(getConfigurationURL());
    }

    /**
     * Gets the configuration for the RServe processes.
     * @return A url that for the RConnectionPool configuration file.
     */
    static URL getConfigurationURL() {
        URL poolConfigURL; // if the user defined a configuration, use it
        final ResourceFinder resourceFinder = new ResourceFinder("config");
        final Configuration systemConfiguration = new SystemConfiguration();
        if (systemConfiguration.containsKey(DEFAULT_CONFIGURATION_KEY)) {
            final String poolConfig = systemConfiguration.getString(DEFAULT_CONFIGURATION_KEY);
            try {
                // First see if we have a URL from the system configuration
                poolConfigURL = new URL(poolConfig);
            } catch (MalformedURLException e) {
                // resource is not a URL, attempt to get the resource from the class path
                poolConfigURL = resourceFinder.findResource(poolConfig);
            }
        } else {
            poolConfigURL = resourceFinder.findResource(DEFAULT_XML_CONFIGURATION_FILE);
        }
        return poolConfigURL;
    }
}
