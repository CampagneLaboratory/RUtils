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
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utilities to aid configuration of Rserve instances.
 */
final class RConfigurationUtils {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RConfigurationUtils.class);

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
        final Configuration systemConfiguration = new SystemConfiguration();
        if (systemConfiguration.containsKey(DEFAULT_CONFIGURATION_KEY)) {
            final String poolConfig = systemConfiguration.getString(DEFAULT_CONFIGURATION_KEY);
            try {
                poolConfigURL = new URL(poolConfig);
            } catch (MalformedURLException e) {
                // resource is not a URL: attempt to get the resource from the class path
                poolConfigURL = getResource(poolConfig);
            }
        } else {
            poolConfigURL = getResource(DEFAULT_XML_CONFIGURATION_FILE);
            // try a resource in the config directory
            if (poolConfigURL == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Trying in the config directory");
                }

                poolConfigURL = getResource("config/" + DEFAULT_XML_CONFIGURATION_FILE);
            }

            if (poolConfigURL == null) {
                // make a last ditch effort to find the file in the a directory called config
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Last try as a file in the config directory");
                    }
                    poolConfigURL = new File("config" + IOUtils.DIR_SEPARATOR
                            + DEFAULT_XML_CONFIGURATION_FILE).toURI().toURL();
                } catch (MalformedURLException e) {
                    // resource is not a URL
                }
            }
        }
        return poolConfigURL;
    }

    /**
     * Search for a resource using the thread context class loader. If that fails, search
     * using the class loader that loaded this class.  If that still fails, try one last
     * time with {@link ClassLoader#getSystemResource(String)}.
     * @param resource The resource to search for
     * @return A url representing the resource or {@code null} if the resource was not found
     */
    static URL getResource(final String resource) {
        URL url;   // get the configuration from the classpath of the current thread
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to find [" + resource + "] using context class loader " + loader);
        }

        url = loader.getResource(resource);
        if (url == null) {
            // We couldn't find resource - now try with the class loader that loaded this class
            loader = RConfigurationUtils.class.getClassLoader();     // NOPMD
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to find [" + resource + "] using class loader " + loader);
            }
            url = loader.getResource(resource);
        }

        if (url == null) {
            // make a last attempt to get the resource from the class path
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to find [" + resource + "] using system class loader");
            }
            url = ClassLoader.getSystemResource(resource);
        }

        return url;
    }
}
