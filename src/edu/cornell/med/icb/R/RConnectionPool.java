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
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles pooling of connections to an Rserve process.  Operations are based on
 * the ObjectPool from commons-pool.
 */
public final class RConnectionPool {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RConnectionPool.class);

    /**
     * The singleton instance of the connection pool.
     */
    private static final RConnectionPool INSTANCE = new RConnectionPool();

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
     * Default port for Rserve process if one was not specified.
     */
    private static final int DEFAULT_RSERVE_PORT = 6311;

    /**
     * Indicates that that connection pool has been closed and not available for use.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * The list of sessions that can be used in the pool.
     */
    private final Deque<RSession> sessions = new LinkedBlockingDeque<RSession>();

    /**
     * The total number of sessions managed by this pool.
     */
    private final AtomicInteger numberOfConnections = new AtomicInteger();

    /**
     * The number of sessions that have been borrowed from the pool and not yet returned.
     */
    private final AtomicInteger numberOfActiveConnections = new AtomicInteger();

    /**
     * Used to synchronize code blocks.
     */
    private final Object syncObject = new Object();

    /**
     * Get the connection pool.
     * @return The connection pool instance.
     */
    public static RConnectionPool getInstance() {
        return INSTANCE;
    }

    /**
     * Private constructor for factory class.
     */
    RConnectionPool() {
        super();

        URL poolConfigURL;

        // if the user defined a configuration, use it
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
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Configuring pool with: " + poolConfigURL);
        }

        final XMLConfiguration configuration;
        try {
            configuration = new XMLConfiguration(poolConfigURL);
        } catch (ConfigurationException e) {
            LOG.error("Cannot read configuration: " + poolConfigURL, e);
            closed.set(true);
            return;
        }

        configure(configuration);
    }

    RConnectionPool(final XMLConfiguration configuration) {
        super();
        configure(configuration);
    }

    /**
     * Search for a resource using the thread context class loader. If that fails, search
     * using the class loader that loaded this class.  If that still fails, try one last
     * time with {@link ClassLoader#getSystemResource(String)}.
     * @param resource The resource to search for
     * @return A url representing the resource or <code>null</code> if the resource was not found
     */
    private URL getResource(final String resource) {
        URL url;   // get the configuration from the classpath
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to find [" + resource + "] using context class loader " + loader);
        }

        url = loader.getResource(resource);
        if (url == null) {
            // We couldn't find resource - now try with the class loader that loaded this class
            loader = RConnectionPool.class.getClassLoader();
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

    private void configure(final XMLConfiguration configuration) {
        final int numberOfRServers = configuration.getMaxIndex("RServer") + 1;
        for (int i = 0; i < numberOfRServers; i++) {
            final String server = "RServer(" + i + ")";
            final String host = configuration.getString(server + "[@host]");
            final int port = configuration.getInt(server + "[@port]", DEFAULT_RSERVE_PORT);
            final String username = configuration.getString(server + "[@username]");
            final String password = configuration.getString(server + "[@password]");

            try {
                addConnection(host, port, username, password);
            } catch (RserveException e) {
                LOG.error("Couldn't connect to " + host + ":" + port, e);
                continue;
            }

            numberOfConnections.getAndIncrement();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        shutdown();
    }

    /**
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     */
    private boolean addConnection(final String host,
                                  final int port,
                                  final String username,
                                  final String password) throws RserveException {
        assertOpen();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting connection with "+ host + ":" + port);
        }

        // create a new connection
        final RConnection connection = new RConnection(host, port);

        // authenticate with the server if needed
        if (connection.needLogin()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Logging in as " + username);
            }
            connection.login(username, password);
        }

        // by this point we know the connection can be made, save it for the pool
        final RSession session = connection.detach();
        return sessions.add(session);
    }

    /**
     * Throws an {@link IllegalStateException} when this pool has been closed.
     * @throws IllegalStateException when this pool has been closed.
     * @see #isClosed()
     */
    private void assertOpen() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Pool is not open");
        }
    }

    /**
     * Has this pool instance been closed.
     * @return <code>true</code> when this pool has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Removes a session from the pool (presumably because there was an issue with the connection.
     * The pool will be closed if this action leaves no valid sessions.
     * @param session The session to remove.
     */
    private void invalidateSession(final RSession session) {
        synchronized (syncObject) {
            if (sessions.remove(session)) {
                if (numberOfConnections.decrementAndGet() <= 0) {
                    shutdown();
                }
            }
        }
    }

    /**
     * Shutdown this pool.
     */
    public void shutdown() {
        closed.set(true);

        // TODO: terminate embedded servers
    }

    /**
     * Get the number of connections that are currently in use.
     * @return The number of connections that have been borrowed but not returned.
     */
    public int getNumberOfActiveConnections() {
        return numberOfActiveConnections.get();
    }

    /**
     * Get the number of connections that are not currently in use.
     * @return The number of connections that are available right now.
     */
    public int getNumberOfIdleConnections()  {
        final int numIdle;
        synchronized (syncObject) {
            numIdle = numberOfConnections.get() - numberOfActiveConnections.get();
        }
        return numIdle;
    }

    public RConnection borrowConnection() {
        assertOpen();

        RConnection connection = null;
        boolean gotConnection = false;
        while (!gotConnection && !isClosed()) {
            final RSession session = sessions.removeFirst();
            try {
                connection = session.attach();
                gotConnection = true;
            } catch (RserveException e) {
                LOG.error("Error with connection", e);
                invalidateSession(session);
            }
        }

        return connection;
    }

    public void returnConnection(final RConnection connection) throws RserveException {
        assertOpen();

        final RSession session = connection.detach();
        sessions.addFirst(session);
    }
}
