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

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.URL;

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
    private final AtomicInteger numberOfSessions = new AtomicInteger();

    /**
     * The number of sessions that have been borrowed from the pool and not yet returned.
     */
    private final AtomicInteger numberOfActiveSessions = new AtomicInteger();

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

        // if the user defined a configuration, use it
        final Configuration systemConfiguraion = new SystemConfiguration();
        final String poolConfig =
                systemConfiguraion.getString("RConnectionPool.config", "RConnectionPool.xml");

        // get the configuration from the classpath
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to find [" + poolConfig + "] using context class loader " + loader);
        }

        URL poolConfigURL = loader.getResource(poolConfig);
        if (poolConfigURL == null) {
            // We couldn't find resource - now try with the classloader that loaded this class
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to find [" + poolConfig + "] using class loader " + loader);
            }
            loader = RConnectionPool.class.getClassLoader();
            poolConfigURL = loader.getResource(poolConfig);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Configuring pool with: " + poolConfigURL);
        }

        final CompositeConfiguration configuration = new CompositeConfiguration();
        try {
            configuration.addConfiguration(new XMLConfiguration(poolConfigURL));
        } catch (ConfigurationException e) {
            LOG.error("Cannot read configuration: " + poolConfigURL);
            // TODO - Do we want to just try and connect to a default Rserve on localhost?
            closed.set(true);
        }

        // TODO: set up connections
    }

    RConnectionPool(final Configuration configuration) {
        super();
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

        // create a new connection
        final RConnection connection = new RConnection(host, port);

        // authenticate with the server if needed
        if (connection.needLogin()) {
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
                if (numberOfSessions.decrementAndGet() <= 0) {
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
        return numberOfActiveSessions.get();
    }

    /**
     * Get the number of connections that are not currently in use.
     * @return The number of connections that are available right now.
     */
    public int getNumberOfIdleConnections()  {
        final int numIdle;
        synchronized (syncObject) {
            numIdle = numberOfSessions.get() - numberOfActiveSessions.get();
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
