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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles pooling of connections to <a href="http://www.rforge.net/Rserve/">Rserve</a> instances.
 * Operations are based on the ObjectPool interface as defined in the
 * <a href="http://commons.apache.org/pool/">commons pool</a> package however, this class does
 * not implement the interface because the commons pool package does not support JDK 1.5+
 * features at the time this class was written.
 * <p>
 * Rserve processes available to the pool are configured using a fairly simply xml file.  The
 * root element is called {@code RConnectionPool} and the child nodes are called {@code RServer}.
 * There should be one {@code Rserver} node per Rserve process you wish to be made available in
 * the pool.  Each Rserve node has the following attributes:
 * <ul>
 * <li>host - The host/ip Rserve is running on (required)
 * <li>port - The TCP port Rserve is listening on (default = 6311)
 * <li>username - Username to supply for the connection
 * <li>password - Password to supply for the connection
 * </ul>
 * The following configuration would make three servers available to the pool.
 * <p><em><blockquote><pre>
 * &lt;!-- Configuration file for the connection pool to Rserve processes --&gt;
 * &lt;RConnectionPool&gt;
 *    &lt;!-- default Rserve process running on localhost --&gt;
 *    &lt;RServer host="localhost"/&gt;
 *
 *    &lt;!-- Rserve process on localhost port 6312 --&gt;
 *    &lt;RServer host="127.0.0.1" port="6312"/&gt;
 *
 *    &lt;!-- Rserve process on foobar.med.cornell.edu port 1234 with authentication --&gt;
 *    &lt;RServer host="foobar.med.cornell.edu" port="1234" username="me" password="mypassword"/&gt;
 * &lt;/RConnectionPool&gt;
 * </pre></blockquote></em>
 *
 * <p>
 * The preferred way to specify the configuration file is through the use of a
 * system property called {@code RConnectionPool.configuration}. The property should be a valid url
 * or the name of a resource file that exists on the classpath.  In case the system property
 * {@code RConnectionPool.configuration} is not defined, then the resource will be set to its
 * default value of {@code RConnectionPool.xml}.
 *
 * <p>
 * The connection pool is implemented using the
 * <a href="http://en.wikipedia.org/wiki/Singleton_pattern">Singleton pattern</a>.
 * Instances of the pool are not created by calling the constructor, but are retrieved using the
 * static method {@link #getInstance()}.  Connections are retrieved from the pool using either
 * {@link #borrowConnection()} or {@link #borrowConnection(long, java.util.concurrent.TimeUnit)}.
 * Both versions will return a valid {@link org.rosuda.REngine.Rserve.RConnection} immediately
 * if one is available.  The borrow method with no parameters will block if there are no
 * connections available, while the latter form will wait until the timeout expires before
 * returning {@code null}.
 */
public final class RConnectionPool {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RConnectionPool.class);

    /**
     * Indicates that that connection pool has been closed and not available for use.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * The list of connections that can be used in the pool.  Note that
     * {@link org.rosuda.REngine.Rserve.RConnection} objects are not stored directly.
     * Rather information used to make the connection objects are stored.
     */
    private final BlockingDeque<RConnectionInfo> connections =
            new LinkedBlockingDeque<RConnectionInfo>();

    /**
     * A list of connections that have been borrowed from the pool but not yet returned.
     */
    private final Map<RConnection, RConnectionInfo> activeConnectionMap =
            new ConcurrentHashMap<RConnection, RConnectionInfo>();

    /**
     * The total number of sessions managed by this pool.
     */
    private final AtomicInteger numberOfConnections = new AtomicInteger();

    /**
     * Used to synchronize code blocks.
     */
    private final Object syncObject = new Object();

    /**
     * Get the connection pool.
     * @return The connection pool instance.
     */
    public static RConnectionPool getInstance() {
        return SingletonHolder.getInstance();
    }

    /**
     * Get the connection pool, suggesting a configuration
     * if the pool is not already created with a different configuration.
     * @param configuration desired configuration for the pool
     * @return The connection pool instance.
     */
    public static RConnectionPool getInstance(final XMLConfiguration configuration) {
        return SingletonHolder.getInstance(configuration);
    }

    /**
     * Get the connection pool, suggesting a configuration
     * if the pool is not already created with a different configuration.
     * @param configurationURL desired configurationn for the pool
     * @return The connection pool instance.
     * @throws ConfigurationException error configuring from the supplied URL
     */
    public static RConnectionPool getInstance(final URL configurationURL)
            throws ConfigurationException {
        return SingletonHolder.getInstance(configurationURL);
    }

    /**
     * Create a new pool to manage {@link org.rosuda.REngine.Rserve.RConnection} objects
     * using the default configuration method.
     */
    private RConnectionPool() {
        super();

        final URL poolConfigURL = RConfigurationUtils.getConfigurationURL();
        if (LOG.isInfoEnabled()) {
            LOG.info("Configuring pool with: " + poolConfigURL);
        }

        try {
            configure(poolConfigURL);
        } catch (ConfigurationException e) {
            LOG.error("Cannot read configuration: " + poolConfigURL, e);
            closed.set(true);
        }
    }

    /**
     * Configure the rserve instances available to this pool using an xml based
     * configuration at the specified URL.
     * @param configurationURL The URL of the configuration to use
     * @throws ConfigurationException if the configuration cannot be built from the url
     */
    private void configure(final URL configurationURL) throws ConfigurationException {
        configure(new XMLConfiguration(configurationURL));
    }

    /**
     * Create a new pool to manage {@link org.rosuda.REngine.Rserve.RConnection} objects
     * using the specified configuration.
     * @param configuration The configuration object that defines the servers available to the pool
     */
    RConnectionPool(final XMLConfiguration configuration) {
        super();
        configure(configuration);
    }

    /**
     * Configure the rserve instances available to this pool using an xml based
     * configuration.
     * @param configuration The configuration to use
     * @return true if the configuration has at least one valid server, false otherwise
     */
    private boolean configure(final XMLConfiguration configuration) {
        configuration.setValidating(true);
        configuration.setReloadingStrategy(new FileChangedReloadingStrategy());
        final int numberOfRServers = configuration.getMaxIndex("RServer") + 1;
        for (int i = 0; i < numberOfRServers; i++) {
            final String server = "RServer(" + i + ")";
            final String host = configuration.getString(server + "[@host]");
            final int port =
                    configuration.getInt(server + "[@port]", RConfigurationUtils.DEFAULT_RSERVE_PORT);
            final String username = configuration.getString(server + "[@username]");
            final String password = configuration.getString(server + "[@password]");

            final boolean added;
            try {
                added = addConnection(host, port, username, password);
            } catch (RserveException e) {
                LOG.error("Couldn't connect to " + host + ":" + port, e);
                continue;
            }

            if (added) {
                numberOfConnections.getAndIncrement();
            } else {
                LOG.error("Unable to add connection to " + host + ":" + port);
            }
        }

        if (numberOfConnections.get() == 0) {
            LOG.error("No valid servers found!  Closing pool");
            closed.set(true);
        } else {
            // add a shutdown hook so that the pool is terminated cleanly on JVM exit
            Runtime.getRuntime().addShutdownHook(
                    new Thread(RConnectionPool.class.getSimpleName() + "-ShutdownHook") {
                        @Override
                        public void run() {
                            LOG.debug("Shutdown hook is closing the pool");
                            close();
                        }
                    });
        }

        return !closed.get();
    }

    /**
     * Adds a connection to the pool of available connections.
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     * @return true if the connection was added successfully, false otherwise
     * @throws RserveException if there is a problem connecting to the server
     */
    private boolean addConnection(final String host,
                                  final int port,
                                  final String username,
                                  final String password) throws RserveException {
        assertOpen();

        final RConnectionInfo connectionInfo =
                new RConnectionInfo(host, port, username, password);
        return connections.add(connectionInfo);
    }

    /**
     * Throws an {@link IllegalStateException} when this pool has been closed.
     * @see #isClosed()
     */
    private void assertOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Pool is not open");
        }
    }

    /**
     * Has this pool instance been closed.
     * @return true when this pool has been closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Create a new pool to manage {@link org.rosuda.REngine.Rserve.RConnection} objects
     * using the specified configuration.
     * @param configurationURL A url for an xml configuration file that defines the servers
     * available to the pool
     * @throws ConfigurationException if the configuration cannot be built from the url
     */
    RConnectionPool(final URL configurationURL) throws ConfigurationException {
        super();
        configure(configurationURL);
    }

    /**
     * Get the number of connections that are not currently in use.
     * @return The number of connections that are available right now.
     */
    public int getNumberOfIdleConnections()  {
        final int numberOfIdleConnections;
        synchronized (syncObject) {
            numberOfIdleConnections = numberOfConnections.get() - activeConnectionMap.size();
        }
        return numberOfIdleConnections;
    }

    /**
     * Close this pool and any active connections associated with it.
     */
    public void close() {
        if (!closed.getAndSet(true)) {
            LOG.debug("Closing down the RConnectionPool");
            synchronized (syncObject) {
                final Iterator<RConnection> activeConnectionIterator =
                        activeConnectionMap.keySet().iterator();
                while (activeConnectionIterator.hasNext()) {
                    final RConnection connection = activeConnectionIterator.next();
                    connection.close();
                    activeConnectionIterator.remove();
                    numberOfConnections.decrementAndGet();
                }

                final Iterator<RConnectionInfo> idleConnectionIterator = connections.iterator();
                while (idleConnectionIterator.hasNext()) {
                    final RConnectionInfo connectionInfo = idleConnectionIterator.next();
                    idleConnectionIterator.remove();
                    numberOfConnections.decrementAndGet();
                }
            }
        }

        // TODO: terminate embedded servers
    }

    /**
     * Obtains an available {@link org.rosuda.REngine.Rserve.RConnection} from this pool.
     * If all the connections managed by this pool are in use, this method will block until
     * a connection becomes available.
     * @return A valid connection object
     * @throws RserveException if there is an issue connecting with an Rserver
     */
    public RConnection borrowConnection() throws RserveException {
        RConnection connection = null;
        boolean gotConnection = false;
        while (!gotConnection) {
            assertOpen();
            RConnectionInfo connectionInfo = null;
            try {
                connectionInfo = connections.takeFirst();
                connection = borrow(connectionInfo);
                gotConnection = true;
            } catch (RserveException e) {
                // perhaps the server went down?
                LOG.error("Error with connection" + connectionInfo, e);
                if (connectionInfo.numberOfFailedConnectionAttempts.incrementAndGet() > 3) {
                    LOG.error("Three strikes - we're out!");
                    invalidateConnection(connectionInfo);
                } else {
                    // put this connection at the end of the queue
                    connections.addLast(connectionInfo);
                    throw e;
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        return connection;
    }

    /**
     * Removes a session from the pool (presumably because there was an issue with the connection
     * to the Rserve process.  The pool will be closed if this action leaves no valid sessions.
     * @param connectionInfo The connection to remove.
     */
    private void invalidateConnection(final RConnectionInfo connectionInfo) {
        if (connections.remove(connectionInfo) && numberOfConnections.decrementAndGet() <= 0) {
            close();
        }
    }

    /**
     * Removes a connection from the pool (presumably because there was an issue with the
     * connection to the Rserve process. The pool will be closed if this action leaves no valid
     * sessions. The connection <strong>must</strong> have been obtained using this pool
     * and not created externally.
     * @param connection The connection to remove.
     */
    public void invalidateConnection(final RConnection connection) {
        assertOpen();

        final RConnectionInfo connectionInfo = activeConnectionMap.remove(connection);
        if (connectionInfo == null) {
            throw new IllegalArgumentException("Connection is not managed by this pool");
        }

        // attempt to close the connection if we still can
        if (connection.isConnected()) {
            connection.close();
        }

        synchronized (syncObject) {
            if (numberOfConnections.decrementAndGet() <= 0) {
                close();
            }
        }
    }

    /**
     * Obtains an available {@link org.rosuda.REngine.Rserve.RConnection} from this pool.
     * If all the connections managed by this pool are in use, this method will wait up to the
     * specified wait time for a connection to become available.
     *
     * @param timeout how long to wait before giving up, in units of <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the <tt>timeout</tt> parameter
     * @return A valid connection object or null if no connection was available withing the timeout
     * period
     * @throws RserveException if there is an issue connecting with an Rserver
     */
    public RConnection borrowConnection(final long timeout, final TimeUnit unit)
            throws RserveException {
        RConnection connection = null;
        boolean gotConnection = false;
        boolean timedOut = false;
        while (!gotConnection && !timedOut) {
            assertOpen();
            RConnectionInfo connectionInfo = null;
            try {
                connectionInfo = connections.pollFirst(timeout, unit);
                if (connectionInfo == null) {
                    LOG.debug("Timeout trying to get a connection");
                    timedOut = true;
                    continue;
                }
                connection = borrow(connectionInfo);
                gotConnection = true;
            } catch (RserveException e) {
                // perhaps the server went down, remove it from the available list
                LOG.error("Error with connection "+ connectionInfo, e);
                if (connectionInfo.numberOfFailedConnectionAttempts.incrementAndGet() > 3) {
                    LOG.error("Three strikes - we're out!");
                    invalidateConnection(connectionInfo);
                } else {
                    // put this connection at the end of the queue
                    connections.addLast(connectionInfo);
                    throw e;
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        return connection;
    }

    /**
     * Get a connection from an existing configuration.
     * @param connectionInfo The session that holds the connection
     * @return A valid connection
     * @throws RserveException if there was a problem getting the connection
     */
    private RConnection borrow(final RConnectionInfo connectionInfo) throws RserveException {
        final String host = connectionInfo.getHost();
        final int port = connectionInfo.getPort();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting connection with " + host + ":" + port);
        }

        // create a new connection
        final RConnection connection = new RConnection(host, port);
        // authenticate with the server if needed
        if (connection.needLogin()) {
            final String username = connectionInfo.getUsername();
            final String password = connectionInfo.getPassword();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Logging in as " + username);
            }
            connection.login(username, password);
        }

        activeConnectionMap.put(connection, connectionInfo);
        return connection;
    }

    /**
     * Get the number of connections that are currently in use.
     * @return The number of connections that have been borrowed but not returned.
     */
    public int getNumberOfActiveConnections() {
        return activeConnectionMap.size();
    }

    /**
     * Get the number of potential connections managed by the pool.
     * @return the number of connections managed by the pool
     */
    public int getNumberOfConnections() {
        return numberOfConnections.get();
    }

    /**
     * Return a connection to the pool. The connection <strong>must</strong> have been obtained
     * using this pool and not created externally.
     * @param connection The connection to return
     */
    public void returnConnection(final RConnection connection) {
        assertOpen();

        final RConnectionInfo connectionInfo = activeConnectionMap.remove(connection);
        if (connectionInfo == null) {
            throw new IllegalArgumentException("Connection is not managed by this pool");
        }

        // attempt to close the connection if we still can
        if (connection.isConnected()) {
            connection.close();
        }

        connections.addFirst(connectionInfo);
    }

    /**
     * SingletonHolder is loaded on the first execution of RConnectionPool.getInstance()
     * or the first access to SingletonHolder.INSTANCE, not before.
     */
    private static final class SingletonHolder {
        /**
         * Used to synchronize code blocks.
         */
        private static final Object holderSyncObject = new Object();

        /**
         * The singleton instance of the connection pool.
         */
        private static RConnectionPool instance;

        /**
         * Used to construct a singleton.
         */
        private SingletonHolder() {
            super();
        }

        /**
         * Used to construct a singleton.
         * @return the singleton RConnectionPool
         */
        private static RConnectionPool getInstance() {
            final RConnectionPool pool;
            synchronized (holderSyncObject) {
                if (instance == null) {
                    instance = new RConnectionPool();
                }
                pool = instance;
            }
            return pool;
        }

        /**
         * Used to construct a singleton.
         * @param configuration configuration to use or null
         * @return the singleton RConnectionPool
         */
        private static RConnectionPool getInstance(final XMLConfiguration configuration) {
            final RConnectionPool pool;
            synchronized (holderSyncObject) {
                if (instance == null) {
                    instance = new RConnectionPool(configuration);
                }
                pool = instance;
            }
            return pool;
        }

        /**
         * Used to construct a singleton.
         * @param configurationURL configuration to use or null
         * @return the singleton RConnectionPool
         * @throws ConfigurationException if the configuration cannot be built from the url
         */
        private static RConnectionPool getInstance(final URL configurationURL)
                throws ConfigurationException {
            final RConnectionPool pool;
            synchronized (holderSyncObject) {
                if (instance == null) {
                    instance = new RConnectionPool(new XMLConfiguration(configurationURL));
                }
                pool = instance;
            }
            return pool;
        }
    }

    private static final class RConnectionInfo extends RConfigurationItem {
        /**
         * Indicates that a connection with this configuration is active.
         */
        private transient boolean active;

        /**
         * A connection based on this configuration.
         */
        private transient RConnection rConnection;

        /**
         * Used to keep track of the number of failed connection attempts.
         */
        private transient AtomicInteger numberOfFailedConnectionAttempts = new AtomicInteger();

        /**
         * Create a new configuration item for an Rserve process.
         * @param host The host/ip Rserve is running on
         * @param port The TCP port Rserve is listening on
         * @param username Username to supply for the connection
         * @param password Password to supply for the connection
         */
        private RConnectionInfo(final String host,
                                final int port,
                                final String username,
                                final String password) {
            super(host, port, username, password);
        }

        /**
         * Indicates that a connection with this configuration is active.
         * @return true if a connection is active
         */
        public boolean isActive() {
            return active;
        }

        /**
         * Indicate that a connection with this configuration is active.
         * @param state true if a connection is active
         */
        public void setActive(final boolean state) {
            this.active = state;
        }

        public RConnection getRConnection() {
            return rConnection;
        }

        public void setRConnection(final RConnection rConnection) {
            this.rConnection = rConnection;
        }
    }
}
