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
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Validates basic functionality of the {@link edu.cornell.med.icb.R.RConnectionPool}.
 */
public class TestRConnectionPool {
    /**
     * A pool configuration with a single server on localhost.
      */
    private static final String POOL_CONFIGURATION_XML =
            "<RConnectionPool><RServer host=\"localhost\"/></RConnectionPool>";

    /**
     * The connection pool under test.  We store it so that it can be shutdown properly.
     */
    private RConnectionPool pool;

    /**
     * Reset the pool before each test.
     */
    @Before
    public void startup() {
        pool = null;
    }

    /**
     * Shutdown the pool after each test.
     */
    @After
    public void shutdown() {
        if (pool != null) {
            pool.shutdown();
        }
    }
    /**
     * Validates that the connection pool will properly hand out a connection
     * to an active Rserve process.  Note: this test assumes that a Rserve
     * process is already running on localhost using the default port.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     * @throws RserveException if there is a problem with the connection to the Rserve process
     */
    @Test
    public void validConnection() throws ConfigurationException, RserveException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);
        assertNotNull("Connection pool should never be null", pool);
        assertFalse("Everybody in - the pool should be open!", pool.isClosed());

        assertEquals("There should be one connection", 1, pool.getNumberOfConnections());
        assertEquals("No connection should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("The connection should be idle", 1, pool.getNumberOfIdleConnections());

        // get a connection from the pool
        final RConnection connection = pool.borrowConnection();
        assertNotNull("Connection should not be null", connection);
        assertTrue("The connection should be connected to the server", connection.isConnected());

        // there should be no more connections available
        assertEquals("There should be one connections", 1, pool.getNumberOfConnections());
        assertEquals("The connection should be active", 1, pool.getNumberOfActiveConnections());
        assertEquals("There should be no idle connections", 0, pool.getNumberOfIdleConnections());

        // but try and get another
        final RConnection connection2 = pool.borrowConnection(100, TimeUnit.MILLISECONDS);
        assertNull("The first connection hasn't been returned - it should be null", connection2);

        // it didn't give us a connection, but make sure the counts didn't change
        assertEquals("There should be one connections", 1, pool.getNumberOfConnections());
        assertEquals("The connection should be active", 1, pool.getNumberOfActiveConnections());
        assertEquals("There should be no idle connections", 0, pool.getNumberOfIdleConnections());

        // return the connection to the pool
        pool.returnConnection(connection);

        // now there should be one available in the pool
        assertEquals("There should be one connection", 1, pool.getNumberOfConnections());
        assertEquals("No connection should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("The connection should be idle", 1, pool.getNumberOfIdleConnections());

        // and the original connection should no longer be connected
        assertFalse("The connection should not be connected to the server anymore",
                connection.isConnected());

        // make sure we can get another good connection after returning the previous one
        final RConnection connection3 = pool.borrowConnection(100, TimeUnit.MILLISECONDS);
        assertNotNull("Connection should not be null", connection3);
        assertTrue("The connection should be connected to the server", connection3.isConnected());

        // there should be no more connections available
        assertEquals("There should be one connections", 1, pool.getNumberOfConnections());
        assertEquals("The connection should be active", 1, pool.getNumberOfActiveConnections());
        assertEquals("There should be no idle connections", 0, pool.getNumberOfIdleConnections());

        pool.returnConnection(connection3);
    }

    /**
     * Validates that the connection pool will not allow connections to be handed out
     * when no valid servers were set.
     */
    @Test(expected = IllegalStateException.class)
    public void noValidServers() {
        // set up a pool with an empty configuration
        pool = new RConnectionPool(new XMLConfiguration());
        assertNotNull("Connection pool should never be null", pool);
        assertTrue("The pool should not be open", pool.isClosed());
        assertEquals("There should be no connections", 0, pool.getNumberOfConnections());
        assertEquals("There should be no connections", 0, pool.getNumberOfActiveConnections());
        assertEquals("There should be no connections", 0, pool.getNumberOfIdleConnections());

        // if we try to get a connection, we shouldn't be able to - there are none configured
        pool.borrowConnection();
    }

    /**
     * Validates that attempting to return a connection that has been closed throws an error.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     * @throws RserveException if there is a problem with the connection to the Rserve process
     */
    @Test(expected = RserveException.class)
    public void returnClosedConnection() throws ConfigurationException, RserveException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);

        assertNotNull("Connection pool should never be null", pool);
        assertFalse("Everybody in - the pool should be open!", pool.isClosed());

        assertEquals("There should be one connection", 1, pool.getNumberOfConnections());
        assertEquals("No connections should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("The connection should be idle", 1, pool.getNumberOfIdleConnections());

        // get a connection from the pool
        final RConnection connection = pool.borrowConnection();
        assertNotNull("Connection should not be null", connection);
        assertTrue("The connection should be connected to the server", connection.isConnected());

        connection.close();
        assertFalse("The connection should not be connected to the server anymore",
                connection.isConnected());

        // return the connection to the pool - this should not work
        pool.returnConnection(connection);
    }

    /**
     * Validates that an attempt to return an invalid connection to the pool throws an error.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     * @throws RserveException if there is a problem with the connection to the Rserve process
     */
    @Test(expected = IllegalArgumentException.class)
    public void returnNullConnection() throws ConfigurationException, RserveException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);
        pool.returnConnection(null);
    }


    /**
     * Validates that the pool can be shut down properly and also that attempting to
     * get a connection after the pool has been shutdown throws an error.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     */
    @Test(expected = IllegalStateException.class)
    public void borrowConnectionAfterShutdown() throws ConfigurationException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);
        assertNotNull("Connection pool should never be null", pool);
        assertFalse("Everybody in - the pool should be open!", pool.isClosed());

        pool.shutdown();
        assertNotNull("Connection pool should never be null", pool);
        assertTrue("Everybody out of the pool!", pool.isClosed());

        assertEquals("There should be no connections", 0, pool.getNumberOfConnections());
        assertEquals("No connections should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("No connections should be idle", 0, pool.getNumberOfIdleConnections());

        pool.borrowConnection();
    }

    /**
     * Validates that the pool can be shut down properly and also that attempting to
     * return a connection after the pool has been shutdown throws an error.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     * @throws RserveException if there is a problem with the connection to the Rserve process
     */
    @Test(expected = IllegalStateException.class)
    public void returnConnectionAfterShutdown() throws ConfigurationException, RserveException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);

        final RConnection connection = pool.borrowConnection();
        assertNotNull("Connection should not be null", connection);
        assertTrue("The connection should be connected to the server", connection.isConnected());

        pool.shutdown();
        assertNotNull("Connection pool should never be null", pool);
        assertTrue("Everybody out of the pool!", pool.isClosed());

        assertEquals("There should be no connections", 0, pool.getNumberOfConnections());
        assertEquals("No connections should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("No connections should be idle", 0, pool.getNumberOfIdleConnections());

        assertNotNull("Connection should not be null", connection);
        assertFalse("The connection should not be connected to the server",
                connection.isConnected());

        pool.returnConnection(connection);
    }

    /**
     * Validates that the connections that have been invalidated are removed from the pool properly.
     * @throws ConfigurationException if there is a problem setting up the default test connection
     */
    @Test
    public void invalidateConnection() throws ConfigurationException {
        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(POOL_CONFIGURATION_XML));
        pool = new RConnectionPool(configuration);

        final RConnection connection = pool.borrowConnection();
        assertNotNull("Connection should not be null", connection);
        assertTrue("The connection should be connected to the server", connection.isConnected());

        assertFalse("The pool should be open", pool.isClosed());
        assertEquals("There should be one connection", 1, pool.getNumberOfConnections());
        assertEquals("The connection should active", 1, pool.getNumberOfActiveConnections());
        assertEquals("No connections should be idle", 0, pool.getNumberOfIdleConnections());

        pool.invalidateConnection(connection);

        // the connection should no longer be connected
        assertNotNull("Connection should not be null", connection);
        assertFalse("The connection should not be connected to the server",
                connection.isConnected());

        assertEquals("There should be no connections", 0, pool.getNumberOfConnections());
        assertEquals("No connections should be active", 0, pool.getNumberOfActiveConnections());
        assertEquals("No connections should be idle", 0, pool.getNumberOfIdleConnections());
        assertTrue("The pool should be closed", pool.isClosed());
    }

    /**
     * Checks that two threads actually get the same connection pool.
     * @throws InterruptedException if the threads are interrupted during the test
     */
    @Test
    public void validateSingleton() throws InterruptedException {
        final RConnectionPool[] pools = new RConnectionPool[2];
        final CountDownLatch latch = new CountDownLatch(2);
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        try {
            threadPool.submit(new Callable<Boolean>() {
                public Boolean call() {
                    pools[0] = RConnectionPool.getInstance();
                    latch.countDown();
                    return true;
                }
            });

            threadPool.submit(new Callable<Boolean>() {
                public Boolean call() {
                    pools[1] = RConnectionPool.getInstance();
                    latch.countDown();
                    return true;
                }
            });

            latch.await();

            assertNotNull("Connection pool should never be null", pools[0]);
            assertNotNull("Connection pool should never be null", pools[1]);
            assertEquals("Pools should be the same", pools[0], pools[1]);
        } finally {
            threadPool.shutdown();

            if (pools[0] != null) {
                pools[0].shutdown();
            }

            if (pools[1] != null) {
                pools[1].shutdown();
            }
        }
    }
}