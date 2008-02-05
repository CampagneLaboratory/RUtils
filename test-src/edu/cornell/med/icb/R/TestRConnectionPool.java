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

import org.apache.commons.configuration.XMLConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.rosuda.REngine.Rserve.RserveException;

public class TestRConnectionPool {
    @Test
    public void getConnection() throws RserveException {
        final RConnectionPool pool = RConnectionPool.getInstance();
        assertNotNull("Connection pool should never be null", pool);
/*
        final RConnection connection = pool.borrowConnection();
        assertNotNull("Connection should not be null", connection);
        assertTrue(connection.isConnected());
*/
    }

    /**
     * Validates that the connection pool will not allow connections to be handed out
     * when no valid servers were set.
     */
    @Test(expected=IllegalStateException.class)
    public void noValidServers() {
        // set up a pool with an empty configuration
        final RConnectionPool pool = new RConnectionPool(new XMLConfiguration());
        assertNotNull("Connection pool should never be null", pool);
        assertTrue("The pool should be not be open", pool.isClosed());
        assertEquals("There should be no connections", 0, pool.getNumberOfConnections());
        assertEquals("There should be no connections", 0, pool.getNumberOfActiveConnections());
        assertEquals("There should be no connections", 0, pool.getNumberOfIdleConnections());

        // if we try to get a connection, we can't - thre are none configured
        pool.borrowConnection();
    }
}