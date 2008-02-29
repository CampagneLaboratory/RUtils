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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

final class RConfigurationServer implements RConfiguration {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RConfigurationServer.class);

    private final BlockingDeque<RConfigurationItem> configs =
            new LinkedBlockingDeque<RConfigurationItem>();

    private RConfigurationServer() {
        super();
        configs.add(new RConfigurationItem("larry", 1234, null, null));
        configs.add(new RConfigurationItem("curly", 42, null, null));
        configs.add(new RConfigurationItem("moe", 6789, null, null));
    }

    public RConfigurationItem borrowConfigurationItem() throws RemoteException {
        final RConfigurationItem item;
        try {
            item = configs.takeFirst();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Giving out: " + item.getHost() + ":" + item.getPort());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException(e.getMessage());
        }
        return item;
    }

    public void returnConfigurationItem(final RConfigurationItem item) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning: " + item.getHost() + ":" + item.getPort());
        }
        configs.add(item);
    }

    public static void main(final String[] args) throws RemoteException {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

        final String name = "RConfiguration";
        final RConfiguration engine = new RConfigurationServer();
        final RConfiguration stub =
                (RConfiguration) UnicastRemoteObject.exportObject(engine, 0);
        final Registry registry = LocateRegistry.getRegistry(2001);
        registry.rebind(name, stub);
        System.out.println("RConfiguration bound");
    }
}
