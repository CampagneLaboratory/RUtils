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

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

/**
 * Interface for distributing R configuration items so that a single configuration can be
 * shared among multiple instances.
 */
public interface RConfiguration extends Remote {
    /**
     * Get a configuration for an Rserve instance.
     * @return A populated configuration that represents an Rserve instance.
     * @throws RemoteException if the configuration cannot be accessed
     */
    RConfigurationItem borrowConfigurationItem() throws RemoteException;

    /**
     * Get a configuration for an Rserve instance. If no items are available at the moment,
     * this method will wait up to the specified wait time for a configuration to become available.
     *
     * @param timeout how long to wait before giving up, in units of <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the <tt>timeout</tt> parameter
     * @return A valid object or null if no connection was available within the timeout period
     * @throws RemoteException if the configuration cannot be accessed
     */
    RConfigurationItem borrowConfigurationItem(long timeout, TimeUnit unit) throws RemoteException;

    /**
     * Return a configuration for an Rserve instance to the available list.
     * @param item A populated configuration that represents an Rserve instance.
     * @throws RemoteException if the configuration cannot be stored
     */
    void returnConfigurationItem(RConfigurationItem item) throws RemoteException;
}