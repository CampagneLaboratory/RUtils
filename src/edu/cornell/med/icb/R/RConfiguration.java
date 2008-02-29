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
     * Return a configuration for an Rserve instance to the available list.
     * @param item A populated configuration that represents an Rserve instance.
     * @throws RemoteException if the configuration cannot be stored
     */
    void returnConfigurationItem(RConfigurationItem item) throws RemoteException;
}