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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * Configuration of a single Rserve instance.
 */
public class RConfigurationItem implements Serializable {
    /**
     * Used during serialization.
     */
    private static final long serialVersionUID = 1L;

    /**
     * @serial The host/ip Rserve is running on.
     */
    private final String host;

    /**
     * @serial The TCP port Rserve is listening on.
     */
    private final int port;

    /**
     * @serial Username to supply for the connection.
     */
    private final String username;

    /**
     * @serial Password to supply for the connection.
     */
    private final String password;

    /**
     * Create a new configuration item for an Rserve process.
     * @param host The host/ip Rserve is running on
     * @param port The TCP port Rserve is listening on
     * @param username Username to supply for the connection
     * @param password Password to supply for the connection
     */
    RConfigurationItem(final String host,
                       final int port,
                       final String username,
                       final String password) {
        super();
        assert StringUtils.isNotBlank(host) : "Host must be provided!";
        this.host = host;
        this.password = password;
        this.port = port;
        this.username = username;
    }

    /**
     * Get the host/ip Rserve is running on.
     * @return the host/ip Rserve is running on
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the TCP port Rserve is listening on.
     * @return port The TCP port Rserve is listening on
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the username to supply for the connection.
     * @return Username to supply for the connection.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Get the password to supply for the connection.
     * @return Password to supply for the connection.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns a string representation of the this configuration.
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("host", host).append("port", port).toString();
    }
}
