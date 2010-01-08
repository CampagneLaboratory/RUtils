/*
 * Copyright (C) 2008-2010 Institute for Computational Biomedicine,
 *                         Weill Medical College of Cornell University
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Validates operations of the {@link edu.cornell.med.icb.R.RConfigurationItem} class.
 */
public class TestRConfigurationItem {
    /**
     * Validate that the {@link edu.cornell.med.icb.R.RConfigurationItem#equals(Object)} method
     * operates properly.
     */
    @Test
    public void equal() {
        final RConfigurationItem item1 = new RConfigurationItem("localhost", 6311, null, null);
        assertTrue("item should be equal to itself", item1.equals(item1));
        assertFalse("item should not be equal to null", item1.equals(null));

        final RConfigurationItem item2 = new RConfigurationItem("localhost", 6311, "", "");
        assertTrue("items should be equal", item1.equals(item2));
        assertTrue("items should be equal", item2.equals(item1));

        final RConfigurationItem item3 = new RConfigurationItem("localhost", 6312, null, null);
        assertFalse("items should not be equal", item1.equals(item3));
        assertFalse("items should not be equal", item3.equals(item1));

        final RConfigurationItem item4 = new RConfigurationItem("127.0.0.1", 6312, null, null);
        assertFalse("items should not be equal", item1.equals(item4));
        assertFalse("items should not be equal", item4.equals(item1));
        assertFalse("items should not be equal", item3.equals(item4));
        assertFalse("items should not be equal", item4.equals(item3));
    }

    /**
     * Validate that the {@link edu.cornell.med.icb.R.RConfigurationItem#hashCode()} method
     * operates properly.
     */
    @Test
    public void hashCodeEqual() {
        final RConfigurationItem item1 = new RConfigurationItem("localhost", 6311, null, null);
        final RConfigurationItem item2 = new RConfigurationItem("localhost", 6311, "", "");

        assertEquals("hash should be the same", item1.hashCode(), item2.hashCode());

        final RConfigurationItem item3 = new RConfigurationItem("localhost", 6312, null, null);
        assertNotSame("hash should not be the same", item1.hashCode(), item3.hashCode());

        final RConfigurationItem item4 = new RConfigurationItem("127.0.0.1", 6312, null, null);
        assertNotSame("hash should not be the same", item1.hashCode(), item4.hashCode());
        assertNotSame("hash should not be the same", item3.hashCode(), item4.hashCode());
    }
}
