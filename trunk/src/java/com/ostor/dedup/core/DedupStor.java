/*
 * Created on Oct 29, 2009
 * Created by Praveen Patnala
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 */

package com.ostor.dedup.core;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;

// Class DedupStor - main repository of objects and segments
public class DedupStor {
	public static String DEFAULT_LOG4J_FILE = "ostor.log4j";

	private static Logger logger = Logger.getLogger(DedupStor.class.getName());

	private DedupSegmentStor segStor;
	private DedupObjectStor objStor;

	public DedupStor() throws Exception {
		// first create the segment stor
		segStor = new DedupSegmentStor();

		// second create the object stor
		objStor = new DedupObjectStor(segStor);
	}

	public DedupSegmentStor getSegmentStor() {
		return segStor;
	}

	public void setSegmentStor(DedupSegmentStor segStor) {
		this.segStor = segStor;
	}

	public DedupObjectStor getObjectStor() {
		return objStor;
	}

	public void setObjectStor(DedupObjectStor objStor) {
		this.objStor = objStor;
	}
}