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

import java.util.*;
import java.net.URLEncoder;
import java.io.*;
import java.lang.*;

// Class DedupSegmentStor - persistent repository of segments
public class DedupPersistentSegmentStor extends DedupSegmentStor {
	private static Logger logger = Logger.getLogger(DedupPersistentSegmentStor.class.getName());

	private String dedupSegmentStorLoc;

	// restore segment stor
	private void restoreSegmentStor() throws Exception  {
		logger.info("Restore segment stor from - " + dedupSegmentStorLoc);

		List<File> allfiles = DedupUtils.getDirFiles(dedupSegmentStorLoc);

		for(File f : allfiles) {
			String name = f.getName();

			// filter random files
			if(name.startsWith(DedupSegmentStor.SEGMENT_ID_PREFIX) == false)
				continue;

			// if it's a data segment file, skip it
			if(name.endsWith(DedupSegmentStor.SERIALIZED_DATA_SUFFIX))
				continue;

			logger.debug("Restoring segment from file - " + name);

			DedupSegment seg = new DedupSegment();
			seg.restoreFromFile(dedupSegmentStorLoc + "/" + name);
			super.addSegment(seg);
		}

		logger.info("Restore segment stor successfully");
	}

	// constructor
	public DedupPersistentSegmentStor(String dedupSegmentStorLoc) 
	throws Exception {
		super();

		logger.info("Created new segment stor");

		this.dedupSegmentStorLoc = dedupSegmentStorLoc;

		// restore segment
		restoreSegmentStor();
	}

	public String getSegmentFileName(DedupSegment seg) {
		return dedupSegmentStorLoc + System.getProperty("file.separator") +
		seg.getId();
	}

	public void addSegment(DedupSegment seg) throws Exception {
		super.addSegment(seg);

		// serialize segment to file
		seg.dumpToFile(getSegmentFileName(seg));
	}

	public void updateSegment(DedupSegment seg) throws Exception {
		logger.debug("Updating seg - " + seg.getId());

		super.updateSegment(seg);

		// serialize segment to file
		seg.dumpToFile(getSegmentFileName(seg));
	}

	public void deleteSegment(DedupSegment seg) throws Exception {
		super.deleteSegment(seg);
	}
}
