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

// Class DedupStor - persistent repository of objects and segments
public class DedupPersistentStor extends DedupStor {

	private static Logger logger = Logger.getLogger(DedupPersistentStor.class.getName());

	private String dStorLoc;
	public static final String DEFAULT_DEDUP_STOR_OBJECTS_LOC_SUFFIX = "objects";
	public static final String DEFAULT_DEDUP_STOR_SEGMENTS_LOC_SUFFIX = "segments";

	public static String getSegmentStorLocation(String dStorLoc) {
		return dStorLoc + System.getProperty("file.separator") + 
		DEFAULT_DEDUP_STOR_SEGMENTS_LOC_SUFFIX;
	}

	public static String getObjectStorLocation(String dStorLoc) {
		return dStorLoc + System.getProperty("file.separator") + 
		DEFAULT_DEDUP_STOR_OBJECTS_LOC_SUFFIX;
	}

	public DedupPersistentStor(String dStorLoc) throws Exception {
		this.dStorLoc = dStorLoc;

		logger.info("Setting dedup stor loc to - " + dStorLoc);

		File dStorDir = new File(dStorLoc);
		File segmentStorDir = new File(getSegmentStorLocation(dStorLoc));
		File objectStorDir = new File(getObjectStorLocation(dStorLoc));

		// create directories if empty
		dStorDir.mkdir();
		segmentStorDir.mkdir();
		objectStorDir.mkdir();

		// first create the segment stor
		DedupPersistentSegmentStor segStor = 
			new DedupPersistentSegmentStor(getSegmentStorLocation(dStorLoc));

		// second create the object stor
		DedupPersistentObjectStor objStor = 
			new DedupPersistentObjectStor(getObjectStorLocation(dStorLoc), segStor);

		setSegmentStor(segStor);

		setObjectStor(objStor);
	}
}