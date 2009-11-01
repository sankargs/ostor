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
import java.util.*;

// Class DedupPersistentObjectStor - persistent repository of objects
public class DedupPersistentObjectStor extends DedupObjectStor {
	private static Logger logger = Logger.getLogger(DedupPersistentObjectStor.class.getName());

	private String dedupObjectStorLoc;

	// restore object stor
	private void restoreObjectStor() throws Exception  {
		logger.info("Restore object stor from - " + dedupObjectStorLoc);

		List<File> allfiles = DedupUtils.getDirFiles(dedupObjectStorLoc);

		for(File f : allfiles) {
			String name = f.getName();

			// filter random files
			if(name.startsWith(DedupObjectStor.OBJECT_ID_PREFIX) == false)
				continue;

			logger.debug("Restoring object from file - " + name);

			DedupObject obj = new DedupObject(getSegmentStor());
			obj.restoreFromFile(getObjectFileName(name));
			addObjectToMap(obj);
		}

		logger.info("Restore object stor successfully");
	}

	public String getObjectFileName(String name) {
		return dedupObjectStorLoc + System.getProperty("file.separator") + name;
	}

	// constructor
	public DedupPersistentObjectStor(String dedupObjectStorLoc, DedupSegmentStor segStor)
	throws Exception {
		super(segStor);

		logger.info("Created new object stor");

		this.dedupObjectStorLoc = dedupObjectStorLoc;

		// restore objects
		restoreObjectStor();
	}

	public void addObject(DedupObject obj) throws Exception {
		super.addObject(obj);

		// serialize object to file
		obj.dumpToFile(dedupObjectStorLoc + "/" + obj.getSerializedName());
	}
}
