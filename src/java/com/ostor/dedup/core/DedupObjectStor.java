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

// Class DedupObjectStor - repository of objects
public class DedupObjectStor {
	private static Logger logger = Logger.getLogger(DedupObjectStor.class.getName());

	public static final String OBJECT_ID_PREFIX = "OBJECT-";

	private TreeMap<String, Object> objMap;
	private DedupSegmentStor segStor;

	// constructor
	public DedupObjectStor(DedupSegmentStor segStor)
	throws Exception {
		logger.info("Create new object stor");

		objMap = new TreeMap<String, Object>();
		this.segStor = segStor;
	}

	public DedupSegmentStor getSegmentStor() {
		return segStor;
	}

	// get an object given name
	public DedupObject getObject(String name) {
		return (DedupObject) objMap.get(name);
	}

	public void addObjectToMap(DedupObject obj) throws Exception {
		if(objMap.get(obj.getName()) != null) {
			throw new Exception("Object with name - " + obj.getName() + 
			" already exists");
		}

		objMap.put(obj.getName(), obj);
	}

	public void addObject(DedupObject obj) throws Exception {
		logger.debug("Adding obj - " + obj.getName());

		addObjectToMap(obj);

		List<DedupObjectSegment> segList = obj.getSeglist();

		// create segments and/or add references
		for(DedupObjectSegment objseg : segList) {
			DedupSegment newseg = objseg.getNewSegment();
			DedupSegment tmpseg = segStor.getSegment(objseg.getSegmentId());

			// NOTE -- should check if newseg is set but segstor already
			// has a segment with that hash

			logger.debug("Search for segment by id in stor - " + 
					objseg.getSegmentId());

			if(tmpseg != null) {
				logger.debug("Segment already exists for offset - " +
						objseg.getOffset());
				tmpseg.addRef();
				segStor.updateSegment(tmpseg);
			} else if(newseg != null) {
				logger.debug("Create new segment");
				newseg.addRef();
				segStor.addSegment(newseg);
			} else {
				logger.error("Adding object - " + obj.getName() +
						" segment with id - " + objseg.getSegmentId() + 
				" not found, no new segment set");
				throw new Exception("Adding object - " + obj.getName() +
						" segment with id - " + objseg.getSegmentId() + 
				" not found, no new segment set");
			}
		}
	}

	public void addFile(String filename) throws Exception {
		logger.debug("Create file now - " + filename);

		byte[] data = DedupUtils.readFile(filename);
		DedupObject obj = new DedupObject(filename, data, segStor);
		addObject(obj);
	}

	public void addDirectory(String dirname) throws Exception {
		logger.info("Create directory now - " + dirname);

		List<File> children = DedupUtils.getFileListing(new File(dirname));

		logger.debug("Got children for dir - " + children.size());

		for(File child : children) {
			if(child.isFile() == false)
				continue;

			logger.debug("Add file - " + child.getPath());
			addFile(child.getPath());
		}
	}

	public void restoreFile(String name, String localdir) throws Exception {
		logger.debug("Restore file - " + name + " to dir - " + localdir);

		DedupObject obj = (DedupObject) objMap.get(name);

		if(obj == null) {
			logger.debug("Object doesn't exist - " + name);
			return;
		}

		String filename = localdir + System.getProperty("file.separator") +
		name;

		obj.generateObjectToFile(filename);

		logger.debug("Restore done");
	}

	public void restoreDirectory(String name, String localdir) throws Exception {
		logger.info("Restore directory - " + name + " to dir - " + localdir);

		SortedMap tailmap = objMap.tailMap(name);

		logger.debug("Tailmap has number of elements - " + tailmap.size());

		Set tailset = tailmap.keySet();

		Iterator it = tailset.iterator();

		while(it.hasNext()) {
			String filename = (String) it.next();

			logger.debug("Restore file - " + filename);

			restoreFile(filename, localdir);
		}
	}

	public void deleteObject(DedupObject obj) {
		logger.debug("Deleting obj - " + obj.getName());

		objMap.remove(obj.getName());
	}

	public void dump(boolean summary, boolean detail) {
		logger.info("Dump object stor, number of object - " + objMap.size());

		if(summary == true) 
			return;

		Collection<Object> objcoll = objMap.values();

		for(Object obj : objcoll) {
			((DedupObject)obj).dump(detail);
		}
	}
}
