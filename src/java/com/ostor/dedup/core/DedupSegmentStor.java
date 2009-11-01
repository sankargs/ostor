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

// Class DedupSegmentStor - repository of segments
public class DedupSegmentStor {
	private static Logger logger = Logger.getLogger(DedupSegmentStor.class.getName());

	public static final String SERIALIZED_DATA_SUFFIX = "_data";
	public static final String SEGMENT_ID_PREFIX = "SEGMENT-";

	private HashMap<String, Object> idMap;
	private HashMap<String, Object> hashMap;

	// constructor
	public DedupSegmentStor() throws Exception {
		logger.info("Create new segment stor");

		idMap = new HashMap<String, Object>();
		hashMap = new HashMap<String, Object>();
	}

	// allocate an id for segment
	public static String allocSegId(DedupHash hash) {
		// some way to encode the hash to ascii-friendly strings
		String id = SEGMENT_ID_PREFIX + URLEncoder.encode(new String(hash.getHashValue()));

		return id; 
	}

	public DedupSegment getSegment(String id) {
		return (DedupSegment) idMap.get(id);
	}

	public DedupSegment getSegment(DedupHash hash) {
		return (DedupSegment) hashMap.get(hash.getHashStrValue());
	}

	public void addSegment(DedupSegment seg) throws Exception {
		logger.debug("Adding seg - " + seg.getId());

		if(idMap.get(seg.getId()) != null) {
			throw new Exception("Segment with id - " + seg.getId() + 
			" already exists");
		}

		if(hashMap.get(seg.getHash().getHashStrValue()) != null) {
			throw new Exception("Segment with hash - " + seg.getHash() + 
			" already exists");
		}

		idMap.put(seg.getId(), seg);
		hashMap.put(seg.getHash().getHashStrValue(), seg);
	}

	public void updateSegment(DedupSegment seg) throws Exception {
		logger.debug("Updating seg - " + seg.getId());

		if(idMap.get(seg.getId()) == null) {
			idMap.put(seg.getId(), seg);
		}

		if(hashMap.get(seg.getHash().getHashStrValue()) == null) {
			hashMap.put(seg.getHash().getHashStrValue(), seg);
		}
	}

	public void deleteSegment(DedupSegment seg) throws Exception {
		logger.debug("Deleting seg - " + seg.getId());

		if(seg.getNumRefs() != 0) {
			logger.info("Segment - " + seg.getId() + 
					" non-zero references - " + 
					seg.getNumRefs());
			throw new Exception("Segment - " + seg.getId() + 
					" non-zero references - " + 
					seg.getNumRefs());
		}

		idMap.remove(seg.getId());
		hashMap.remove(seg.getHash().getHashStrValue());
	}

	public void dump(boolean summary, boolean detail) {
		logger.info("Dump segment stor, number of segments - " + idMap.size());

		if(summary == true) 
			return;

		Collection<Object> segcoll = idMap.values();

		for(Object seg : segcoll) {
			((DedupSegment)seg).dump(detail);
		}
	}
}
