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
import java.io.*;
import java.nio.*;
import org.apache.commons.codec.binary.*;

// Class DedupObject
public class DedupObject implements IDedupFileSerializable,IDedupSerializableComparable {
	private static Logger logger = Logger.getLogger(DedupObject.class.getName());

	private String name;
	private int len;
	private List<DedupObjectSegment> seglist;
	private String serializedName;
	private DedupSegmentStor segStor;

	private static final String TOK_NAME = "NAME";
	private static final String TOK_LEN = "LEN";
	private static final String TOK_SEGLIST = "SEGLIST";
	private static final String TOK_SEG = "SEG";

	// default constructor 
	public DedupObject() {

	}

	// default constructor 
	public DedupObject(DedupSegmentStor segStor) {
		this.segStor = segStor;
	}

	// constructor
	public DedupObject(String name, byte[] data, DedupSegmentStor segStor) 
	throws Exception {
		this.serializedName = getObjectSerializedName(name);
		this.name = name;
		this.segStor = segStor;

		logger.debug("Adding object - " + name + " of length - " + data.length);

		// identify segments, create segments and/or add references to segments
		DedupFingerprint dedupfp = DedupFingerprint.getInstance();

		// identify segments
		List<DedupFingerprintSegment> segs = dedupfp.getSegments(data);

		logger.debug("Created segments for object, num - " + segs.size());

		seglist = new ArrayList<DedupObjectSegment>();

		len = data.length;

		for(DedupFingerprintSegment seg : segs) {
			DedupSegment tmpseg = segStor.getSegment(seg.getHash());

			DedupObjectSegment objseg = null;

			// check if seg already exists in the stor
			if(tmpseg != null) {
				logger.debug("Segment with hash - " + seg.getHash() + 
				"already exists in the stor");
				objseg = new DedupObjectSegment(name,
						tmpseg.getId(), 
						seg.getOffset(),
						seg.getLength(),
						seg.getHash());
			} else {
				// check if seg exists in the current object
				for(DedupObjectSegment tmpobjseg : seglist) {
					if(tmpobjseg.getHash() == seg.getHash()) {
						logger.debug("Segment with hash - " + seg.getHash() + 
						"already exists in the current object");
						objseg =
							new DedupObjectSegment(name,
									tmpobjseg.getSegmentId(),
									seg.getOffset(),
									seg.getLength(),
									seg.getHash());
						break;
					}
				}
			}

			// if segment not found, create a new one
			if(objseg == null) {
				String segId = segStor.allocSegId(seg.getHash());

				byte[] segdata = new byte[seg.getLength()];

				System.arraycopy(data, seg.getOffset(), segdata, 0, seg.getLength());

				DedupSegment newseg = new DedupSegment(segId, 
						seg.getLength(), 
						segdata, 
						seg.getHash());

				logger.debug("Create new segment with id - " + segId);
				objseg =  new DedupObjectSegment(name,
						segId, 
						seg.getOffset(),
						seg.getLength(),
						seg.getHash());

				// set the new segment
				objseg.setNewSegment(newseg);
			}

			logger.debug("Add new object segment");
			seglist.add(objseg);
		}
	}

	// constructor given segment list (assume not sorted)
	public DedupObject(String name, List<DedupObjectSegment> unsortedSeglist,
			DedupSegmentStor segStor) {
		logger.debug("Create object - " + name);

		this.serializedName = getObjectSerializedName(name);
		this.name = name;
		this.segStor = segStor;

		int totalLen = 0;
		List<DedupObjectSegment> sortedSeglist = 
			new ArrayList<DedupObjectSegment>();

		HashMap map = new HashMap();

		for(DedupObjectSegment objSeg : unsortedSeglist) {
			totalLen += objSeg.getLength();
			map.put(new Integer(objSeg.getOffset()), objSeg);
		}

		ArrayList keys = new ArrayList();
		keys.addAll(map.keySet());
		Collections.sort(keys);

		Iterator it = keys.iterator();

		while(it.hasNext()) {
			Integer key = (Integer) it.next();
			DedupObjectSegment objSeg = (DedupObjectSegment) map.get(key);
			sortedSeglist.add(objSeg);
		}

		this.len = totalLen;
		this.seglist = sortedSeglist;
	}

	public static String getObjectSerializedName(String objName) {
		Base64 b = new Base64();

		return (DedupObjectStor.OBJECT_ID_PREFIX + 
				new String(b.encode(objName.getBytes())));
	}

	public String getName() {
		return name;
	}

	public String getSerializedName() {
		return serializedName;
	}

	public List<DedupObjectSegment> getSeglist() {
		return seglist;
	}

	public void deleteObject() {
		// delete segments and/or add references to segments
	}

	public void dumpOut(Writer objout) throws Exception {
		objout.write(DedupSerializeUtils.dump(TOK_NAME, name));
		objout.write(DedupSerializeUtils.dump(TOK_LEN, len));

		String objsegliststr = new String();

		for(DedupObjectSegment objseg : seglist) {
			objsegliststr += DedupSerializeUtils.dump(TOK_SEG, objseg.dumpToString());
		}

		objout.write(DedupSerializeUtils.dump(TOK_SEGLIST, objsegliststr));

		// dummy
		objout.write("");
	}

	public String dumpToString() throws Exception {
		StringWriter bufout = new StringWriter();

		dumpOut(bufout);

		String ret = bufout.toString();

		bufout.flush();
		bufout.close();	

		return ret;
	}

	public void dumpToFile(String fileName) throws Exception {
		logger.debug("Dump to file for object - " + name);

		BufferedWriter fileout = DedupUtils.setupFileWriter(fileName);

		dumpOut(fileout);

		fileout.flush();
		fileout.close();	
	}

	public void restoreFromString(String str) throws Exception {
		name = DedupSerializeUtils.restore(str, TOK_NAME);
		len = DedupSerializeUtils.restoreToInt(str, TOK_LEN);

		String objsegliststr = DedupSerializeUtils.restore(str, TOK_SEGLIST);

		logger.debug("Restore object from dump");

		List<String> objsegstrarr = DedupSerializeUtils.restoreArray(objsegliststr, TOK_SEG);

		seglist = new ArrayList<DedupObjectSegment>();

		for(String objsegstr : objsegstrarr) {
			logger.debug("Restoring object segment - " + objsegstr);

			DedupObjectSegment tmpobjseg = new DedupObjectSegment();
			tmpobjseg.restoreFromString(objsegstr);

			logger.debug("Restored object segment - " + tmpobjseg);

			seglist.add(tmpobjseg);
		}
	}

	public void restoreFromFile(String fileName) throws Exception {
		logger.debug("Restore object from file - " + fileName);
		restoreFromString(DedupUtils.dumpFile(fileName));
	}

	public void generateObjectOut(OutputStream objout) throws Exception {
		for(DedupObjectSegment objseg : seglist) {
			segStor.getSegment(objseg.getSegmentId()).generateSegmentOut(objout);
		}
	}

	public void generateObjectToFile(String fileName) throws Exception {
		logger.debug("GenerateObject to file for object - " + name);

		FileOutputStream fos = DedupUtils.setupFileOutputstream(fileName);

		generateObjectOut(fos);

		fos.close();
	}

	public int compareTo(IDedupComparable obj) {
		DedupObject other = (DedupObject) obj;

		return serializedName.compareTo(getSerializedName());
	}

	public String toString() {
		return "[Object - " + name + "]" + " length - " + len + 
		" num segs - " + seglist.size();
	}

	public void dump(boolean detail) {
		int uniqueSegs = 0;
		int uniqueSegsSize = 0;

		for(DedupObjectSegment seg : seglist) {
			logger.debug("Dump object segment - " + seg);

			DedupSegment segment = segStor.getSegment(seg.getSegmentId());

			if(segment == null) {
				logger.error("Couldn't find segment by id - " + seg.getSegmentId());
				return;
			}

			if(segment.getNumRefs() <= 1) {
				uniqueSegs++;
				uniqueSegsSize += segment.getLen();
			}
		}

		int uniquePercent = (int)(100 * uniqueSegsSize / len);

		logger.info(this + " unique:: segs - (" + uniqueSegs + "/" + 
				seglist.size() + ") size - (" + uniquePercent + "%)");

		if(detail == false)
			return;

		logger.info("Dump segments - ");

		int i = 0;

		for(DedupObjectSegment seg : seglist) {
			logger.info("[" + i + "] " + seg);
			i++;
		}
	}
}
