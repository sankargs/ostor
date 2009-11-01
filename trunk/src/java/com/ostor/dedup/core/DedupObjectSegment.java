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
import java.nio.*;
import java.io.*;

// class definition for dedup fingerprint segment
public class DedupObjectSegment implements IDedupSerializableComparable {
	private static Logger logger = Logger.getLogger(DedupObjectSegment.class.getName());

	private String objName;
	private String segId;
	private int offset;
	private int length;
	private DedupHash hash;
	private DedupSegment newSegment; // set if a new segment is being added

	private static final String TOK_OBJNAME = "OBJNAME";
	private static final String TOK_SEGID = "SEGID";
	private static final String TOK_OFFSET = "OFFSET";
	private static final String TOK_LEN = "LEN";
	private static final String TOK_HASH = "HASH";

	// default constructor 
	public DedupObjectSegment() {
	}

	// constructor
	public DedupObjectSegment(String objName, String segId, int offset, 
			int length, DedupHash hash) {
		this.objName = objName;
		this.segId = segId;
		this.offset = offset;
		this.length = length;
		this.hash = hash;
	}

	public DedupSegment getNewSegment() {
		return newSegment;
	}

	public void setNewSegment(DedupSegment newSegment) {
		this.newSegment = newSegment;
	}

	public String getObjectName() {
		return objName;
	}

	public String getSegmentId() {
		return segId;
	}

	public int getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}

	public DedupHash getHash() {
		return hash;
	}

	public String dumpToString() throws Exception {
		String buf = new String();

		buf += DedupSerializeUtils.dump(TOK_OBJNAME, objName);
		buf += DedupSerializeUtils.dump(TOK_SEGID, segId);
		buf += DedupSerializeUtils.dump(TOK_OFFSET, offset);
		buf += DedupSerializeUtils.dump(TOK_LEN, length);
		buf += DedupSerializeUtils.dump(TOK_HASH, hash.dumpToString());

		return buf;
	}

	public void restoreFromString(String str) throws Exception {
		objName = DedupSerializeUtils.restore(str, TOK_OBJNAME);
		segId = DedupSerializeUtils.restore(str, TOK_SEGID);
		offset = DedupSerializeUtils.restoreToInt(str, TOK_OFFSET);
		length = DedupSerializeUtils.restoreToInt(str, TOK_LEN);
		hash = new DedupHash();
		hash.restoreFromString(DedupSerializeUtils.restore(str, TOK_HASH));
	}

	public int compareTo(IDedupComparable obj) {
		DedupObjectSegment other = (DedupObjectSegment) obj;

		return hash.compareTo(getHash());
	}

	public String toString() {
		return "[obj - " + objName + " segment " + segId + " at - " + offset + 
		" - len - " + length + " hash - " + hash + " ]";
	}
}
