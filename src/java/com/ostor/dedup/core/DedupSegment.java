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
import java.util.zip.*;


// class definition for dedup segment
public class DedupSegment implements IDedupFileSerializable {
	private static Logger logger = Logger.getLogger(DedupSegment.class.getName());

	public static String DEFAULT_DEDUP_SEGMENT_ID = "";

	private String id; // id is the hash
	private int len;
	private int numRefs;
	private DedupHash hash;
	private byte[] data;

	private static final String TOK_ID = "ID";
	private static final String TOK_LEN = "LEN";
	private static final String TOK_NUMREFS = "NUMREFS";
	private static final String TOK_HASH = "HASH";

	// default constructor 
	public DedupSegment() {
	}

	// dedup segment constructor
	public DedupSegment(String id, int len, byte[] data, DedupHash hash) {
		this.id = id;
		this.len = len;
		this.data = data;
		this.hash = hash;
	}

	public DedupSegment(String id, int len, byte[] data, DedupHash hash,
			int numRefs) {
		this(id, len, data, hash);
		this.numRefs = numRefs;
	}

	public String getId() {
		return id;
	}

	public DedupHash getHash() {
		return hash;
	}

	public int getNumRefs() {
		return numRefs;
	}

	public int getLen() {
		return len;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public void setNumRefs(int numRefs) {
		this.numRefs = numRefs;
	}

	public void addRef() {
		++numRefs;
	}

	public void deleteReference() {
		--numRefs;

		if(numRefs == 0) {
			logger.debug("Number of refernces for segment is 0");
		}
	}

	private void dumpMetaData(Writer metaout) throws Exception {
		metaout.write(DedupSerializeUtils.dump(TOK_ID, id));
		metaout.write(DedupSerializeUtils.dump(TOK_LEN, len));
		metaout.write(DedupSerializeUtils.dump(TOK_NUMREFS, numRefs));
		metaout.write(DedupSerializeUtils.dump(TOK_HASH, hash.dumpToString()));

		metaout.flush();
		metaout.close();
	}

	public String dumpMetaData() throws Exception {
		StringWriter bufout = new StringWriter();

		dumpMetaData(bufout);

		String ret = bufout.toString();

		bufout.flush();
		bufout.close();	

		return ret;
	}

	public void dumpToFile(String fileName) throws Exception {
		logger.debug("Dump to file, segment - " + id);

		// first write the meta data
		dumpMetaData(DedupUtils.setupFileWriter(fileName));

		// now write the data portion
		FileOutputStream dataout = 
			new FileOutputStream(fileName + DedupSegmentStor.SERIALIZED_DATA_SUFFIX);

		// NOTE -- have a compress option
		dataout.write(data);

		// NOTE -- disable compression for now
		/*
	byte[] compdata = DedupUtils.compressData(data);

	logger.debug("Compress data to a size - " + compdata.length + 
		    " original data length - " + data.length);

	// write compressed data
	dataout.write(compdata);
		 */

		dataout.flush();
		dataout.close();

		logger.debug("Dump to file succeeded");
	}

	public void restoreMetaData(String str) throws Exception {
		id = DedupSerializeUtils.restore(str, TOK_ID);
		len = DedupSerializeUtils.restoreToInt(str, TOK_LEN);
		numRefs = DedupSerializeUtils.restoreToInt(str, TOK_NUMREFS);

		hash = new DedupHash();
		hash.restoreFromString(DedupSerializeUtils.restore(str, TOK_HASH));
	}

	public void restoreFromFile(String fileName) throws Exception {
		logger.debug("Restore from file, segment - " + fileName);

		// first restore the meta data
		restoreMetaData(DedupUtils.dumpFile(fileName));

		// NOTE -- have a compress option
		data = DedupUtils.readFile(fileName + DedupSegmentStor.SERIALIZED_DATA_SUFFIX);

		logger.debug("Read data of length - " + data.length);

		// NOTE -- disable compression for now
		/*
	// now read the data portion
	byte[] compdata = DedupUtils.readFile(fileName + DedupSegmentStor.SERIALIZED_DATA_SUFFIX);

	// decompressed data
	data = DedupUtils.decompressData(compdata, len);

	logger.debug("Compressed data of size - " + compdata.length + 
		    " original data length - " + data.length);
		 */
	}

	public void generateSegmentOut(OutputStream segout) throws Exception {
		segout.write(data);
	}

	public void generateSegmentToFile(String fileName) throws Exception {
		logger.debug("GenerateSegment to file for segment - " + id);

		FileOutputStream fos = DedupUtils.setupFileOutputstream(fileName);

		generateSegmentOut(fos);

		fos.close();
	}


	public String toString() {
		return "Id - " + id + ", len - " + len + ", num refs - " + 
		numRefs + ", hash - " + hash;
	}

	public void dump(boolean detail) {
		logger.info("Dump segment - " + this);
	}
}
