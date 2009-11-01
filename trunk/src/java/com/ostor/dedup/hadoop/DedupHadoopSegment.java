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

package com.ostor.dedup.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.GzipCodec.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import com.ostor.dedup.core.*;

// Class DedupHadoopSegment - DedipSegment in Hadoop
public class DedupHadoopSegment extends DedupSegment implements IDedupHDFSSerializable {
	private static Logger logger = Logger.getLogger(DedupHadoopSegment.class.getName());

	// default constructor 
	public DedupHadoopSegment() {
		super();
	}

	// constructor
	public DedupHadoopSegment(String id, int len, byte[] data, DedupHash hash) {
		super(id, len, data, hash);
	}

	// copy constructor 
	public DedupHadoopSegment(DedupSegment other) {
		super(other.getId(), other.getLen(), other.getData(), other.getHash(),
				other.getNumRefs());
	}


	public void dumpToHDFS(FileSystem fs, Path path) throws Exception {
		logger.debug("Dump segment - " + getId() + " to path - " + path);

		FSDataOutputStream fdos = fs.create(path);

		fdos.write(dumpMetaData().getBytes());

		fdos.flush();
		fdos.close();

		logger.debug("Dump segment data of length - " + getData().length);

		FSDataOutputStream fdosData = fs.create(new Path(path.toString() + DedupSegmentStor.SERIALIZED_DATA_SUFFIX));

		fdosData.write(getData());

		fdosData.flush();
		fdosData.close();
	}

	public void restoreFromHDFS(FileSystem fs, Path path) throws Exception {
		FSDataInputStream fdis = fs.open(path);

		// NOTE -- need to implement
	}
}