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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import com.ostor.dedup.core.*;

// Class DedupHadoopObject - DedupObject in Hadoop
public class DedupHadoopObject extends DedupObject implements IDedupHDFSSerializable {
	private static Logger logger = Logger.getLogger(DedupHadoopObject.class.getName());

	// default constructor 
	public DedupHadoopObject() throws Exception {
		super();
	}

	// constructor
	public DedupHadoopObject(String name, byte[] data, 
			DedupSegmentStor segStor) throws Exception {
		super(name, data, segStor);
	}

	// constructor given segment list (assume not sorted)
	public DedupHadoopObject(String name,
			List<DedupObjectSegment> unsortedSeglist,
			DedupSegmentStor segStor) throws Exception {
		super(name, unsortedSeglist, segStor);
	}

	public void dumpToHDFS(FileSystem fs, Path path) throws Exception {
		logger.debug("Dump object - " + getName() + " to path - " + path);

		FSDataOutputStream fdos = fs.create(path);

		fdos.write(dumpToString().getBytes());

		fdos.flush();
		fdos.close();
	}

	public void restoreFromHDFS(FileSystem fs, Path path) throws Exception {
		FSDataInputStream fdis = fs.open(path);

		// NOTE -- need to implement
	}
}