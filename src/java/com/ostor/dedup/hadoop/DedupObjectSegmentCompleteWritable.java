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

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import com.ostor.dedup.core.*;

public class DedupObjectSegmentCompleteWritable extends DedupObjectSegmentWritable {
	private static Logger logger = Logger.getLogger(DedupObjectSegmentCompleteWritable.class.getName());

	private static final String TOK_SEGMENT = "SEGMENT";
	private static final String TOK_SEGMENTDATA = "SEGMENTDATA";

	public DedupObjectSegmentCompleteWritable() {
		super();
	}

	public DedupObjectSegmentCompleteWritable(DedupObjectSegment val) {
		super(val);
	}

	// NOTE -- this is a hack. Need a DedupObjectSegmentComplete class 
	// and just call dump & restore. 

	public void restoreFromString(String line) throws IOException {
		try {
			DedupObjectSegment val = (DedupObjectSegment) get();

			val.restoreFromString(line);

			DedupSegment seg = new DedupSegment();

			seg.restoreMetaData(DedupSerializeUtils.restore(line, TOK_SEGMENT));

			byte[] segmentData = DedupSerializeUtils.restoreToBinary(line, TOK_SEGMENTDATA);

			String encodeddata = DedupSerializeUtils.restore(line, TOK_SEGMENTDATA);

			logger.debug("Encoded data is of size - "+ encodeddata.length());

			seg.setData(segmentData);

			logger.debug("Recovered dedup segment - " + seg + " of length - " +
					segmentData.length);

			val.setNewSegment(seg);
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception while restoring object seg from - " +
					e);
			throw new IOException("Caught exception while restoring object seg from - " + e);
		}
	}

	public String dumpToString() throws IOException {
		try{
			DedupObjectSegment val = (DedupObjectSegment) get();

			logger.debug("Dumping dedup object segment - " + val);

			String dumpout = val.dumpToString();

			logger.debug("Dump dedup segment - " + val.getNewSegment());

			dumpout += DedupSerializeUtils.dump(TOK_SEGMENT, 
					val.getNewSegment().dumpMetaData());

			logger.debug("Dump dedup segment data of length - " + 
					val.getNewSegment().getData().length);

			String dumpdataout =  DedupSerializeUtils.dumpBinary(TOK_SEGMENTDATA,
					val.getNewSegment().getData());

			logger.debug("Data encoded is of size - " + dumpdataout.length());

			dumpout += dumpdataout;

			return dumpout;
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception while writing object seg to - " +
					e);
			throw new IOException("Caught exception while writing object seg to - " + e);
		}
	}
}
