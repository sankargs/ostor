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

public class BinaryRecordReader implements RecordReader<LongWritable, BytesWritable> {
	private static Logger logger = Logger.getLogger(BinaryRecordReader.class.getName());

	private long start = 0;
	private int length = 0;
	private byte[] buffer = null;
	private boolean written = false;

	public BinaryRecordReader(JobConf conf, FileSplit split)
	throws IOException {
		start = split.getStart();
		length = (int) split.getLength();

		final Path file = split.getPath();

		logger.debug("Reading input - " + file + " start - " + start +
				" of length - " + length);

		// open the file 
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());
		buffer = new byte[length];
		fileIn.readFully(0, buffer);

		logger.trace("Dump buffer - " + new String(buffer));
	}

	public void close() {

	}

	public LongWritable createKey() {
		return new LongWritable(start);
	}

	public BytesWritable createValue() {
		return new BytesWritable();
	}

	public long getPos() {
		return start;
	}

	public float getProgress() {
		if(written == true)
			return 1;
		else 
			return 0;
	}

	public boolean next(LongWritable key, BytesWritable value) {
		if(written == true)
			return false;

		logger.debug("Setting value of length - " + buffer.length);
		value.set(buffer, 0, buffer.length);
		written = true;
		return true;
	}
}
