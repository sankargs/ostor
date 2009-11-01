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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import com.ostor.dedup.core.*;

public class DedupObjectInputFormat extends FileInputFormat {
	private static Logger logger = Logger.getLogger(DedupObjectInputFormat.class.getName());

	public DedupObjectInputFormat() {
		super();
		logger.info("Call super");
	}

	public RecordReader<LongWritable, BytesWritable> 
	getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
	throws IOException {

		logger.info("Dump record split - " + genericSplit);
		logger.info("Total length - " + genericSplit.getLength());

		reporter.setStatus(genericSplit.toString());
		return new BinaryRecordReader(job, (FileSplit) genericSplit);
	}

}
