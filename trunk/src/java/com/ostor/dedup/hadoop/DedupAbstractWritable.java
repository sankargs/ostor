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

public abstract class DedupAbstractWritable implements WritableComparable {
	private static Logger logger = Logger.getLogger(DedupAbstractWritable.class.getName());

	private IDedupSerializableComparable val;

	public abstract IDedupSerializableComparable initObject();

	public DedupAbstractWritable() {
		logger.debug("");
		set(initObject());
	}

	public DedupAbstractWritable(IDedupSerializableComparable val) {
		logger.debug("val - " + val);
		set(val);
	}

	public void set(IDedupSerializableComparable val) {
		logger.debug("set - " + val);
		this.val = val;
	}

	public IDedupSerializableComparable get() {
		logger.debug("get - " + val);
		return val;
	}

	public void restoreFromString(String line) throws IOException {
		try {
			val.restoreFromString(line);
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception while restoring hash from - " +
					line);
			throw new IOException("Caught exception while restoring hash from - " + line);
		}
	}

	public void readFields(DataInput in) throws IOException {
		String line = in.readUTF();

		logger.debug("read, total read - " + line.length());

		this.val = initObject();

		restoreFromString(line);

		logger.debug("read - " + val);
	}

	public String dumpToString() throws IOException {
		try {
			return val.dumpToString();
		} 

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception while dumping hash from - " +
					val);
			throw new IOException("Caught exception while dumping hash from - " + val);
		}
	}

	public void write(DataOutput out) throws IOException {
		logger.debug("write - " + val);

		String dumpout = dumpToString();

		out.writeUTF(dumpout);
	}

	public int compareTo(Object obj) {
		IDedupSerializableComparable other = 
			(IDedupSerializableComparable) ((DedupAbstractWritable) obj).get();

		return val.compareTo(other);
	}

	public String toString() {
		//        return val.toString();
		// NOTE -- is this a hadoop bug? I thought 'write' was there
		// to serialize the output
		logger.debug("Call dump to string for now");

		String out = null;

		try {
			out = dumpToString();
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while dumping val - " + e);
		}

		return out;
	}
}

