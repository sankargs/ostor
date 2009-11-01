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

public class DedupStorHadoopUtils {
	private static Logger logger = Logger.getLogger(DedupStorHadoopUtils.class.getName());

	public static final String DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_LOC_SUFFIX = "dstor/objects";
	public static final String DEFAULT_DEDUP_STOR_HADOOP_SEGMENTS_LOC_SUFFIX = "dstor/segments";

	public static final String DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_TMP_PATH = "tmp/objects";

	public static final String HADOOP_CONF_SEGMENTS_STOR_PATH_KEY = "conf-segments-path";
	public static final String HADOOP_CONF_OBJECTS_STOR_PATH_KEY = "conf-objects-path";
	public static final String HADOOP_CONF_OBJECTS_TMP_PATH_KEY = "conf-objects-tmp";

	/*
    public static void storeDedupObject(FileSystem fs, Path storLoc, 
					DedupObject obj) throws Exception {

    }

    public static void storeDedupSegment(FileSystem fs, Path storLoc, 
					 DedupSegment seg) throws Exception {

    }
	 */
}