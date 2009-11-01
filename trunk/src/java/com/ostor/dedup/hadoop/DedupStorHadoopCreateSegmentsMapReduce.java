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

public class DedupStorHadoopCreateSegmentsMapReduce {
	private static Logger logger = Logger.getLogger(DedupStorHadoopCreateSegmentsMapReduce.class.getName());

	public static class DedupStorHadoopCreateSegmentsMapper extends MapReduceBase implements Mapper<LongWritable, BytesWritable, DedupHashWritable, DedupObjectSegmentCompleteWritable> {
		private DedupStor dStor = null;
		private String objName = null;

		public void configure(JobConf conf) {
			objName = conf.get("map.input.file");

			PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);

			logger.info("In mapper.configure(), processing - " + objName);

			try {
				dStor = new DedupStor();
			}

			catch (Exception e) {
				e.printStackTrace();
				logger.error("Error while creating  stor - " + e);
			}
		}

		public void map(LongWritable key, BytesWritable value, OutputCollector<DedupHashWritable, DedupObjectSegmentCompleteWritable> output, Reporter reporter) throws IOException {
			logger.debug("Mapper.map - key - " + key);

			logger.debug("Value has data of length - " + value.getSize());

			DedupObject obj;

			// data is valid only upto getLength()
			byte[] data = new byte[value.getSize()];

			System.arraycopy(value.get(), 0, data, 0, data.length);

			try {
				obj = new DedupHadoopObject(objName, data, dStor.getSegmentStor());

				dStor.getObjectStor().addObject(obj);
			}

			catch(Exception e) {
				logger.error("Couldn't allocate dedup object for " +
						objName + " key - " + key + " exception - " + e);
				throw new IOException("Couldn't allocate dedup object for " +
						objName + " key - " + key +
						" exception - " + e);
			}

			logger.debug("Processed input, dump object - " + obj);
			obj.dump(false);

			// emit output
			for(DedupObjectSegment objSeg : obj.getSeglist()) {
				DedupSegment segment = 
					dStor.getSegmentStor().getSegment(objSeg.getSegmentId());

				if(segment == null) {
					logger.error("Couldn't find segment by id - " + 
							objSeg.getSegmentId());
					throw new IOException("Couldn't find segment by id - " + 
							objSeg.getSegmentId());
				}

				DedupHashWritable outputKey = 
					new DedupHashWritable(segment.getHash());

				DedupObjectSegmentCompleteWritable outputValue  = 
					new DedupObjectSegmentCompleteWritable(objSeg);

				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class DedupStorHadoopCreateSegmentsReducer extends MapReduceBase implements Reducer<DedupHashWritable, DedupObjectSegmentCompleteWritable, Text, DedupObjectSegmentWritable> {
		private Path segmentStorPath;
		private FileSystem fs;

		public void configure(JobConf conf) {
			try {
				PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);
				fs = FileSystem.get(conf);

				segmentStorPath = new Path(conf.get(DedupStorHadoopUtils.HADOOP_CONF_SEGMENTS_STOR_PATH_KEY));
			}

			catch(Exception e) {
				e.printStackTrace();
				logger.error("Caught exception in reducer.configure()");
			}

			logger.info("In reducer.configure() - segments stor path - " +
					segmentStorPath);
		}

		public void reduce(DedupHashWritable key, Iterator<DedupObjectSegmentCompleteWritable> values, OutputCollector<Text, DedupObjectSegmentWritable> output, Reporter reporter ) throws IOException {
			DedupHadoopSegment seg = null;
			int numRefs = 0;

			logger.debug("Reducer.reduce - key - " + key);

			while(values.hasNext()) {
				numRefs++;

				DedupObjectSegmentCompleteWritable value = values.next();

				DedupObjectSegment objseg = (DedupObjectSegment) value.get();

				logger.debug("Reducer.reduce for obj - " + 
						objseg.getObjectName() + " value - " + value);


				if(seg == null) 
					seg = new DedupHadoopSegment(objseg.getNewSegment());

				Text outputKey = new Text(objseg.getObjectName());

				DedupObjectSegmentWritable outputValue = 
					new DedupObjectSegmentWritable(objseg);

				output.collect(outputKey, outputValue);
			}

			logger.debug("Created dedup segment - " + seg + " write now ");

			try {
				seg.setNumRefs(numRefs);

				// NOTE -- should be a better way to do this?
				// write dedup segment to HDFS
				seg.dumpToHDFS(fs, new Path(segmentStorPath, seg.getId()));
			}

			catch(Exception e) {
				e.printStackTrace();
				logger.error("Couldn't write segment to hdfs - " + 
						seg.getId() + " exception - " + e);
				throw new IOException("Couldn't write segment to hdfs - " + 
						seg.getId() + " exception - " + e);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("NOTE: Setting up logs from conf file - " + 
				DedupStor.DEFAULT_LOG4J_FILE);

		PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);

		JobConf conf = new JobConf(DedupStorHadoopCreateSegmentsMapReduce.class);
		conf.setJobName("dedup-create-segments");

		conf.setMapOutputKeyClass(DedupHashWritable.class);
		conf.setMapOutputValueClass(DedupObjectSegmentCompleteWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DedupObjectSegmentWritable.class);

		conf.setMapperClass(DedupStorHadoopCreateSegmentsMapper.class);
		conf.setReducerClass(DedupStorHadoopCreateSegmentsReducer.class);

		conf.setInputFormat(DedupObjectInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		logger.info("Set input dir - " + args[0]);
		logger.info("Set output dir - " + args[1]);

		Path inputPath = new Path(args[0]);
		Path segmentStorPath = new Path(args[1], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_SEGMENTS_LOC_SUFFIX);
		Path objectMapPath = new Path(args[1], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_TMP_PATH);

		conf.set(DedupStorHadoopUtils.HADOOP_CONF_SEGMENTS_STOR_PATH_KEY, segmentStorPath.toString());
		conf.set(DedupStorHadoopUtils.HADOOP_CONF_OBJECTS_TMP_PATH_KEY, objectMapPath.toString());

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, objectMapPath);

		JobClient.runJob(conf);
	}
}


