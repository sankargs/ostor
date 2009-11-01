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

public class DedupStorHadoopCreateObjectsMapReduce {
	private static Logger logger = Logger.getLogger(DedupStorHadoopCreateObjectsMapReduce.class.getName());

	public static class DedupStorHadoopCreateObjectsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DedupObjectSegmentWritable> {
		private static String inputSplitToken = null;
		private static int numInputTokens = 2;

		public void configure(JobConf conf) {
			PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);
			logger.info("In mapper.configure()");
			inputSplitToken = conf.get("mapred.textoutputformat.separator", "\t");
			logger.info("Input split token - " + inputSplitToken);
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DedupObjectSegmentWritable> output, Reporter reporter) throws IOException {
			String input = value.toString();

			logger.debug("Mapper.map - input - " + input);

			String[] tokens = input.split(inputSplitToken);

			if(tokens.length != numInputTokens) {
				logger.error("Input line invalid - " + input);
				throw new IOException("Input line invalid - " + input);
			}

			String objName = tokens[0];

			DedupObjectSegment objSeg = new DedupObjectSegment();

			try {
				objSeg.restoreFromString(tokens[1]);
			}

			catch (Exception e) {
				e.printStackTrace();
				logger.error("Couldn't create object segment from input - " +
						input);
				throw new IOException("Couldn't create object segment from " +
						"input - " + input);
			}

			Text outputKey = new Text(objName);

			DedupObjectSegmentWritable outputValue = 
				new DedupObjectSegmentWritable(objSeg);

			logger.debug("Emit output - " + outputKey + " with value - " + 
					outputValue);

			output.collect(outputKey, outputValue);
		}
	}

	public static class DedupStorHadoopCreateObjectsReducer extends MapReduceBase implements Reducer<Text, DedupObjectSegmentWritable, Text, Text> {
		private Text successText = new Text("success");
		private Path objectStorPath;
		private FileSystem fs;

		public void configure(JobConf conf) {
			try {
				PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);
				fs = FileSystem.get(conf);

				objectStorPath = new Path(conf.get(DedupStorHadoopUtils.HADOOP_CONF_OBJECTS_STOR_PATH_KEY));
			}

			catch(Exception e) {
				e.printStackTrace();
				logger.error("Caught exception in reducer.configure()");
			}

			logger.info("In reducer.configure() - object stor path - " +
					objectStorPath);
		}

		public void reduce(Text key, Iterator<DedupObjectSegmentWritable> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
			String objName = key.toString();

			logger.debug("Process segmnts for object - " + objName);

			List<DedupObjectSegment> seglist = new ArrayList<DedupObjectSegment>();

			while(values.hasNext()) {
				DedupObjectSegmentWritable value =  values.next();

				DedupObjectSegment objseg = (DedupObjectSegment) value.get();

				logger.debug("Process objseg - " + objseg);

				seglist.add(objseg);
			}

			DedupHadoopObject obj = null;

			try {
				obj = new DedupHadoopObject(objName, seglist, 
						new DedupSegmentStor());
			}

			catch(Exception e) {
				e.printStackTrace();
				obj = null;
				logger.error("Exception while allocating object");
			}

			if(obj == null) {
				logger.error("Couldn't create object - " + obj + 
						" from list of segments of size - " + 
						seglist.size());
				throw new IOException("Couldn't create object - " + obj + 
						" from list of segments of size - " + 
						seglist.size());
			}

			logger.debug("Created dedup object - " + obj + " write now ");

			try {
				// NOTE -- should be a better way to do this?
				// write dedup object to HDFS
				obj.dumpToHDFS(fs, new Path(objectStorPath, obj.getSerializedName()));
			}

			catch(Exception e) {
				e.printStackTrace();
				logger.error("Couldn't write object to hdfs - " + obj + 
						" exception - " + e);
				throw new IOException("Couldn't write object to hdfs - " + obj 
						+ " exception - " + e);
			}

			Text outputKey = new Text(obj.getName());

			output.collect(outputKey, successText);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("NOTE: Setting up logs from conf file - " + 
				DedupStor.DEFAULT_LOG4J_FILE);

		PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);

		JobConf conf = new JobConf(DedupStorHadoopCreateObjectsMapReduce.class);
		conf.setJobName("dedup-create-objects");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DedupObjectSegmentWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(DedupStorHadoopCreateObjectsMapper.class);
		conf.setReducerClass(DedupStorHadoopCreateObjectsReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		Path inputPath = new Path(args[0], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_TMP_PATH);
		Path segmentStorPath = new Path(args[0], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_SEGMENTS_LOC_SUFFIX);
		Path objectStorPath = new Path(args[0], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_LOC_SUFFIX);
		Path objectMapPath = new Path(args[0], DedupStorHadoopUtils.DEFAULT_DEDUP_STOR_HADOOP_OBJECTS_TMP_PATH);

		conf.set(DedupStorHadoopUtils.HADOOP_CONF_SEGMENTS_STOR_PATH_KEY, segmentStorPath.toString());
		conf.set(DedupStorHadoopUtils.HADOOP_CONF_OBJECTS_STOR_PATH_KEY, objectStorPath.toString());
		conf.set(DedupStorHadoopUtils.HADOOP_CONF_OBJECTS_TMP_PATH_KEY, objectMapPath.toString());

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, objectStorPath);

		JobClient.runJob(conf);
	}
}

