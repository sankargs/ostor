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
import java.io.*;
import java.lang.*;
import java.util.zip.*;

public class DedupUtils {
	private static Logger logger = Logger.getLogger(DedupUtils.class.getName());

	public static final long MAX_FILE_SIZE = 1000000000L;

	public static byte[] readFile(String fileName) throws Exception {
		File fobj = new File(fileName);
		long tmpfilelen = fobj.length();

		if(tmpfilelen > MAX_FILE_SIZE) {
			logger.error("Cannot process file - " + fileName + 
					" of length - " + tmpfilelen +
					" exceeds max supported - " + MAX_FILE_SIZE);
			throw new Exception("Cannot process file - " + fileName + 
					" of length - " + tmpfilelen +
					" exceeds max supported - " + MAX_FILE_SIZE);
		}

		int filelen = (int) tmpfilelen;
		FileInputStream f = new FileInputStream(fileName);
		int FILE_CHUNK_SIZE = 4*1024;
		byte[] arr = new byte[filelen];
		int nRead;
		int len = 0;

		logger.debug("File length is - " + filelen);

		int chunk = (filelen > FILE_CHUNK_SIZE ? FILE_CHUNK_SIZE : filelen);

		while (len < filelen && (nRead=f.read(arr, len, chunk)) != -1) {
			len += nRead;
			chunk = (filelen - len > FILE_CHUNK_SIZE ? FILE_CHUNK_SIZE : 
				filelen - len);
		}

		logger.debug("Data read is - " + len);

		if(len != filelen) {
			logger.error("Data read - " + len + " not the same as " +
					"file length - " + filelen);
			throw new Exception("Data read - " + len + " not the same as " +
					"file length - " + filelen);
		}

		return arr;
	}

	public static String dumpFile(String fileName) throws Exception { 
		return new String(readFile(fileName));
	}

	// helper method to get contents of a directory
	public static List<File> getDirFiles(String path) {
		File dir = new File(path);

		logger.debug("Read list of files from directory - " + path);

		if (!dir.exists() || !dir.isDirectory()) {
			logger.error("Directory is empty - " + path);
			return null;
		}
		else {
			List<File> allfiles = Arrays.asList(dir.listFiles());

			List<File> ret = new ArrayList<File>();

			for(File file : allfiles) {
				if (file.isDirectory() == true)
					continue;

				logger.debug("Adding file to list - " + file.getName());
				ret.add(file);
			}

			logger.info("Return file list of size - " + ret.size());
			return ret;
		}
	} 

	public static void writeFile(String fileName, byte[] data) 
	throws Exception {
		FileOutputStream f = new FileOutputStream(fileName);
		int FILE_CHUNK_SIZE = 4*1024;
		int len = 0;

		while(len < data.length) {
			int nWrite = (FILE_CHUNK_SIZE > (data.length - len) ?
					FILE_CHUNK_SIZE : (data.length - len));
			f.write(data, len, nWrite);
			len += nWrite;
		}

		logger.debug("File written - " + len);
	}

	public static byte[] compressData(byte[] data, int offset, int length)
	throws Exception {
		int MAX_INCREASE = 1024;

		Deflater compresser = new Deflater();
		compresser.setInput(data, offset, length);
		compresser.finish();

		byte[] compdata = new byte[length + MAX_INCREASE];

		// compress all data
		int compdatalen = compresser.deflate(compdata);

		byte[] out = new byte[compdatalen];
		System.arraycopy(compdata, 0, out, 0, compdatalen);

		// return exact data
		return out;
	}

	public static byte[] compressData(byte[] data) throws Exception {
		return compressData(data, 0, data.length);
	}

	public static byte[] decompressData(byte[] data, int offset, int length,
			int maxlength) throws Exception {

		Inflater decompresser = new Inflater();
		decompresser.setInput(data, offset, length);

		byte[] compdata = new byte[maxlength];

		// decompress all data
		int datalen = decompresser.inflate(compdata);

		byte[] out = new byte[datalen];
		System.arraycopy(compdata, 0, out, 0, datalen);

		// return exact data
		return out;
	}

	public static byte[] decompressData(byte[] data, int maxlength) 
	throws Exception {
		return decompressData(data, 0, data.length, maxlength);
	}

	public static BufferedReader setupFileReader(String fileName) 
	throws Exception {
		return new BufferedReader(new FileReader(fileName)); 
	}

	public static BufferedReader setupTermInputReader() 
	throws Exception {
		return new BufferedReader(new InputStreamReader(System.in));
	}

	public static String getNextLine(BufferedReader f) throws Exception {
		return f.readLine();
	}

	public static String getNextTrimLine(BufferedReader f) throws Exception {
		return f.readLine().trim();

	}

	public static void close(BufferedReader in) throws Exception {
		in.close();
	}

	public static BufferedWriter setupFileWriter(String fileName) 
	throws Exception {
		return setupFileWriter(fileName, false);
	}

	public static BufferedWriter setupFileWriter(String fileName, boolean append) 
	throws Exception {
		File file = new File(fileName);

		String parentFile = file.getParent();

		// create directories automatically
		new File(parentFile).mkdirs();

		return new BufferedWriter(new FileWriter(fileName, append)); 
	}

	public static BufferedWriter setupTermOutputWriter() 
	throws Exception {
		return new BufferedWriter(new OutputStreamWriter(System.out));
	}

	public static FileOutputStream setupFileOutputstream(String fileName)
	throws Exception {
		return setupFileOutputstream(fileName, false);
	}

	public static FileOutputStream setupFileOutputstream(String fileName, 
			boolean append) 
	throws Exception {
		File file = new File(fileName);

		String parentFile = file.getParent();

		// create directories automatically
		new File(parentFile).mkdirs();

		return new FileOutputStream(file, append);
	}

	public static void put(BufferedWriter out, String output, boolean flush) 
	throws Exception {
		out.write(output);

		if(flush == true) 
			out.flush();
	} 

	public static void putLine(BufferedWriter out, String output, boolean flush) 
	throws Exception {
		out.write(output + "\n");

		if(flush == true) 
			out.flush();
	} 

	public static void close(BufferedWriter out) throws Exception {
		out.close();
	}

	// recursively list all files under a root directory
	public static List<File> getFileListing(File rootDir) throws Exception {
		List<File> result = new ArrayList<File>();

		File[] children = rootDir.listFiles();

		for(File child : children) {
			result.add(child);

			if(child.isFile() == false) {
				result.addAll(getFileListing(child));
			}
		}

		return result;
	}
}
