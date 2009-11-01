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
import org.apache.commons.codec.binary.*;

public class DedupSerializeUtils {
	private static Logger logger = Logger.getLogger(DedupSerializeUtils.class.getName());

	public static String beginToken(String token) {
		return "<" + token + ">";
	}

	public static String endToken(String token) {
		return "</" + token + ">";
	}

	public static String dump(String token, String value) {
		return beginToken(token) + value + endToken(token);
	}

	public static String dump(String token, int value) {
		return beginToken(token) + (new Integer(value)).toString() + endToken(token);
	}

	public static String dump(String token, long value) {
		return beginToken(token) + (new Long(value)).toString() + endToken(token);
	}

	public static String dumpBinary(String token, byte[] data) {
		Base64 b = new Base64();

		logger.debug("Got data of size - " + data.length + 
				" encoded data is of size - " + b.encode(data).length);

		return beginToken(token) + new String(b.encode(data)) + endToken(token);
	}

	public static String restore(String dump, String token) throws Exception {
		String begstr = beginToken(token);
		String endstr = endToken(token);
		int begIndex = dump.indexOf(begstr);
		int endIndex = dump.indexOf(endstr);

		if(begIndex == -1 || endIndex == -1) {
			throw new Exception("Couldn't find token - " + token);
		}

		begIndex += begstr.length();

		return dump.substring(begIndex, endIndex);
	}

	public static int restoreToInt(String dump, String token) throws Exception {
		return (new Integer(restore(dump, token))).intValue();
	}

	public static long restoreToLong(String dump, String token) throws Exception {
		return (new Long(restore(dump, token))).longValue();
	}

	public static byte[] restoreToBinary(String dump, String token) throws Exception {
		String encodedData = restore(dump, token);

		Base64 b = new Base64();

		return b.decode(encodedData.getBytes());
	}

	public static List<String> restoreArray(String dump, String token) {
		List<String> ret = new ArrayList<String>();
		String begstr = beginToken(token);
		String endstr = endToken(token);
		int begIndex = 0;
		int endIndex = 0;

		logger.debug("Restore array of token - " + token);

		while(true) {
			begIndex = dump.indexOf(begstr, begIndex);
			endIndex = dump.indexOf(endstr, endIndex);

			logger.trace("Found item at - " + begIndex + " to - " + endIndex);

			if(begIndex == -1 || endIndex == -1)
				break;

			begIndex += begstr.length();

			ret.add(dump.substring(begIndex, endIndex));

			endIndex += endstr.length();
		}

		return ret;
	}
}

