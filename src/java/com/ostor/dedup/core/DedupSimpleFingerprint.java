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
import java.nio.*;

public class DedupSimpleFingerprint extends DedupFingerprint {
	private static Logger logger = Logger.getLogger(DedupSimpleFingerprint.class.getName());

	public static long FP_PUSH_FACTOR = (1 << FP_LEN);
	private long sum = 0;

	// get hash 
	public long getHash() {
		return sum;
	}

	// initialize the FP with data at an offset
	public void init(byte[] data, int offset) {
		reset();

		for(int i = 0; i < FP_LEN; ++i)
			sum = (2*sum) + data[offset+i];
	}

	// push (automatically pops oldest one)
	public void push(byte[] data, int offset) {
		sum -= (FP_PUSH_FACTOR * data[offset - FP_LEN - 1]);
		sum = (2*sum) + data[offset];
	}

	// reset the operation
	public void reset() {
		sum = 0;
	}

	// create and add dedup segment
	private void addDedupSegment(List<DedupFingerprintSegment> list,
			byte[] data, int offset, int length) {
		logger.debug("Creating new dedup segment at - " + offset +
				" with length - " + length);

		DedupHash hash = new DedupHash(data, offset, length);

		DedupFingerprintSegment simplefpseg = 
			new DedupSimpleFingerprintSegment(data, offset, length, hash);

		list.add(simplefpseg);
	}

	// check if match found 
	private boolean foundMatch(Long rhash) {
		String SIMPLEFP_MATCH1 = "19560";
		String SIMPLEFP_MATCH2 = "29560";
		String rhashStr = new String(rhash.toString());

		if((rhashStr.endsWith(SIMPLEFP_MATCH1) == true) ||
				(rhashStr.endsWith(SIMPLEFP_MATCH2) == true)) 
			return true;
		else 
			return false;
	}


	public List<DedupFingerprintSegment> getSegments(byte[] data) throws Exception {
		List<DedupFingerprintSegment> ret = new ArrayList<DedupFingerprintSegment>();
		int datalen = data.length;
		int currseglen = 0;
		int currsegindex = -1;
		int i = 0;

		reset();

		while(i < datalen) {
			// if currently looking for a segment
			if(currseglen == 0) {
				logger.debug("Looking for a new segment at - " + i);

				if(datalen - i < MIN_SEGMENT) {
					logger.debug("Reached end of file, last " +
							"segment starts at - " + i + 
							" of len - " + (datalen - i));

					// create and add a new dedup segment 
					addDedupSegment(ret, data, i, datalen - i);

					break;
				}

				logger.debug("Initiated segment at - " + i);
				currsegindex = i;
				currseglen = MIN_SEGMENT;
				i += MIN_SEGMENT;
				init(data, i - FP_LEN);
			} else {
				Long rhash = new Long(getHash());
				String rhashStr = new String(rhash.toString());

				if(foundMatch(rhash) == true ||
						currseglen == MAX_SEGMENT || i == datalen-1) {
					// create and add a new dedup segment 
					addDedupSegment(ret, data, currsegindex, currseglen);

					logger.debug("[FOUND] segment at - " + currsegindex +
							" of length - " + currseglen + 
							" simple hash value " + rhash);

					currsegindex = -1;
					currseglen = 0;
					reset();
					/*
		    if(i == datalen-1)
			break;
					 */
				} else {
					push(data, i);
					currseglen++;
					i++;
				}
			}
		}

		return ret;
	}
}
