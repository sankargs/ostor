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

// class definition for dedup fingerprint segment
public abstract class DedupFingerprintSegment {
	private static Logger logger = Logger.getLogger(DedupFingerprintSegment.class.getName());

	private byte[] data;
	private int offset;
	private int length;
	private DedupHash hash;

	public DedupFingerprintSegment(byte[] data, int offset, int length,
			DedupHash hash) {
		this.data = data;
		this.offset = offset;
		this.length = length;
		this.hash = hash;
	}

	public byte[] getData() {
		return data;
	}

	public int getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}

	public DedupHash getHash() {
		return hash;
	}
}

