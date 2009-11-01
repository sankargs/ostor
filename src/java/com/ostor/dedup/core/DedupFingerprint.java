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

import java.util.*;
import java.nio.*;

// Abtsract Class DedupFingerprint
public abstract class DedupFingerprint {
	public static int MIN_SEGMENT = 2*1024;
	public static int MAX_SEGMENT = 30*1024;
	public static int FP_LEN = 48;

	abstract public List<DedupFingerprintSegment> getSegments(byte[] data) throws Exception;

	public static DedupFingerprint getInstance() {
		// factory method to use the type of fingerprint algorithm

		return new DedupSimpleFingerprint();
	}
}
