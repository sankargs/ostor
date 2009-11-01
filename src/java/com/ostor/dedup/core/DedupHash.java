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

import java.security.MessageDigest;
import org.apache.commons.codec.binary.*;

// Class DedupHash
public class DedupHash implements IDedupSerializableComparable {
	private static Logger logger = Logger.getLogger(DedupHash.class.getName());

	private byte[] hash;
	private String strsha1hash;

	private static final String TOK_SHA1HASH = "SHA1HASH";

	public static byte[] getShaHash(byte[] data, int offset, int len) {
		MessageDigest md = null;
		byte[] sha1hash = null;

		try {
			md = MessageDigest.getInstance("SHA-1");

			md.update(data, offset, len);

			sha1hash = md.digest();
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception in sha1", e);
			return null;
		}

		return sha1hash;
	}

	// default constructor
	public DedupHash() {
	}

	// constructor with data
	public DedupHash(byte[] data) {
		this(data, 0, data.length);
	}

	// constructor with data
	public DedupHash(byte[] data, int offset, int length) {
		hash = getShaHash(data, offset, length);

		Base64 b = new Base64();

		byte[] sha1b64 = b.encodeBase64(hash);

		strsha1hash = new String(sha1b64);
	}

	public byte[] getHashValue() {
		return hash;
	}

	public String getHashStrValue() {
		return strsha1hash;
	}

	public String dumpToString() throws Exception {
		return DedupSerializeUtils.dump(TOK_SHA1HASH, strsha1hash);
	}

	public void restoreFromString(String str) throws Exception {
		strsha1hash = DedupSerializeUtils.restore(str, TOK_SHA1HASH);

		Base64 b = new Base64();

		hash = b.decodeBase64(strsha1hash.getBytes());
	}

	public int compareTo(IDedupComparable obj) {
		DedupHash other = (DedupHash) obj;

		return strsha1hash.compareTo(other.strsha1hash);
	}

	public String toString() {
		return strsha1hash;
	}
}

