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

import java.io.*;

// Class DedupStorCmd - command line dedup stor 
public class DedupStorCmd {
	private static Logger logger = Logger.getLogger(DedupStorCmd.class.getName());

	public static void dedupTestUsage() {
		logger.info("Usage: java DedupStor <input> <output>\n");
	}

	public static void dedupTest(String[] args) throws Exception {
		if(args.length < 2) {
			dedupTestUsage();
			return;
		}

		String input = args[0];
		String output = args[1];

		DedupStor dstor = new DedupPersistentStor(output);

		dstor.getObjectStor().addDirectory(input);
	}

	public static void main(String[] args) {
		System.out.println("NOTE: Setting up logs from conf file - " + 
				DedupStor.DEFAULT_LOG4J_FILE);

		try {
			PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);
			dedupTest(args);
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception in dedupCli", e);
		}
	}
}