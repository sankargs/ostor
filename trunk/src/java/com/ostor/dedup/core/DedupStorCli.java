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

// Class DedupStorCli - CLI program to test dedup
public class DedupStorCli {
	private static Logger logger = Logger.getLogger(DedupStorCli.class.getName());

	public static void dedupCliHelp() {
		logger.info("help - list of supported commands \n");
		logger.info("show segment <all/segment-id> [<summary/detail>] - " +
				"dump segment(s) information with normal (default), " +
		"summary and detailed information \n");
		logger.info("show object <all/object-id> [<summary/detail>] - " +
				"dump object(s) information with normal (default), " +
		"summary and detailed information \n");
		logger.info("add file <filename> \n");
		logger.info("add dir <dirname> \n");
		logger.info("restore file <filename> <localdir> \n");
		logger.info("restore dir <dirname> <localdir> \n");
		logger.info("\n");
	}

	public static String processCliCommand(DedupStor dstor, String cmd) 
	throws Exception {
		String[] cmdtoks = cmd.split(" ");
		DedupSegmentStor segStor = dstor.getSegmentStor();
		DedupObjectStor objStor = dstor.getObjectStor();
		String cmdSuccess = "success";
		String cmdFailure = "failure";
		String cmdNotUnderstood = "command not understood";

		String resp = cmdSuccess;

		try {
			if(cmdtoks[0].equals("help")) {
				dedupCliHelp();
			} else if(cmdtoks[0].equals("show")) {
				if(cmdtoks.length < 2) {
					resp = cmdNotUnderstood;
					return resp;
				}

				String id = cmdtoks[2];

				boolean all = false;

				if(cmdtoks[2].equals("all")) 
					all = true;

				boolean summary = false;
				boolean detail = false;

				if(cmdtoks.length > 3) {
					if(cmdtoks[3].equals("summary"))
						summary = true;
					else if(cmdtoks[3].equals("detail"))
						detail = true;
				}

				if(cmdtoks[1].equals("segment")) {
					if(all == true)
						segStor.dump(summary, detail);
					else
						segStor.getSegment(id).dump(detail);

				} else if(cmdtoks[1].equals("object")) {
					if(all == true)
						objStor.dump(summary, detail);
					else
						objStor.getObject(id).dump(detail);
				}
			} else if(cmdtoks[0].equals("add")) {
				if(cmdtoks[1].equals("file")) {
					objStor.addFile(cmdtoks[2]);
				} else if(cmdtoks[1].equals("dir")) {
					objStor.addDirectory(cmdtoks[2]);
				} else {
					resp = cmdNotUnderstood;
				}
			} else if(cmdtoks[0].equals("restore")) {
				if(cmdtoks.length < 4) {
					resp = cmdNotUnderstood;
					return resp;
				}

				String file = cmdtoks[2];
				String localdir = cmdtoks[3];

				if(cmdtoks[1].equals("file")) {
					objStor.restoreFile(file, localdir);
				} else if(cmdtoks[1].equals("dir")) {
					objStor.restoreDirectory(file, localdir);
				} else {
					resp = cmdNotUnderstood;
				}
			} else {
				resp = cmdNotUnderstood;
			}
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception while executing command - " +
					cmd, e);
			resp = cmdFailure;
		}

		return resp;
	}

	public static void dedupCliUsage() {
		logger.info("Usage: java DedupStorCli <dstor-dir>\n");
	}

	public static void dedupCli(String[] args) throws Exception {
		if(args.length < 1) {
			dedupCliUsage();
			return;
		}

		String dstorDir = args[0];

		DedupStor dstor = new DedupPersistentStor(dstorDir);

		String clistr = "<dedupstor> ";

		BufferedReader stdin = DedupUtils.setupTermInputReader();
		BufferedWriter stdout = DedupUtils.setupTermOutputWriter();

		DedupUtils.put(stdout, clistr + "Started, enter command - ", true);

		String cmd = DedupUtils.getNextTrimLine(stdin);
		String resp = "";

		while(!cmd.equalsIgnoreCase("done")) {
			DedupUtils.putLine(stdout, clistr + " command - " + cmd, true);

			resp = processCliCommand(dstor, cmd);

			DedupUtils.putLine(stdout, clistr + " response - " + resp, true);

			cmd = DedupUtils.getNextTrimLine(stdin);

			DedupUtils.putLine(stdout, clistr, true);
		}

		DedupUtils.put(stdout, clistr + "Ended", true);  
	}

	public static void main(String[] args) {
		System.out.println("NOTE: Setting up logs from conf file - " + 
				DedupStor.DEFAULT_LOG4J_FILE);
		try {
			PropertyConfigurator.configure(DedupStor.DEFAULT_LOG4J_FILE);
			dedupCli(args);
		}

		catch (Exception e) {
			e.printStackTrace();
			logger.error("Caught exception in dedupCli", e);
		}
	}
}
