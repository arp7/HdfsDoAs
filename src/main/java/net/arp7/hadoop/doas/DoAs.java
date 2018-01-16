/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.arp7.hadoop.doas;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;


/**
 * Attempt to write a file with and without
 * {@link UserGroupInformation#doAs(PrivilegedAction)}. 
 */
public class DoAs {
  private static final Logger LOG = LoggerFactory.getLogger(DoAs.class);

  public static void main( String[] args)
      throws IOException, URISyntaxException {
    if (args.length != 3) {
      LOG.error("Usage: DoAs <principal> <keytab-file> <output-file>");
      System.exit(1);
    }
    
    DoAs doas = new DoAs(args[0], args[1]);
    boolean success = doas.writeFile(args[2]);
    LOG.info("{}uccessful writing file {} without doAs",
        success ? "S" : "Uns", args[2]);
    
    success = doas.writeFileWithDoAs(args[2]);
    LOG.info("{}uccessful writing file {} with doAs",
        success ? "S" : "Uns", args[2]);
  }

  private final UserGroupInformation ugi;

  private DoAs(String principal, String keytabFile) throws IOException {
    File f = new File(keytabFile);
    if (!f.exists() || !f.canRead()) {
      LOG.error("File {} does not exist or is not readable.", keytabFile);
    }

    this.ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        principal, keytabFile);;
  }

  /**
   * Attempt to write the file to the filesystem without doAs.
   * @return true on success, false on failure.
   */
  private boolean writeFile(String fileName) {
    try (FSDataOutputStream os = FileSystem.get(new URI(fileName),
        new HdfsConfiguration()).create(new Path(fileName))) {
      os.write(new byte[1024]); // Write some zeroes to the file.
      return true;
    } catch (Exception e) {
      LOG.error("Got exception", e);
    }
    return false;
  }

  /**
   * Attempt to write the file to the filesystem with doAs.
   * @return true on success, false on failure.
   */
  private boolean writeFileWithDoAs(final String fileName) {
    return ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        return writeFile(fileName);
      }
    });
  }
}
