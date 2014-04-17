/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ailk.oci.ocnosql.client;

import org.apache.hadoop.util.ProgramDriver;

import com.ailk.oci.ocnosql.client.load.LoadIncrementalHFiles;
import com.ailk.oci.ocnosql.client.load.mutiple.MutipleColumnImportTsv;
import com.ailk.oci.ocnosql.client.load.single.SingleColumnImportTsv;

/**
 * Driver for hbase mapreduce jobs. Select which to run by passing
 * name of job to this main.
 */
public class Driver {
  /**
   * @param args
   * @throws Throwable
   */
  public static void main(String[] args) throws Throwable {
    ProgramDriver pgd = new ProgramDriver();
    pgd.addClass(SingleColumnImportTsv.NAME, SingleColumnImportTsv.class, "Import data as single column in TSV format.");
    pgd.addClass(MutipleColumnImportTsv.NAME, MutipleColumnImportTsv.class, "Import data as mutiple column in TSV format.");
    pgd.addClass(LoadIncrementalHFiles.NAME, LoadIncrementalHFiles.class, "Complete a bulk data load.");
    ProgramDriver.class.getMethod("driver", new Class [] {String[].class}).
      invoke(pgd, new Object[]{args});
  }
}
