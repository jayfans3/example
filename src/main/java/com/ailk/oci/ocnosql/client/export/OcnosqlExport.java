/**
 * Copyright 2009 The Apache Software Foundation
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
package com.ailk.oci.ocnosql.client.export;

import com.ailk.oci.ocnosql.client.cache.OciTableRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.config.spi.Connection;

public class OcnosqlExport {
	private final static Log log = LogFactory.getLog(OcnosqlExport.class.getSimpleName());
	private static String NAME = "ocnosqlImport";
	private String msg;
	
  public boolean execute(Connection conn, OciTableRef table) throws ClientRuntimeException {
	  //TODO
	  return false;
  }

	
	public static void main(String[] args) {

        OciTableRef table = new OciTableRef();
		table.setName("zhuangyang_test");
		table.setColumns("HBASE_ROW_KEY,f:a");
		table.setSeperator(";");
		table.setExportPath("/zhuangyang/export");
		table.setCompressor("com.ailk.oci.ocnosql.client.compress.HbaseCompressImpl");
		table.setRowkeyGenerator(com.ailk.oci.ocnosql.client.rowkeygenerator.RowKeyGeneratorHolder.TYPE.md5.name());
		
		OcnosqlExport exportor = new OcnosqlExport();
		exportor.execute(Connection.getInstance(), table);
	}
}
