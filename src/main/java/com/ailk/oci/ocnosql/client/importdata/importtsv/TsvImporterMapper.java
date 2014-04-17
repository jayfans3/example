/**
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
package com.ailk.oci.ocnosql.client.importdata.importtsv;

import com.ailk.oci.ocnosql.client.importdata.HbasePutUtil;
import com.ailk.oci.ocnosql.client.importdata.TablePutPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

/**
 * Write table content out to files in hdfs.
 */
public class TsvImporterMapper
        extends Mapper<LongWritable, Text, ImmutableBytesPairWritable, Put> {

    /**
     * Timestamp for all inserted rows
     */
    private long ts;

    /**
     * Column seperator
     */
    private String separator;

    /**
     * Should skip bad lines
     */
    private boolean skipBadLines;
    private Counter badLineCount;

    private ImportTsv.TsvParser parser;

    public long getTs() {
        return ts;
    }

    public boolean getSkipBadLines() {
        return skipBadLines;
    }

    public Counter getBadLineCount() {
        return badLineCount;
    }

    public void incrementBadLineCount(int count) {
        this.badLineCount.increment(count);
    }

    /**
     * Handles initializing this class with objects specific to it (i.e., the parser).
     * Common initialization that might be leveraged by a subsclass is done in
     * <code>doSetup</code>. Hence a subclass may choose to override this method
     * and call <code>doSetup</code> as well before handling it's own custom params.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) {
        doSetup(context);

//        Configuration conf = context.getConfiguration();
//
//        parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY),
//                separator);
//        if (parser.getRowKeyColumnIndex() == -1) {
//            throw new RuntimeException("No row key column specified");
//        }
    }

    /**
     * Handles common parameter initialization that a subclass might want to leverage.
     *
     * @param context
     */
    protected void doSetup(Context context) {
        Configuration conf = context.getConfiguration();

        // If a custom separator has been used,
        // decode it back from Base64 encoding.
        separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
        if (separator == null) {
            separator = ImportTsv.DEFAULT_SEPARATOR;
        } else {
            separator = new String(Base64.decode(separator));
        }

        ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, System.currentTimeMillis());

        skipBadLines = context.getConfiguration().getBoolean(
                ImportTsv.SKIP_LINES_CONF_KEY, true);
        badLineCount = context.getCounter("ImportTsv", "Bad Lines");
    }

    /**
     * Convert a line of TSV text into an HBase table row.
     */
    @Override
    public void map(LongWritable offset, Text value,
                    Context context)
            throws IOException {
        try {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            HbasePutUtil util = new HbasePutUtil();
            List<TablePutPair> list = util.createPut(filename, value.toString());
            for (TablePutPair pair : list) {
                Put put = pair.getPut();
                context.write(new ImmutableBytesPairWritable(pair.getTablename(), put.getRow()), put);
            }
        } catch (IllegalArgumentException e) {
            if (skipBadLines) {
                System.err.println(
                        "Bad line at offset: " + offset.get() + ":\n" +
                                e.getMessage());
                incrementBadLineCount(1);
                return;
            } else {
                throw new IOException(e);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
