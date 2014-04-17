package com.ailk.oci.ocnosql.client.cache;

import java.util.List;

import com.ailk.oci.ocnosql.client.query.ColumnFamily;

public class OciTableRef {

	private String name;
	private String[] rowkey;
	private String startKey;
	private String stopKey;
	private String seperator;
	private String columns; //字段串
	private String inputPath; //输入路径
	private String exportPath; //输出路径
	private String importTmpOutputPath; //临时输出路径
	private String skipBadLine; //是否滤掉过错误行
	private String compressor; //压缩解压类
	private String rowkeyGenerator; //ROWKEY产生算法
    private String rowkeyColumn;
    private String algoColumn;
    private String callback;
    private String rowKeyUnique;
	private List<ColumnFamily> columnFamilies;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String[] getRowkey() {
		return rowkey;
	}
	public void setRowkey(String[] rowkey) {
		this.rowkey = rowkey;
	}
	public String getSeperator() {
		return seperator;
	}
	public void setSeperator(String seperator) {
		this.seperator = seperator;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getInputPath() {
		return inputPath;
	}
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	public String getImportTmpOutputPath() {
		return importTmpOutputPath;
	}
	public void setImportTmpOutputPath(String importTmpOutputPath) {
		this.importTmpOutputPath = importTmpOutputPath;
	}
	public String getSkipBadLine() {
		return skipBadLine;
	}
	public void setSkipBadLine(String skipBadLine) {
		this.skipBadLine = skipBadLine;
	}
	public String getExportPath() {
		return exportPath;
	}
	public void setExportPath(String exportPath) {
		this.exportPath = exportPath;
	}
	public String getCompressor() {
		return compressor;
	}
	public void setCompressor(String compressor) {
		this.compressor = compressor;
	}
	public String getRowkeyGenerator() {
		return rowkeyGenerator;
	}
	public void setRowkeyGenerator(String rowkeyGenerator) {
		this.rowkeyGenerator = rowkeyGenerator;
	}
	public String getStartKey() {
		return startKey;
	}
	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}
	public String getStopKey() {
		return stopKey;
	}
	public void setStopKey(String stopKey) {
		this.stopKey = stopKey;
	}
	public List<ColumnFamily> getColumnFamilies() {
		return columnFamilies;
	}
	public void setColumnFamilies(List<ColumnFamily> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}

    public String getRowkeyColumn() {
        return rowkeyColumn;
    }

    public void setRowkeyColumn(String rowkeyColumn) {
        this.rowkeyColumn = rowkeyColumn;
    }

    public String getAlgoColumn() {
        return algoColumn;
    }

    public void setAlgoColumn(String algoColumn) {
        this.algoColumn = algoColumn;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getRowKeyUnique() {
        return rowKeyUnique;
    }

    public void setRowKeyUnique(String rowKeyUnique) {
        this.rowKeyUnique = rowKeyUnique;
    }
}
