package com.cstore.transToHive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class exeHiveQL {
	public static void main(String[] args) throws SQLException {

		if (args.length < 2) {
			System.out.print("请输入你要查询的条件：日志级别  日期");
			System.exit(1);
		}

		String type = args[0];
		String date = args[1];

		// 在hive中创建表
		HiveUtil.createTable("create table if not exists loginfo11 ( rdate String,time ARRAY<string>,type STRING,relateclass STRING,information1 STRING,information2 STRING,information3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'");
		// 加载hadoop日志文件，*表示加载所有的日志文件
		HiveUtil.loadDate("load data local inpath '/usr/local/hadoop2/logs/*' overwrite into table loginfo11");
		// 查询有用的信息，这里依据日期和日志级别过滤信息
		// ResultSet res1 =
		// HiveUtil.queryHive("select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo11 where type='ERROR' and rdate='2011-07-29' ");
		ResultSet res1 = HiveUtil
				.queryHive("select rdate,time[0],type,relateclass,information1,information2,information3 from loginfo11 where type='"
						+ type + "' and rdate='" + date + "' ");
		// 查出的信息经过变换后保存到mysql中。
		HiveUtil.hiveTomysql(res1);
		// 最后关闭此次会话的hive连接
		getConnect.closeHive();
		// 关闭mysql连接
		getConnect.closemysql();

	}
}

