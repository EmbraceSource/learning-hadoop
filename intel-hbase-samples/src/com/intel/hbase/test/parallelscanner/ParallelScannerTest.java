package com.intel.hbase.test.parallelscanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.intel.hbase.test.util.TimeCounter;

public class ParallelScannerTest {

    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            throw new Exception("Table name not specified.");
        }
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, args[0]);
        String startKey = args[1];
        String stopKey = args[2];
        boolean isParallel = Boolean.parseBoolean(args[3]);
        String familyName = args[4];
        String columnName = args[5];
        String remainder = args[6];

        System.out.println("++ Parallel Scanning : " + isParallel + " ++");

        TimeCounter executeTimer = new TimeCounter();
        executeTimer.begin();
        executeTimer.enter();

        Scan scan = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
        int count = 0;
        if (isParallel) {
            scan.setParallel(true);
        }
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes(familyName),
                Bytes.toBytes(columnName), CompareOp.LESS, Bytes
                        .toBytes(remainder)));
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        while (r != null) {
            count++;
            r = scanner.next();
        }
        System.out
                .println("++ Scanning finished with count : " + count + " ++");
        scanner.close();

        executeTimer.leave();
        executeTimer.end();
        System.out.println("++ Time cost for scanning: "
                + executeTimer.getTimeString() + " ++");
    }
}
