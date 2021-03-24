package com.GoogleLLC;

import com.google.api.gax.rpc.ServerStream;
    import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
    import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
    import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
    import com.google.cloud.bigquery.storage.v1beta2.AppendRowsResponse;
    import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
    import com.google.cloud.bigquery.storage.v1beta2.JsonStreamWriter;
    import com.google.cloud.bigquery.storage.v1beta2.TableName;
    import java.io.BufferedReader;
    import java.io.FileReader;
    import java.nio.file.Files;
    import java.nio.file.Paths;
    import java.time.Instant;
    import java.util.ArrayList;
    import java.util.concurrent.ExecutorService;
    import java.util.concurrent.Executors;
    import org.json.JSONObject;
    import java.io.IOException;
    import java.util.ArrayList;
    import java.util.Iterator;
    import java.util.List;
    import java.util.concurrent.ExecutionException;
    import java.util.concurrent.Executor;
    import java.util.logging.Logger;
    import com.google.api.core.ApiFuture;
    import com.google.api.core.ApiFutureCallback;
    import com.google.api.core.ApiFutures;
    import com.google.cloud.ServiceOptions;
    import com.google.cloud.bigquery.BigQuery;
    import com.google.cloud.bigquery.DatasetInfo;
    import com.google.cloud.bigquery.Field;
    import com.google.cloud.bigquery.Field.Mode;
    import com.google.cloud.bigquery.FieldValueList;
    import com.google.cloud.bigquery.Schema;
    import com.google.cloud.bigquery.StandardSQLTypeName;
    import com.google.cloud.bigquery.StandardTableDefinition;
    import com.google.cloud.bigquery.TableId;
    import com.google.cloud.bigquery.TableInfo;
    import com.google.cloud.bigquery.TableResult;
    import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
    import com.google.common.util.concurrent.MoreExecutors;
    import com.google.errorprone.annotations.concurrent.GuardedBy;
    import com.google.protobuf.Descriptors;
    import java.util.concurrent.ExecutionException;
    import java.util.concurrent.Executor;
    import java.util.logging.Logger;
    import org.json.JSONArray;
    import org.json.JSONObject;
    import org.threeten.bp.LocalDateTime;

public class App {
    public enum RowComplexity {
        SIMPLE,
        COMPLEX
    }

    private static final Logger LOG =
        Logger.getLogger(App.class.getName());

    private static String dataset;
    private static BigQueryWriteClient client;
    private static String parentProjectId;
    private static BigQuery bigquery;
    private static int requestLimit = 10000;

    @GuardedBy("this")
    private long inflightCount = 0;

    @GuardedBy("this")
    private long failureCount = 0;

    @GuardedBy("this")
    private long successCount = 0;

    private static JSONObject MakeJsonObject(RowComplexity complexity) throws IOException {
        JSONObject object = new JSONObject();
        // TODO(jstocklass): Add option for testing protobuf format using StreamWriter2
        switch (complexity) {
            case SIMPLE:
                object.put("test_str", "aaa");
                object.put("test_numerics", new JSONArray(new String[] {"1234", "-900000"}));
                object.put("test_datetime", String.valueOf(LocalDateTime.now()));
                break;
            case COMPLEX:
                object.put("test_str", "aaa");
                object.put(
                    "test_numerics1",
                    new JSONArray(
                        new String[] {
                            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                            "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
                            "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
                            "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54",
                            "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
                            "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
                            "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93",
                            "94", "95", "96", "97", "98", "99", "100"
                        }));
                object.put(
                    "test_numerics2",
                    new JSONArray(
                        new String[] {
                            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                            "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
                            "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
                            "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54",
                            "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
                            "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
                            "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93",
                            "94", "95", "96", "97", "98", "99", "100"
                        }));
                object.put(
                    "test_numerics3",
                    new JSONArray(
                        new String[] {
                            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                            "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
                            "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
                            "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54",
                            "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
                            "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
                            "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93",
                            "94", "95", "96", "97", "98", "99", "100"
                        }));
                object.put("test_datetime", String.valueOf(LocalDateTime.now()));
                object.put(
                    "test_bools",
                    new JSONArray(
                        new boolean[] {
                            false, true, false, true, false, true, false, true, false, true, false, true,
                            false, true, false, true, false, true, true, false, true, false, true, false,
                            true, false, true, false, true, false, true, true, false, true, false, true,
                            false, true, false, true, false, true, false, true, true, false, true, false,
                            true, false, true, false, true, false, true, false, true, true, false, true,
                            false, true, false, true, false, true, false, true, false, true, true, false,
                            true, false, true, false, true, false, true, false, true, false, true, true,
                            false, true, false, true, false, true, false, true, false, true, false, true,
                            true, false, true, false, true, false, true, false, true, false, true, false,
                            true, true, false, true, false, true, false, true, false, true, false, true,
                            false, true,
                        }));
                JSONObject sub = new JSONObject();
                sub.put("sub_bool", true);
                sub.put("sub_int", 12);
                sub.put("sub_string", "Test Test Test");
                object.put("test_subs", new JSONArray(new JSONObject[] {sub, sub, sub, sub, sub, sub}));
                break;
            default:
                break;
        }
        return object;
    }

    private static TableInfo MakeSimpleSchemaTable(String name) {
        TableInfo tableInfo =
            TableInfo.newBuilder(
                TableId.of(dataset, name),
                StandardTableDefinition.of(
                    Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                            "test_str", StandardSQLTypeName.STRING)
                            .build(),
                        com.google.cloud.bigquery.Field.newBuilder(
                            "test_numerics", StandardSQLTypeName.NUMERIC)
                            .setMode(Field.Mode.REPEATED)
                            .build(),
                        com.google.cloud.bigquery.Field.newBuilder(
                            "test_datetime", StandardSQLTypeName.DATETIME)
                            .build())))
                .build();
        bigquery.create(tableInfo);
        return tableInfo;
    }

    private static TableInfo MakeComplexSchemaTable(String name) {
        TableInfo tableInfo =
            TableInfo.newBuilder(
                TableId.of(dataset, name),
                StandardTableDefinition.of(
                    Schema.of(
                        Field.newBuilder("test_str", StandardSQLTypeName.STRING).build(),
                        Field.newBuilder("test_numerics1", StandardSQLTypeName.NUMERIC)
                            .setMode(Mode.REPEATED)
                            .build(),
                        Field.newBuilder("test_numerics2", StandardSQLTypeName.NUMERIC)
                            .setMode(Mode.REPEATED)
                            .build(),
                        Field.newBuilder("test_numerics3", StandardSQLTypeName.NUMERIC)
                            .setMode(Mode.REPEATED)
                            .build(),
                        Field.newBuilder("test_datetime", StandardSQLTypeName.DATETIME).build(),
                        Field.newBuilder("test_bools", StandardSQLTypeName.BOOL)
                            .setMode(Mode.REPEATED)
                            .build(),
                        Field.newBuilder(
                            "test_subs",
                            StandardSQLTypeName.STRUCT,
                            Field.of("sub_bool", StandardSQLTypeName.BOOL),
                            Field.of("sub_int", StandardSQLTypeName.INT64),
                            Field.of("sub_string", StandardSQLTypeName.STRING))
                            .setMode(Mode.REPEATED)
                            .build())))
                .build();
        bigquery.create(tableInfo);
        return tableInfo;
    }

    public static void beforeClass() throws IOException {
        parentProjectId = String.format("projects/%s", ServiceOptions.getDefaultProjectId());
        client = BigQueryWriteClient.create();
        RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create();
        bigquery = bigqueryHelper.getOptions().getService();
        dataset = RemoteBigQueryHelper.generateDatasetName();
        DatasetInfo datasetInfo =
            DatasetInfo.newBuilder(/* datasetId = */ dataset).setDescription("BigQuery Write Java long test dataset").build();
        LOG.info("Creating dataset: " + dataset);
        bigquery.create(datasetInfo);
    }

    public static void afterClass() {
        if (client != null) {
            client.close();
        }
        if (bigquery != null && dataset != null) {
            RemoteBigQueryHelper.forceDelete(bigquery, dataset);
            LOG.info("Deleted test dataset: " + dataset);
        }
    }

    public static class Job implements Runnable {
        private String stream;
        private String readerCell;
        private ArrayList<String> output;
        public Job(String stream, String readerCell, ArrayList<String> output) {
            this.stream = stream;
            this.readerCell = readerCell;
            this.output = output;
        }
        public void run() {
            BigQueryReadClient client;
            try {
                client = BigQueryReadClient.create();
            } catch (Exception e) {
                System.exit(1);
                return;
            }

            long firstResponseTime = 0;
            long totalResponseTime = 0;
            long totalResponses = 0;
            long totalBytes = 0;
            long totalRows = 0;

            long requestStart = System.nanoTime();
            ServerStream<ReadRowsResponse> stream =
                client
                    .readRowsCallable()
                    .call(ReadRowsRequest.newBuilder().setReadStream(this.stream).build());
            long responseStart = requestStart;
            for (ReadRowsResponse response : stream) {
                long responseEnd = System.nanoTime();

                if (firstResponseTime == 0) {
                    firstResponseTime = responseEnd - requestStart;
                    totalResponseTime += firstResponseTime;
                } else {
                    totalResponseTime += responseEnd - responseStart;
                }
                totalResponses++;
                if (response.hasAvroRows()) {
                    totalBytes += response.getAvroRows().getSerializedBinaryRows().size();
                } else {
                    totalBytes += response.getArrowRecordBatch().getSerializedRecordBatch().size();
                }
                totalRows += response.getRowCount();

                responseStart = System.nanoTime();
            }

            synchronized (output) {
                output.add(
                    getMetric(
                        "stream_read_time", nanoToSeconds(System.nanoTime() - requestStart),
                        readerCell));
                output.add(
                    getMetric("first_response_time", nanoToSeconds(firstResponseTime), readerCell));
                output.add(
                    getMetric("total_response_time", nanoToSeconds(totalResponseTime), readerCell));
                output.add(
                    getMetric(
                        "bytes_per_second", totalBytes / nanoToSeconds(totalResponseTime),
                        readerCell));
                output.add(
                    getMetric("rows_per_second", totalRows / nanoToSeconds(totalResponseTime),
                        readerCell));
                output.add(
                    getMetric(
                        "responses_per_second",
                        totalResponses / nanoToSeconds(totalResponseTime),
                        readerCell));
            }

            client.shutdownNow();
        }

        public static double nanoToSeconds(long value) {
            return ((double) value) / (1.0e9);
        }

        public static String getMetric(String name, double value, String reader_cell) {
            JSONObject metric = new JSONObject();
            metric.put("name", name);
            metric.put("value", value);
            metric.put("time", Instant.now().getEpochSecond());
            metric.put("reader_cell", reader_cell);
            return metric.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Running main method");
        // Run all of the stress "Testing" here. It's not really testing it's just gathering
        // Data on how the performance goes. Tests that catch breakage should already be going
        // before the code ever reaches here.
        LOG.info("Running before class");
        beforeClass();
        LOG.info("Running after class");
        afterClass();
//        String inputName = "yeet";  //args[0];
//        String outputName = "gang";  //args[1];
//
//        ExecutorService pool = Executors
//            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//
//        ArrayList<String> output = new ArrayList<String>();
//        BufferedReader input = new BufferedReader(new FileReader(inputName));
//        for (String line; (line = input.readLine()) != null; ) {
//            JSONObject obj = new JSONObject(line);
//            Job job = new Job(obj.getString("stream"), obj.getString("reader_cell"), output);
//            pool.submit(job);
//        }
//
//        pool.shutdown();
//        while (!pool.isTerminated()) {
//            Thread.sleep(1000);
//        }
//
//        synchronized (output) {
//            Files.write(Paths.get(outputName), output);
//        }
    }
}