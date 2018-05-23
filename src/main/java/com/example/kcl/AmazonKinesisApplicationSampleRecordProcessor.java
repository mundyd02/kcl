/*
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.ParseException;
import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.math.BigInteger;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationSampleRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(AmazonKinesisApplicationSampleRecordProcessor.class);
    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("CountingServiceTest");


    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     * 
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    public static String toISO8601UTC(Date date) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        return df.format(date);
    }

    public static Date fromISO8601UTC(String dateStr) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        try {
            return df.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Process a single record.
     * 
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {
        // TODO Add your own record processing logic here

        String data = null;
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.getData()).toString();

            JsonObject jsonRecord = new Gson().fromJson(data, JsonObject.class);
            JsonElement reportType = jsonRecord.get("REPORTTYPE");
            JsonElement ts = jsonRecord.get("TS");
            JsonElement resourceId = jsonRecord.get("RESOURCEID");
            JsonElement total = jsonRecord.get("TOTAL");
            JsonElement value = jsonRecord.get("VALUE");

            String reportKey = "";

            MessageDigest md5 = null;

            try
            {
                md5 = MessageDigest.getInstance("MD5");
            } catch(NoSuchAlgorithmException e) {

            }

            if (reportType.getAsString().equals("rating-for-id") &&
                    resourceId.getAsString().equals("urn:bbc:sportsdata::recordid:kcl012")) {
                 try {

                     byte[] digest = md5.digest(("rating-for-id/"+ resourceId).getBytes(UTF_8));
                     // Convert to 32-char long String:
                     String reportHash = String.format("%032x", new BigInteger(1, digest));
                     reportKey = reportHash + "-rating-for-id";

                    Item item = table.getItem("ReportKey", reportKey);
                    String recordString = item.getString("ReportData");
                    JsonParser parser = new JsonParser();
                    JsonObject jsonReportExisting = parser.parse(recordString).getAsJsonObject();
                    jsonReportExisting = jsonReportExisting.getAsJsonObject("rated");
                    JsonElement totalRecord = jsonReportExisting.get("total");
                    JsonElement valueRecord = jsonReportExisting.get("value");
                    JsonElement lastUpdatedRecord = jsonReportExisting.get("lastUpdated");

                     /* {"rated":{"total":2,"value":1,"lastUpdated":"2018-03-16T13:40:40.000Z"}}*/
                    Date lastDateRecord = fromISO8601UTC(lastUpdatedRecord.getAsString());
                    Date previousDate = new Date(ts.getAsLong());
                    if (previousDate.after(lastDateRecord)) {
                        JsonObject jsonNew = new JsonObject();
                        jsonNew.addProperty("total", total.getAsInt() + totalRecord.getAsInt());
                        jsonNew.addProperty("value", value.getAsInt() + valueRecord.getAsInt());
                        jsonNew.addProperty("lastUpdated", toISO8601UTC(previousDate));
                        JsonObject jsonMain = new JsonObject();
                        jsonMain.add("rated", jsonNew);

                        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                                .withPrimaryKey("ReportKey", reportKey)
                                .withUpdateExpression("set ReportData = :val")
                                .withValueMap(new ValueMap()
                                        .withString(":val", jsonMain.toString()));

                                /* TODO add conditional version check - retries? */


                        try {
                            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
                            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
                        }
                        catch (Exception e) {
                            System.err.println("Unable to update item: " + reportKey);
                            System.err.println(e.getMessage());
                        }

                    }
                }
                catch (Exception e) {
                    System.err.println("GetItem failed.");
                    System.err.println(e.getMessage());

                    try {
                        JsonObject jsonNew = new JsonObject();
                        jsonNew.addProperty("total", total.getAsInt());
                        jsonNew.addProperty("value", value.getAsInt());
                        jsonNew.addProperty("lastUpdated", toISO8601UTC(new Date(ts.getAsLong())));
                        JsonObject jsonMain = new JsonObject();
                        jsonMain.add("rated", jsonNew);


                        Item item = new Item().withPrimaryKey("ReportKey", reportKey).withString("ReportData", jsonMain.toString()).
                                withString("ResourceId", resourceId.toString()).
                                withString("Version", "11111222");
                        table.putItem(item);

                    }
                    catch (Exception e2) {
                        System.err.println("Create items failed.");
                        System.err.println(e2.getMessage());

                    }
                }
            }

            // Assume this record came from AmazonKinesisSample and log its age.
            long recordCreateTime = new Long(data.substring("testData-".length()));
            long ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime;

            LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data + ", Created "
                    + ageOfRecordInMillis + " milliseconds ago.");
        } catch (NumberFormatException e) {
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (CharacterCodingException e) {
            LOG.error("Malformed data: " + data, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }
}
