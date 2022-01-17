package gcp_lab1;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PubSubToBigQuery {

    /**
     * The logger to output status messages to
     */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    /**
     * side outputs
     */
    private static TupleTag<AccountSchema> validMsgs = new TupleTag<AccountSchema>() {};
    private static TupleTag<String> invalidMsgs = new TupleTag<String>(){};

    /**
     * The Options class provides the custom execution options passed by the executor at the
     * command-line
     */
    public interface Options extends DataflowPipelineOptions {

        @Description("BigQuery table name")
        String getTableName();
        void setTableName(String outputTableName);

        @Description("PubSub Subscription")
        String getSubTopic();
        void setSubTopic(String subscription);

        @Description("DLQ topic")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);

    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);

    }

    /**
     * A Json Validator to separate an input message as valid or invalid message
     */
    static class validJson extends DoFn<String,AccountSchema> {
        @ProcessElement
        public void processElement(@Element String json,ProcessContext processContext)throws Exception{
            try {
                Gson gson = new Gson();
                AccountSchema account = gson.fromJson(json, AccountSchema.class);
                processContext.output(validMsgs,account);
            }catch(Exception e){
                e.printStackTrace();
                processContext.output(invalidMsgs,json);
            }
        }
    }

    // Schema for JSON to row conversion with three different fields and data types
    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    /**
     * This function creates the pipeline to check for the valid or invalid message and
     * write it accordingly to BigQuery Table or DLQ topic
     */
    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        //Read message from pub/sub (subscription)
        PCollectionTuple pubsubMessage = pipeline
                .apply("Read Message From PS-Subscription", PubsubIO.readStrings().fromSubscription(options.getSubTopic()))
                //Filter data into two category (VALID and INVALID)
                .apply("Validator", ParDo.of(new validJson()).withOutputTags(validMsgs, TupleTagList.of(invalidMsgs)));

        //get PCollections for both VALID and INVALID Data
        PCollection<AccountSchema> validData = pubsubMessage.get(validMsgs);
        PCollection<String> invalidData = pubsubMessage.get(invalidMsgs);

        validData.apply("Gson to Json Convertor",ParDo.of(new DoFn<AccountSchema, String>() {
                    @ProcessElement
                    public void convert(ProcessContext context){
                        Gson g = new Gson();
                        String gsonString = g.toJson(context.element());
                        context.output(gsonString);
                    }
                })).apply("Json To Row Convertor",JsonToRow.withSchema(rawSchema)).
                //Write valid message to BigQuery Table
                apply("Write Message To BigQuery Table",BigQueryIO.<Row>write().to(options.getTableName())
                        .useBeamSchema()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        //Write invalid message to DLQ Topic
        invalidData.apply("Send Invalid Message To DLQ",PubsubIO.writeStrings().to(options.getDlqTopic()));

        return pipeline.run();

    }

}