package executors;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.GroupingOptions;
import steps.ExtractionStep;
import steps.GroupingStep;
import steps.LoadStep;


public class PageViewsExecutor {

    private GroupingOptions options;
    private Pipeline pipeline;

    public PageViewsExecutor(String[] args) {
        this.options = PipelineOptionsFactory.fromArgs(args)
                                             .withValidation()
                                             .as(GroupingOptions.class);

        this.pipeline = Pipeline.create(this.options);
    }

    public void run(){
        ExtractionStep extractionStep = new ExtractionStep(this.options, this.pipeline);
        PCollection<JSONObject> jsons = extractionStep.apply();

        GroupingStep groupingStep = new GroupingStep(this.options, jsons);
        PCollection<KV<String, Iterable<JSONObject>>> groupedJsons = groupingStep.apply();

        // TODO order by timestamp (inside each customer group)

        // TODO classify as abandoned or not

        LoadStep loadStep = new LoadStep(this.options, groupedJsons);
        loadStep.apply();

        this.pipeline.run();
    }

    public static void main(String[] args) {
        PageViewsExecutor executor = new PageViewsExecutor(args);
        executor.run();
    }
}
