package executors;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.GroupingAndSortingOptions;
import steps.ClassificationStep;
import steps.ExtractionStep;
import steps.GroupingStep;
import steps.LoadingStep;
import steps.SortingStep;


public class PageViewsExecutor {

    private GroupingAndSortingOptions options;
    private Pipeline pipeline;

    public PageViewsExecutor(String[] args) {
        this.options = PipelineOptionsFactory.fromArgs(args)
                                             .withValidation()
                                             .as(GroupingAndSortingOptions.class);

        this.pipeline = Pipeline.create(this.options);
    }

    public void run(){
        ExtractionStep extractionStep = new ExtractionStep(this.options, this.pipeline);
        PCollection<JSONObject> jsons = extractionStep.apply();

        GroupingStep groupingStep = new GroupingStep(this.options, jsons);
        PCollection<JSONArray> groupedJsons = groupingStep.apply();

        SortingStep sortingStep = new SortingStep(this.options, groupedJsons);
        PCollection<JSONArray> sortedGroupedJsons = sortingStep.apply();

        ClassificationStep classificationStep = new ClassificationStep(this.options, sortedGroupedJsons);
        PCollection<JSONArray> classifiedJsons = classificationStep.apply();

        LoadingStep loadingStep = new LoadingStep(this.options, classifiedJsons);
        loadingStep.apply();

        this.pipeline.run();
    }

    public static void main(String[] args) {
        PageViewsExecutor executor = new PageViewsExecutor(args);
        executor.run();
    }
}
