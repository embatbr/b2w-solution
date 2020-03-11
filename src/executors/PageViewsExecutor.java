package executors;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;

import options.BaseOptions;
import steps.ExtractionStep;
import steps.LoadStep;


public class PageViewsExecutor {

    private BaseOptions options;
    private Pipeline pipeline;

    public PageViewsExecutor(String[] args) {
        this.options = PipelineOptionsFactory.fromArgs(args)
                                             .withValidation()
                                             .as(BaseOptions.class);

        this.pipeline = Pipeline.create(this.options);
    }

    public void run(){
        ExtractionStep extractionStep = new ExtractionStep(this.options, this.pipeline);
        PCollection<JSONObject> jsons = extractionStep.apply();

        // TODO group by customer

        // TODO order by timestamp (inside each customer group)

        // TODO classify as abandoned or not

        LoadStep loadStep = new LoadStep(this.options, jsons);
        loadStep.apply();

        this.pipeline.run();
    }

    public static void main(String[] args) {
        PageViewsExecutor executor = new PageViewsExecutor(args);
        executor.run();
    }
}
