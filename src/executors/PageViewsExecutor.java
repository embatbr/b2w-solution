package executors;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import options.BaseOptions;


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
    }

    public static void main(String[] args) {
        PageViewsExecutor executor = new PageViewsExecutor(args);
        executor.run();
    }
}
