import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;


public class Executor {

    private Options options;
    private Pipeline pipeline;

    public Executor(String[] args) {
        this.options = PipelineOptionsFactory.fromArgs(args)
                                             .withValidation()
                                             .as(Options.class);

        this.pipeline = Pipeline.create(this.options);
    }

    public void run(){
    }

    public static void main(String[] args) {
        Executor executor = new Executor(args);
        executor.run();
    }
}
