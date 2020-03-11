package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import options.BaseOptions;

public class ExtractionStep {

    private BaseOptions options;
    private Pipeline pipeline;

    public ExtractionStep(BaseOptions options, Pipeline pipeline) {
        this.options = options;
        this.pipeline = pipeline;
    }

    public PCollection<String> apply() {
        PCollection<String> lines = this.pipeline.apply(
            TextIO.read()
                  .from(this.options.getInputPath())
        );

        return lines;
    }
}
