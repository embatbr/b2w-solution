package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

import options.BaseOptions;

public class LoadStep {

    private BaseOptions options;
    private PCollection<String> lines;

    public LoadStep(BaseOptions options, PCollection<String> lines) {
        this.options = options;
        this.lines = lines;
    }

    public void apply() {
        lines.apply(
            TextIO.write()
                  .withoutSharding()
                  .to(this.options.getOutputPath())
        );
    }
}
