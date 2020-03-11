package options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;
// import org.apache.beam.sdk.options.ValueProvider;


public interface BaseOptions extends PipelineOptions {

    @Validation.Required
    @Default.String(value="")
    String getInputPath();
    void setInputPath(String inputPath);

    @Validation.Required
    @Default.String(value="")
    String getOutputPath();
    void setOutputPath(String outputPath);
}