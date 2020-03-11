package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import options.BaseOptions;


class StringToJsonFn extends DoFn<String, JSONObject> {

    public StringToJsonFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws ParseException {
        JSONParser parser = new JSONParser();

        String in = context.element();
        JSONObject out = (JSONObject) parser.parse(in);

        context.output(out);
    }
}


public class ExtractionStep {

    private BaseOptions options;
    private Pipeline pipeline;

    public ExtractionStep(BaseOptions options, Pipeline pipeline) {
        this.options = options;
        this.pipeline = pipeline;
    }

    public PCollection<JSONObject> apply() {
        PCollection<String> lines = this.pipeline.apply(
            TextIO.read()
                  .from(this.options.getInputPath())
        );

        return lines.apply(
            ParDo.of(
                new StringToJsonFn()
            )
        );
    }
}
