package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;

import options.BaseOptions;


class JsonToStringFn extends DoFn<JSONObject, String> {

    public JsonToStringFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        JSONObject in = context.element();
        String out = in.toJSONString();

        context.output(out);
    }
}


public class LoadStep {

    private BaseOptions options;
    private PCollection<JSONObject> jsons;

    public LoadStep(BaseOptions options, PCollection<JSONObject> jsons) {
        this.options = options;
        this.jsons = jsons;
    }

    public void apply() {
        PCollection<String> lines = jsons.apply(
            ParDo.of(
                new JsonToStringFn()
            )
        );

        lines.apply(
            TextIO.write()
                  .withoutSharding()
                  .to(this.options.getOutputPath())
        );
    }
}
