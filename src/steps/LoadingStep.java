package steps;


import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import options.BaseOptions;


class JsonToStringFn extends DoFn<JSONArray, String> {

    public JsonToStringFn() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        JSONArray in = context.element();
        if(in.size() > 0) {
            JSONObject json = (JSONObject) in.get(0);
            String out = json.toJSONString();

            context.output(out);
        }
    }
}


public class LoadingStep {

    private BaseOptions options;
    private PCollection<JSONArray> sortedGroupedJsons;

    public LoadingStep(BaseOptions options, PCollection<JSONArray> sortedGroupedJsons) {
        this.options = options;
        this.sortedGroupedJsons = sortedGroupedJsons;
    }

    public void apply() {
        PCollection<String> lines = sortedGroupedJsons.apply(
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
