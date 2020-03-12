package options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;


public interface GroupingOptions extends BaseOptions {

    @Validation.Required
    @Default.String(value="")
    String getGroupKey();
    void setGroupKey(String groupKey);
}
