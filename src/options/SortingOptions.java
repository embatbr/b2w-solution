package options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;


public interface SortingOptions extends BaseOptions {

    @Validation.Required
    @Default.String(value="")
    String getSortKey();
    void setSortKey(String sortKey);
}
