package eu.comsode.unifiedviews.plugins.extractor.skgovcrz;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

/**
 * Vaadin configuration dialog .
 */
public class SkGovCRZVaadinDialog extends AbstractDialog<SkGovCRZConfig_V1> {

    private static final long serialVersionUID = 5654427105244031516L;

    public SkGovCRZVaadinDialog() {
        super(SkGovCRZ.class);
    }

    @Override
    public void setConfiguration(SkGovCRZConfig_V1 c) throws DPUConfigException {

    }

    @Override
    public SkGovCRZConfig_V1 getConfiguration() throws DPUConfigException {
        final SkGovCRZConfig_V1 c = new SkGovCRZConfig_V1();

        return c;
    }

    @Override
    public void buildDialogLayout() {
    }

}
