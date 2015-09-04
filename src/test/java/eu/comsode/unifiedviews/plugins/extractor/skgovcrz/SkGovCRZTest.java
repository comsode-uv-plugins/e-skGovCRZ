package eu.comsode.unifiedviews.plugins.extractor.skgovcrz;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.junit.Test;

import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.helpers.dpu.test.config.ConfigurationBuilder;

public class SkGovCRZTest {
    @Test
    public void test() throws Exception {
        SkGovCRZConfig_V1 config = new SkGovCRZConfig_V1();

        // Prepare DPU.
        SkGovCRZ dpu = new SkGovCRZ();
        dpu.configure((new ConfigurationBuilder()).setDpuConfiguration(config).toString());

        // Prepare test environment.
        TestEnvironment environment = new TestEnvironment();
        CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
        encoder.onMalformedInput(CodingErrorAction.REPORT);
        encoder.onUnmappableCharacter(CodingErrorAction.REPORT);

        // Prepare data unit.
        WritableRDFDataUnit rdfOutput = environment.createRdfOutput("rdfOutput", false);
        try {
            // Run.
            environment.run(dpu);

        } finally {
            // Release resources.
            environment.release();
        }
    }
}
