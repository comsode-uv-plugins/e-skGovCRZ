package eu.comsode.unifiedviews.plugins.extractor.skgovcrz;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.files.WritableFilesDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;

/**
 * Main data processing unit class.
 */
@DPU.AsExtractor
public class SkGovCRZ extends AbstractDpu<SkGovCRZConfig_V1> {

    private static final Logger LOG = LoggerFactory.getLogger(SkGovCRZ.class);

    private static final String INPUT_URL = "http://www.crz.gov.sk/";

    private static final String defaultPath = "/index.php";

    private static final String CSV_HEADER = "\"Názov zmluvy\";\"Typ zmluvy\";\"Rezort\";\"Objednávate¾\";\"Dodávate¾\";\"IÈO\""
            + ";\"Èíslo zmluvy\";\"Dátum zverejnenia\";\"Dátum uzavretia\";\"Dátum úèinnosti\";\"Dátum platnosti do\""
            + ";\"Celková èiastka\";\"Príloha\";\"Detail\"";

    private String sessionId = null;

    private Map<String, String> keys = new HashMap<String, String>();

    @DataUnit.AsOutput(name = "filesOutput")
    public WritableFilesDataUnit filesOutput;

    public SkGovCRZ() {
        super(SkGovCRZVaadinDialog.class, ConfigHistory.noHistory(SkGovCRZConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        File outputFile = null;
        initializeKeysMap();
        try {
            outputFile = File.createTempFile("____", ".csv", new File(URI.create(filesOutput.getBaseFileURIString())));
        } catch (IOException | DataUnitException ex) {
            throw ContextUtils.dpuException(ctx, ex, "SkMartinDebtors.execute.exception");
        }

        CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
        encoder.onMalformedInput(CodingErrorAction.REPORT);
        encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        int pageCounter = 0;
        try (PrintWriter outputWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, false), encoder))); CloseableHttpClient httpclient = HttpClients.createDefault()) {

            outputWriter.println(CSV_HEADER);
            HttpGet httpGet = new HttpGet(INPUT_URL);
            httpGet.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            httpGet.setHeader("Accept-Encoding", "gzip, deflate");
            httpGet.setHeader("Accept-Language", "en-US,cs;q=0.7,en;q=0.3");
            httpGet.setHeader("Connection", "keep-alive");
            httpGet.setHeader("Host", (new URL(INPUT_URL)).getHost());
            httpGet.setHeader("Referer", INPUT_URL);
            httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0");
            CloseableHttpResponse response1 = httpclient.execute(httpGet);

            LOG.debug(String.format("GET response status line: %s", response1.getStatusLine()));
            int responseCode = response1.getStatusLine().getStatusCode();
            StringBuilder headerSb = new StringBuilder();
            for (Header h : response1.getAllHeaders()) {
                headerSb.append("Key : " + h.getName() + " ,Value : " + h.getValue());
            }
            LOG.debug(headerSb.toString());

            Header[] cookies = response1.getHeaders("Set-Cookie");
            String[] cookieParts = cookies[0].getValue().split("; ");
            sessionId = cookieParts[0];
            String response = null;
            if (responseCode != HttpURLConnection.HTTP_OK) {
                LOG.error("GET request not worked");
                throw new Exception("GET request not worked");
            }
            HttpEntity entity = null;
            try {
                entity = response1.getEntity();
                response = EntityUtils.toString(entity, "UTF-8");
            } finally {
                EntityUtils.consumeQuietly(entity);
                response1.close();
            }
            LOG.debug(String.format("Server response:\n%s", response));
            Document rootDoc = Jsoup.parse(response);

            Element pageFilter = rootDoc.select("input#filterFrm_ID").first();
            if (pageFilter == null) {
                throw new Exception("Error scrapping page. The content of the page is empty.");
            }
            String idKeyName = pageFilter.attr("name");
            String idValue = pageFilter.attr("value");
            URIBuilder builder = new URIBuilder();
            URL url = new URL(INPUT_URL);
            builder.setScheme(url.getProtocol());
            builder.setHost(url.getHost());
            builder.setPath(defaultPath);
            builder.setParameter(idKeyName, idValue);
            builder.setParameter("page", Integer.toString(pageCounter));
            int counter = 0;
            while (true) {
                response = getDetailInfo(httpclient, builder.build());
                LOG.debug(String.format("Server response:\n%s", response));
                Document doc = Jsoup.parse(response);

                Element content = doc.select("table.table_list").first();
                if (content == null) {
                    break;
                }
                Elements trs = content.select("tr");
                boolean tableHeader = true;
                for (Element tr : trs) {
                    if (tableHeader) {
                        tableHeader = false;
                        continue;
                    }
                    Element td = tr.select("td.cell2").first();
                    URIBuilder defaultLinkBuilder = new URIBuilder();
                    defaultLinkBuilder.setScheme(url.getProtocol());
                    defaultLinkBuilder.setHost(url.getHost());
                    defaultLinkBuilder.setPath(defaultPath);
                    for (Map.Entry<String, String> parameter : parseQuery(td.select("a[href]").first().attr("href")).entrySet()) {
                        defaultLinkBuilder.setParameter(parameter.getKey(), parameter.getValue());
                    }
                    String zmluvaDetail = getDetailInfo(httpclient, defaultLinkBuilder.build());
                    Element zmluvaDetailContent = Jsoup.parse(zmluvaDetail);
                    LOG.debug(zmluvaDetailContent.html());
                    Element detailContent = zmluvaDetailContent.select("div#content").first();
                    getDetails(detailContent, url);
                    keys.put("Detail", defaultLinkBuilder.build().toString());
                    outputWriter.println(buildCsvRow());
                    counter++;
                    LOG.info("Done contract {}", counter);
                }
                pageCounter++;
                builder = new URIBuilder();
                builder.setHost(url.getHost()).setScheme(url.getProtocol()).setPath(defaultPath).setParameter(idKeyName, idValue).setParameter("page", Integer.toString(pageCounter));
            }

        } catch (Exception ex) {
            throw ContextUtils.dpuException(ctx, ex, "SkMartinContracts.execute.exception");
        }

    }

    private String normalizeSum(String sum) {
        if (StringUtils.isNotBlank(sum)) {
            String sumToConvert = sum.replaceAll("[^0-9]", "").trim();
            Double result = null;
            try {
                result = Double.parseDouble(sumToConvert);
            } catch (NumberFormatException ex) {
                LOG.error(String.format("Problem converting string %s to Double.", sumToConvert), ex);
            }
            return result.toString();
        } else {
            return "";
        }
    }

    private String getDetailInfo(CloseableHttpClient client, URI url) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        httpGet.setHeader("Accept-Encoding", "gzip, deflate");
        httpGet.setHeader("Accept-Language", "en-US,cs;q=0.7,en;q=0.3");
        httpGet.setHeader("Connection", "keep-alive");
        httpGet.setHeader("Cookie", sessionId);
        httpGet.setHeader("Host", url.getHost());
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0");

        String responseDoc = null;
        try (CloseableHttpResponse response2 = client.execute(httpGet)) {

            LOG.debug("GET Response Code :: " + response2.getStatusLine().getStatusCode());

            LOG.debug("Printing Response Header...\n");
            StringBuilder headerSb = new StringBuilder();
            for (Header h : response2.getAllHeaders()) {
                headerSb.append("Key : " + h.getName() + " ,Value : " + h.getValue());
            }
            LOG.debug(headerSb.toString());
            HttpEntity entity = null;
            try {
                entity = response2.getEntity();
                if (response2.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) { //success
                    responseDoc = EntityUtils.toString(entity, "UTF-8");
                } else {
                    LOG.error("GET request not worked");
                }
            } finally {
                EntityUtils.consumeQuietly(entity);
            }
        }
        return responseDoc;
    }

    private void getDetails(Element detail, URL url) throws Exception {
        initializeKeysMap();
        Element divDates = detail.select("div.area1").first();
        Element datesTable = divDates.select("table").first();
        Elements datesTrs = datesTable.select("tr");
        for (Element tr : datesTrs) {
            String key = tr.select("th").first().text().replaceAll(":", "");
            String value = tr.select("td").first().text();
            if (keys.containsKey(key)) {
                keys.put(key, value);
            } else {
                LOG.warn("Invalid key: " + key);
            }
        }
        Element divPriloha = detail.select("div.area2").first();
        if (divPriloha != null) {
            String prilohaKey = divPriloha.select("h2").first().text().replaceAll(":", "");
            Elements prilohy = divPriloha.select("a[href]");
            if (prilohy.size() > 1) {
                LOG.error(String.format("Zmluva contains more (%d) Priloha than expected!", prilohy.size()));
            } else if (prilohy.size() == 0) {
                if (keys.containsKey(prilohaKey)) {
                    keys.put(prilohaKey, "");
                } else {
                    LOG.warn("Invalid key: " + prilohaKey);
                }
            } else {
                String prilohaValue = prilohy.first().attr("href");
                URI prilohaLink = buildUri(url, prilohaValue);
                if (keys.containsKey(prilohaKey)) {
                    keys.put(prilohaKey, prilohaLink.toString());
                } else {
                    LOG.warn("Invalid key: " + prilohaKey);
                }
            }
        } else {
            LOG.warn("There is no Prilaha in this Zmluva!");
        }
        Element divIdent = detail.select("div.area3").first();
        Element identTable = divIdent.select("table").first();
        Elements identTrs = identTable.select("tr");
        for (Element tr : identTrs) {
            String key = tr.select("th").first().text().replaceAll(":", "");
            String value = tr.select("td").first().text();
            if (keys.containsKey(key)) {
                keys.put(key, value);
            } else {
                LOG.warn("Invalid key: " + key);
            }
        }
        Element divCena = detail.select("div.area4").first();
        Element divCelkCena = divCena.select("div.last").first();
        String cenaKey = divCelkCena.select("strong").first().text().replaceAll(":", "");
        String cenaValue = divCelkCena.select("span").first().text();
        if (keys.containsKey(cenaKey)) {
            keys.put(cenaKey, normalizeSum(cenaValue));
        } else {
            LOG.warn("Invalid key: " + cenaKey);
        }
    }

    private Map<String, String> parseQuery(String queryString) {
        Map<String, String> result = new HashMap<String, String>();
        String query = queryString.substring(queryString.indexOf('?') + 1);
        for (String pair : query.split("&")) {
            int idxOfEqual = pair.indexOf("=");

            if (idxOfEqual < 0) {
                result.put(pair, "");
                continue;
            }

            String key = pair.substring(0, idxOfEqual);
            String value = pair.substring(idxOfEqual + 1);

            result.put(key, value);
        }
        return result;
    }

    private void initializeKeysMap() {
        keys.put("Názov zmluvy", "");
        keys.put("Typ", "");
        keys.put("Rezort", "");
        keys.put("Objednávate¾", "");
        keys.put("Dodávate¾", "");
        keys.put("IÈO", "");
        keys.put("Èíslo zmluvy", "");
        keys.put("Dátum zverejnenia", "");
        keys.put("Dátum uzavretia", "");
        keys.put("Dátum úèinnosti", "");
        keys.put("Dátum platnosti do", "");
        keys.put("Celková èiastka", "");
        keys.put("Príloha", "");
        keys.put("Detail", "");
    }

    private URI buildUri(URL url, String params) {
        URIBuilder defaultLinkBuilder = new URIBuilder();
        defaultLinkBuilder.setScheme(url.getProtocol());
        defaultLinkBuilder.setHost(url.getHost());
        defaultLinkBuilder.setPath(defaultPath);
        for (Map.Entry<String, String> parameter : parseQuery(params).entrySet()) {
            defaultLinkBuilder.setParameter(parameter.getKey(), parameter.getValue());
        }
        URI result = null;
        try {
            result = defaultLinkBuilder.build();
        } catch (URISyntaxException e) {
            LOG.error("Problem generating URI for params: " + params);
        }
        return result;
    }

    private String buildCsvRow() {
        StringBuilder sb = new StringBuilder();
        sb.append("\"").append(keys.get("Názov zmluvy")).append("\";");
        sb.append("\"").append(keys.get("Typ")).append("\";");
        sb.append("\"").append(keys.get("Rezort")).append("\";");
        sb.append("\"").append(keys.get("Objednávate¾")).append("\";");
        sb.append("\"").append(keys.get("Dodávate¾")).append("\";");
        sb.append("\"").append(keys.get("IÈO")).append("\";");
        sb.append("\"").append(keys.get("Èíslo zmluvy")).append("\";");
        sb.append("\"").append(keys.get("Dátum zverejnenia")).append("\";");
        sb.append("\"").append(keys.get("Dátum uzavretia")).append("\";");
        sb.append("\"").append(keys.get("Dátum úèinnosti")).append("\";");
        sb.append("\"").append(keys.get("Dátum platnosti do")).append("\";");
        sb.append("\"").append(keys.get("Celková èiastka")).append("\";");
        sb.append("\"").append(keys.get("Príloha")).append("\";");
        sb.append("\"").append(keys.get("Detail")).append("\"");
        return sb.toString();
    }
}
