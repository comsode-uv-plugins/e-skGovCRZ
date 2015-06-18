package eu.comsode.unifiedviews.plugins.extractor.skgovcrz;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;
import eu.unifiedviews.helpers.dpu.rdf.EntityBuilder;

/**
 * Main data processing unit class.
 */
@DPU.AsExtractor
public class SkGovCRZ extends AbstractDpu<SkGovCRZConfig_V1> {

    private static final Logger LOG = LoggerFactory.getLogger(SkGovCRZ.class);

    private static final String INPUT_URL = "http://www.crz.gov.sk/";

    private static final String BASE_URI = "http://localhost/";

    private static final String PURL_URI = "http://purl.org/procurement/public-contracts#";

    private static final String defaultPath = "/index.php";

    private String sessionId = null;

    private Map<String, Integer> keys = new HashMap<String, Integer>();

    @DataUnit.AsOutput(name = "rdfOutput")
    public WritableRDFDataUnit rdfOutput;

    public SkGovCRZ() {
        super(SkGovCRZVaadinDialog.class, ConfigHistory.noHistory(SkGovCRZConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        initializeKeysMap();
        RepositoryConnection connection = null;
        int pageCounter = 0;
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            org.openrdf.model.URI graph = rdfOutput.addNewDataGraph("skGovCRZRdfData");
            connection = rdfOutput.getConnection();
            ValueFactory vf = ValueFactoryImpl.getInstance();

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
            int contractCounter = 0;
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
                    URI detailURI = buildUri(url, td.select("a[href]").first().attr("href"));
                    String zmluvaDetail = getDetailInfo(httpclient, detailURI);
                    Element zmluvaDetailContent = Jsoup.parse(zmluvaDetail);
                    LOG.debug(zmluvaDetailContent.html());
                    Element detailContent = zmluvaDetailContent.select("div#content").first();

                    UUID uuid = UUID.randomUUID();
                    org.openrdf.model.URI uri = vf.createURI(BASE_URI + uuid.toString());
                    EntityBuilder eb = new EntityBuilder(uri, vf);
                    eb.property(RDF.TYPE, vf.createURI(PURL_URI));
                    eb.property(vf.createURI(BASE_URI + "linka-na-detail"), detailURI.toString());
                    eb = getDetails(detailContent, eb, vf, httpclient, url);
                    keys.put("linka-na-detail", keys.get("linka-na-detail") + 1);

                    connection.add(eb.asStatements(), graph);
                    contractCounter++;
                    LOG.debug("Number of scrapped projects: " + Integer.toString(contractCounter));
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

    private EntityBuilder getDetails(Element detail, EntityBuilder eb, ValueFactory vf, CloseableHttpClient httpClient, URL url) throws Exception {
        initializeKeysMap();
        Element divDates = detail.select("div.area1").first();
        Element datesTable = divDates.select("table").first();
        Elements datesTrs = datesTable.select("tr");
        for (Element tr : datesTrs) {
            String key = slugify(tr.select("th").first().text());
            String value = tr.select("td").first().text();
            if (keys.containsKey(key)) {
                keys.put(key, keys.get(key) + 1);
            } else {
                LOG.warn("Invalid key: " + key);
            }
            eb.property(vf.createURI(BASE_URI + key), value);
        }
        Element divPriloha = detail.select("div.area2").first();
        if (divPriloha != null) {
            String prilohaKey = slugify(divPriloha.select("h2").first().text());
            Elements prilohy = divPriloha.select("a[href]");
            for (Element priloha : prilohy) {
                String prilohaValue = priloha.attr("href");
                URI prilohaLink = buildUri(url, prilohaValue);
                if (keys.containsKey(prilohaKey)) {
                    keys.put(prilohaKey, keys.get(prilohaKey) + 1);
                } else {
                    LOG.warn("Invalid key: " + prilohaKey);
                }
                eb.property(vf.createURI(BASE_URI + prilohaKey), prilohaLink.toString());
            }
        } else {
            LOG.warn("There is no attachment to this contract!");
        }
        Element divIdent = detail.select("div.area3").first();
        Element identTable = divIdent.select("table").first();
        Elements identTrs = identTable.select("tr");
        for (Element tr : identTrs) {
            String key = slugify(tr.select("th").first().text());
            String value = tr.select("td").first().text();
            if (keys.containsKey(key)) {
                keys.put(key, keys.get(key) + 1);
            } else {
                LOG.warn("Invalid key: " + key);
            }
            eb.property(vf.createURI(BASE_URI + key), value);
        }
        Element divCena = detail.select("div.area4").first();
        Element divCelkCena = divCena.select("div.last").first();
        String cenaKey = slugify(divCelkCena.select("strong").first().text());
        String cenaValue = divCelkCena.select("span").first().text();
        if (keys.containsKey(cenaKey)) {
            keys.put(cenaKey, keys.get(cenaKey) + 1);
        } else {
            LOG.warn("Invalid key: " + cenaKey);
        }
        eb.property(vf.createURI(BASE_URI + cenaKey), normalizeSum(cenaValue));
        return eb;
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
        keys.put("nazov-zmluvy", 0);
        keys.put("typ", 0);
        keys.put("rezort", 0);
        keys.put("objednavatel", 0);
        keys.put("dodavatel", 0);
        keys.put("ico", 0);
        keys.put("cislo-zmluvy", 0);
        keys.put("datum-zverejnenia", 0);
        keys.put("datum-uzavretia", 0);
        keys.put("datum-ucinnosti", 0);
        keys.put("datum-platnosti-do", 0);
        keys.put("celkova-ciastka", 0);
        keys.put("priloha", 0);
        keys.put("linka-na-detail", 0);
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

    private static String slugify(String input) {
        String result = StringUtils.stripAccents(input);
        result = StringUtils.lowerCase(result).trim();
        result = result.replaceAll("[^a-zA-Z0-9\\s]", "");
        result = result.replaceAll("\\b\\s+", "-");
        return result;

    }

}
