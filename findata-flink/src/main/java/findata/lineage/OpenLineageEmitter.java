package findata.lineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.UUID;

/**
 * Lightweight OpenLineage emitter for Marquez (HTTP POST /api/v1/lineage).
 * Failures are logged and never fail the Flink job.
 */
public final class OpenLineageEmitter {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String PRODUCER = "https://github.com/tradebot/findata-flink";
	private static final String SCHEMA_URL =
			"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

	private final String url;
	private final String namespace;
	private final String jobName;
	private final String runId;
	private final boolean enabled;
	private final HttpClient http = HttpClient.newHttpClient();

	public OpenLineageEmitter(String jobName) {
		this.jobName = jobName;
		this.namespace = envOrDefault("OPENLINEAGE_NAMESPACE", "tradebot");
		this.url = envOrDefault("OPENLINEAGE_URL", "http://marquez:5000").replaceAll("/$", "");
		this.enabled = !"false".equalsIgnoreCase(envOrDefault("OPENLINEAGE_ENABLED", "false"));
		this.runId = UUID.randomUUID().toString();
	}

	public void start(String inputTopic, String outputTopic, String kafkaBootstrap) {
		emit("START", inputTopic, outputTopic, kafkaBootstrap);
	}

	public void complete(String inputTopic, String outputTopic, String kafkaBootstrap) {
		emit("COMPLETE", inputTopic, outputTopic, kafkaBootstrap);
	}

	public void fail(String inputTopic, String outputTopic, String kafkaBootstrap, String error) {
		emit("FAIL", inputTopic, outputTopic, kafkaBootstrap, error);
	}

	private void emit(String eventType, String inputTopic, String outputTopic, String kafkaBootstrap) {
		emit(eventType, inputTopic, outputTopic, kafkaBootstrap, null);
	}

	private void emit(
			String eventType,
			String inputTopic,
			String outputTopic,
			String kafkaBootstrap,
			String error) {
		if (!enabled) {
			return;
		}
		try {
			ObjectNode event = MAPPER.createObjectNode();
			event.put("eventType", eventType);
			event.put("eventTime", Instant.now().toString());
			event.put("producer", PRODUCER);
			event.put("schemaURL", SCHEMA_URL);

			ObjectNode run = event.putObject("run");
			run.put("runId", runId);
			ObjectNode runFacets = run.putObject("facets");
			if (error != null) {
				ObjectNode err = runFacets.putObject("errorMessage");
				err.put("_producer", PRODUCER);
				err.put("message", error);
				err.put("programmingLanguage", "JAVA");
			}

			ObjectNode job = event.putObject("job");
			job.put("namespace", namespace);
			job.put("name", jobName);
			job.putObject("facets");

			ArrayNode inputs = event.putArray("inputs");
			inputs.add(kafkaDataset(inputTopic, kafkaBootstrap));

			ArrayNode outputs = event.putArray("outputs");
			outputs.add(kafkaDataset(outputTopic, kafkaBootstrap));

			HttpRequest request = HttpRequest.newBuilder()
					.uri(URI.create(url + "/api/v1/lineage"))
					.header("Content-Type", "application/json")
					.POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(event)))
					.build();

			HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
			System.out.println("[openlineage] " + eventType + " " + namespace + "." + jobName
					+ " status=" + response.statusCode());
		} catch (Exception e) {
			System.err.println("[openlineage] failed to emit " + eventType + ": " + e.getMessage());
		}
	}

	private static ObjectNode kafkaDataset(String topic, String bootstrap) {
		ObjectNode ds = MAPPER.createObjectNode();
		ds.put("namespace", "kafka");
		ds.put("name", topic);
		ObjectNode facets = ds.putObject("facets");
		ObjectNode dataSource = facets.putObject("dataSource");
		dataSource.put("_producer", PRODUCER);
		dataSource.put(
				"_schemaURL",
				"https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json");
		dataSource.put("name", "kafka");
		dataSource.put("uri", "kafka://" + bootstrap);
		return ds;
	}

	private static String envOrDefault(String key, String defaultValue) {
		String value = System.getenv(key);
		return (value == null || value.isBlank()) ? defaultValue : value;
	}
}
