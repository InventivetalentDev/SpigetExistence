package org.spiget.existence;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Projections;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.bson.Document;
import org.spiget.client.HtmlUnitClient;
import org.spiget.client.SpigetClient;
import org.spiget.client.SpigetResponse;
import org.spiget.data.author.ListedAuthor;
import org.spiget.data.category.ListedCategory;
import org.spiget.data.resource.ListedResource;
import org.spiget.data.resource.Rating;
import org.spiget.data.resource.Resource;
import org.spiget.data.resource.SpigetIcon;
import org.spiget.data.resource.version.ListedResourceVersion;
import org.spiget.database.DatabaseClient;
import org.spiget.parser.ResourcePageParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Log4j2
public class SpigetExistence {

	public static JsonObject config;

	public static DatabaseClient databaseClient;

	public SpigetExistence() {
	}

	public SpigetExistence init() throws IOException {
		config = new JsonParser().parse(new FileReader("config.json")).getAsJsonObject();
		SpigetClient.config = config;
		SpigetClient.userAgent = config.get("request.userAgent").getAsString();
		SpigetClient.loadCookiesFromFile();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					log.info("Saving cookies...");
					SpigetClient.saveCookiesToFile();
				} catch (IOException e) {
					log.warn("Failed to save cookies", e);
				}
				try {
					log.info("Disconnecting database...");
					databaseClient.disconnect();
				} catch (IOException e) {
					log.warn("Failed to disconnect from database", e);
				}
			}
		});

		{
			log.info("Initializing & testing database connection...");
			long testStart = System.currentTimeMillis();
			try {
				databaseClient = new DatabaseClient(
						config.get("database.name").getAsString(),
						config.get("database.host").getAsString(),
						config.get("database.port").getAsInt(),
						config.get("database.user").getAsString(),
						config.get("database.pass").getAsString().toCharArray(),
						config.get("database.db").getAsString());
				databaseClient.connect(config.get("database.timeout").getAsInt());
				databaseClient.collectionCount();
				log.info("Connection successful (" + (System.currentTimeMillis() - testStart) + "ms)");
			} catch (Exception e) {
				log.fatal("Connection failed after " + (System.currentTimeMillis() - testStart) + "ms", e);
				log.fatal("Aborting.");
				System.exit(-1);
				return null;
			}
		}

		{
			log.info("Testing SpigotMC connection...");
			long testStart = System.currentTimeMillis();
			try {
				SpigetResponse response = SpigetClient.get(SpigetClient.BASE_URL);
				log.info("Connection successful (" + (System.currentTimeMillis() - testStart) + "ms)");
			} catch (Exception e) {
				log.fatal("Connection failed after " + (System.currentTimeMillis() - testStart) + "ms", e);
				log.fatal("Aborting.");
				System.exit(-1);
				return null;
			}
		}

		return this;
	}

	public void check() {
		ResourcePageParser resourcePageParser = new ResourcePageParser();

		int offset = config.get("offset").getAsInt();

		long start = System.currentTimeMillis();
		databaseClient.updateStatus("existence.start", start);
		databaseClient.updateStatus("existence.end", 0);

		int counter = 0;
		int suspectCounter = 0;
		FindIterable<Document> iterable = databaseClient.getResourcesCollection().find().projection(Projections.include("_id"));
		Set<Document> set = new HashSet<>();
		for (Document document : iterable)
			set.add(document);

		databaseClient.updateStatus("existence.document.amount", set.size());

		for (Document document : set) {
			counter++;
			int id = document.getInteger("_id");
			if (counter < offset) {
				log.info("Skipping #" + counter + " (Offset: " + offset + ")");
				continue;
			}
			log.info("Checking #" + counter + " (" + id + ")");

			databaseClient.updateStatus("existence.document.index", counter);
			databaseClient.updateStatus("existence.document.id", id);

			try {
				SpigetResponse response = SpigetClient.get(SpigetClient.BASE_URL + "resources/" + id);
				if (response.getCode() == 503) {// Hit Cloudflare -> ignore it
					continue;
				}
				org.jsoup.nodes.Document resourceDocument = response.getDocument();
				// Create a fake listed-resource, so stuff doesn't go crazy while parsing
				ListedResource listedResource = new ListedResource(id, "");
				listedResource.setIcon(new SpigetIcon("", ""));
				listedResource.setAuthor(new ListedAuthor(0, ""));
				listedResource.setRating(new Rating(0, 0));
				listedResource.setCategory(new ListedCategory(0));
				listedResource.setVersion(new ListedResourceVersion(0, "", 0));
				listedResource.setTag("");
				Resource resource = null;
				try {
					resource = resourcePageParser.parse(resourceDocument, listedResource);
				} catch (Exception e) {
					log.log(Level.WARN, "Parse-Exception", e);
				}

				boolean complete = true;
				if (resource == null) {
					complete = false;
				} else {
					if (resource.getDescription() == null) { complete = false; }
					if (resource.getFile() == null || resource.getFile().getType() == null) { complete = false; }
					if (resource.getLinks().get("discussion") == null) { complete = false; }
				}

				if (!complete) {
					log.warn("Incomplete data -> Setting Status#1");
					setResourceStatus(id, 1);
					suspectCounter++;
					databaseClient.updateStatus("existence.document.suspects", suspectCounter);
					continue;
				}

				// Resource exists -> Reset
				resetResourceStatus(id);

				// Update download count
				if (resource.getDownloads() > 0) { updateDownloads(id, resource.getDownloads()); }
			} catch (Throwable throwable) {
				if (throwable.getMessage() != null && throwable.getMessage().contains("Read timed out")) {
					log.log(Level.WARN, "Read timeout -> Setting Status#3", throwable);
					setResourceStatus(id, 3);
					suspectCounter++;
					databaseClient.updateStatus("existence.document.suspects", suspectCounter);
				} else {
					log.log(Level.WARN, "Unknown exception -> Setting Status#2", throwable);
					setResourceStatus(id, 2);
					suspectCounter++;
					databaseClient.updateStatus("existence.document.suspects", suspectCounter);
				}
			}

			if (counter % 10 == 0) {
				log.debug("> Reset Client");
				HtmlUnitClient.disposeClient();
				Runtime.getRuntime().gc();

				databaseClient.updateSystemStats("existence.");
			}
		}

		long end = System.currentTimeMillis();
		databaseClient.updateStatus("existence.end", end);

		log.info("Done! " + suspectCounter + "/" + counter + " resources are probably deleted.");
	}

	void setResourceStatus(int id, int status) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("existenceStatus", status)));
	}

	void resetResourceStatus(int id) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$unset", new Document("existenceStatus", "")));
	}

	void updateDownloads(int id, int downloads) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("downloads", downloads)));
	}

}
