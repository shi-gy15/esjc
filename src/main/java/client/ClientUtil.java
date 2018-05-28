package client;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.github.msemys.esjc.EventData;
import com.github.msemys.esjc.ResolvedEvent;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ClientUtil {
	private static JsonParser jsonParser = new JsonParser();
	
	public static JsonElement gsonFromFile(String filename) throws FileNotFoundException{
		return jsonParser.parse(new FileReader(filename));
	}
	
	public static JsonElement gsonFromString(String string) {
		return jsonParser.parse(string);
	}
	
	public static JsonObject gsonFromSingleEvent(ResolvedEvent event) {
		JsonObject obj = new JsonObject();
		
		obj.add("stream", (JsonPrimitive) gsonFromString(event.originalStreamId()));
		obj.add("type", (JsonPrimitive) gsonFromString(event.originalEvent().eventType));
		obj.add("data", (JsonObject) gsonFromString(new String(event.originalEvent().data)));
		
		return obj;
	}
	
	public static JsonObject genGsonList(List<ResolvedEvent> events) {
		JsonObject obj = new JsonObject();
		JsonArray eventsGson = new JsonArray();
		
		for (ResolvedEvent event : events) {
			eventsGson.add(gsonFromSingleEvent(event));
		}
		
		obj.add("events", eventsGson);
		return obj;
	}
	
	/**
	 * 
	 * json format:
	 * {
	 *   "events" : [
	 *     {
	 *     "stream" : "<stream-str>",
	 *       "type" : "<type-str>"
	 *       "data" : {...}
	 *     },
	 *     {
	 *       "type" : "<type-str>"
	 *       "data" : {...}
	 *     },
	 *     ...
	 *   ]
	 * }
	 */
	
	public static String getStream(JsonObject gson) {
		return gson.get("stream").getAsString();
	}
	
	public static List<EventData> loadEventsFromGson(JsonElement gson) {
		List<EventData> list = new ArrayList<EventData>();
		JsonArray events = null;
		if (gson instanceof JsonObject) {
			events = ((JsonObject) gson).get("events").getAsJsonArray();
		}
		else if (gson instanceof JsonArray) {
			events = (JsonArray) gson;
		}
		else {
			events = (JsonArray) gson;
		}
		for (Object event : events) {
			JsonObject e = ((JsonElement)event).getAsJsonObject();
			list.add(EventData.newBuilder()
				 	.eventId(UUID.randomUUID())
				 	.type(e.get("type").getAsString())
				 	.jsonData(e.get("data").getAsJsonObject().toString())
				 	.build());
		}
		
		return list;
	}
	
}
