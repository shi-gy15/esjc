package client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.github.msemys.esjc.EventData;
import com.github.msemys.esjc.EventStore;
import com.github.msemys.esjc.EventStoreBuilder;
import com.github.msemys.esjc.ExpectedVersion;
import com.github.msemys.esjc.PersistentSubscription;
import com.github.msemys.esjc.PersistentSubscriptionListener;
import com.github.msemys.esjc.PersistentSubscriptionSettings;
import com.github.msemys.esjc.Position;
import com.github.msemys.esjc.ResolvedEvent;
import com.github.msemys.esjc.StreamPosition;
import com.github.msemys.esjc.system.SystemConsumerStrategy;

public class Client {
	
	private EventStore eventstore;
	private static final int maxEventNum = 4096;
	private List<ResolvedEvent> receivedEvents;
	private CompletableFuture<PersistentSubscription> subscription;
	
	public static void main(String[] args) throws Exception{
		
//		Client client = new Client("123.206.20.171", 1113);
		Client client = new Client("127.0.0.1", 1113);
		
		// args[0]: test json filepath
		if (args.length < 1) {
			System.out.println("1 argument required(test json filepath).");
			return;
		}
		String filename = args[0];
		
//		String filename = "D:\\Work\\2018Spring\\lab\\client\\esjc\\src\\main\\java\\client\\test.json";
		List<EventData> lis = null;
		String jsons = "jsontest";
		String json = null, json2 = null;
		
		try {
			lis = ClientUtil.loadEventsFromGson(ClientUtil.gsonFromFile(filename));
		} catch (Exception e) {
			System.out.println(e.toString());
			throw e;
		}
		
		client.deleteStream(jsons, true);
		client.writeListToStream(jsons, lis, true);
		json = ClientUtil.genGsonList(client.readStreamUsingType(jsons, "delete")).toString();
		json2 = ClientUtil.genGsonList(client.readStreamAscend(jsons, true)).toString();
		
		
		System.out.println(json);
		System.out.println(json2);
		
		client.deletePersistentSubscription("jsontest", "local");
		client.createPersistentSubscription("jsontest", "local");
		client.subscribePersistent("jsontest", "local");
		
		System.out.println("Test finished");
		return;
	}
	
	Client() {
		this("127.0.0.1", 1113);
	}
	
	Client(String url, int port) {
		this(url, port, "admin", "changeit");
	}
	
	Client(String url, int port, String username, String password) {
		eventstore = EventStoreBuilder.newBuilder()
			    .singleNodeAddress(url, port)
			    .userCredentials(username, password)
			    .build();
		receivedEvents = new ArrayList<ResolvedEvent>();
	}
	
	// API : Write a sequence of EventData into one stream
	public void writeListToStream(String stream, List<EventData> list, boolean sync) {
		CompletableFuture<Void> future = eventstore.appendToStream(stream, ExpectedVersion.ANY, list)
				.thenAccept(r -> System.out.println("Write " + r.logPosition));
		if (sync) {
			future.join();
		}
	}
	
	// API : Read all events of one stream, in asc order
	public List<ResolvedEvent> readStreamAscend(String stream, boolean sync) {
		List<ResolvedEvent> list = new ArrayList<ResolvedEvent>();
		CompletableFuture<Void> future = eventstore.readStreamEventsForward(stream, 0, maxEventNum, false)
			.thenAccept(result -> result.events.forEach(e -> list.add(e)));
		if (sync) {
			future.join();
		}
		
		return list;
	}
	
	// API : Read all events of one stream, in desc order
	public List<ResolvedEvent> readStreamDescend(String stream, boolean sync) {
		List<ResolvedEvent> list = new ArrayList<ResolvedEvent>();
		CompletableFuture<Void> future = eventstore.readStreamEventsBackward(stream, StreamPosition.END, maxEventNum, false)
			.thenAccept(result -> result.events.forEach(e -> list.add(e)));
		if (sync) {
			future.join();
		}
		
		return list;
	}
	
	// API : Read all events of all stream, in asc order
	public List<ResolvedEvent> readAllAscend(boolean sync) {
        List<ResolvedEvent> list = new ArrayList<ResolvedEvent>();
        CompletableFuture<Void> future = eventstore.readAllEventsForward(Position.START, maxEventNum, false)
            .thenAccept(result -> result.events.forEach(e -> list.add(e)));
        if (sync) {
            future.join();
        }
        
        return list;
    }
    
    // API : Read all events of all stream, in desc order
	public List<ResolvedEvent> readAllDescend(boolean sync) {
        List<ResolvedEvent> list = new ArrayList<ResolvedEvent>();
        CompletableFuture<Void> future = eventstore.readAllEventsBackward(Position.END, maxEventNum, false)
            .thenAccept(result -> result.events.forEach(e -> list.add(e)));
        if (sync) {
            future.join();
        }
        
        return list;
	}
	
	// API : Conditional read events of one stream, in asc order
	public List<ResolvedEvent> readStreamUsingType(String stream, String type) {
		List<ResolvedEvent> list = eventstore.streamEventsForward(stream, 0, maxEventNum, false)
        	.filter(e -> e.originalEvent().eventType.indexOf(type) >= 0)
        	.collect(Collectors.toList());
        
        return list;
	}

	// API : Delete one stream
	public void deleteStream(String stream, boolean sync) {
		try {
			CompletableFuture<Void> future = eventstore.deleteStream(stream, ExpectedVersion.ANY)
		            .thenAccept(result -> System.out.println(result.logPosition));
			if (sync) {
	            future.join();
	        }
		} catch (Exception e) {
			System.out.println("Stream not existed.");
		}
	}
	
	// API : Create a persistent subscription of one stream(default sync)
	public void createPersistentSubscription(String stream, String group) {
		eventstore.createPersistentSubscription(stream, group, PersistentSubscriptionSettings.newBuilder()
		    .resolveLinkTos(false)
		    .historyBufferSize(20)
		    .liveBufferSize(10)
		    .minCheckPointCount(10)
		    .maxCheckPointCount(1000)
		    .checkPointAfter(Duration.ofSeconds(2))
		    .maxRetryCount(500)
		    .maxSubscriberCount(5)
		    .messageTimeout(Duration.ofSeconds(30))
		    .readBatchSize(500)
		    .startFromCurrent()
		    .timingStatistics(false)
		    .namedConsumerStrategy(SystemConsumerStrategy.ROUND_ROBIN)
		    .build()
		).thenAccept(r -> System.out.println(r.status)).join();
	}
	
	// API : Delete a persistent subscription of one stream(default sync)
	public void deletePersistentSubscription(String stream, String group) {
		try {
			eventstore.deletePersistentSubscription(stream, group).thenAccept(r -> System.out.println(r.status)).join();
		} catch (Exception e) {
			System.out.println("Subscription not existed.");
		}
		
	}
	
	// API : Subscribe to persistent subscription of one stream(default sync)
	public void subscribePersistent(String stream, String group) {
		subscription = eventstore.subscribeToPersistent(stream, group, new MyListener(this));
		subscription.join();
	}
	
	class MyListener implements PersistentSubscriptionListener {
		
		private Client ctx;
		
		MyListener(Client vctx) {
			super();
			this.ctx = vctx;
		}
		
		@Override
		public void onEvent(PersistentSubscription subscription, ResolvedEvent event) {
			ctx.receivedEvents.add(event);
			System.out.println(ClientUtil.gsonFromSingleEvent(event).toString());
		}
	}
}
