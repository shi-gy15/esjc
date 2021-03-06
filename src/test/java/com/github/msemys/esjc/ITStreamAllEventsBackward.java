package com.github.msemys.esjc;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.github.msemys.esjc.matcher.RecordedEventListMatcher.containsInOrder;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.*;

public class ITStreamAllEventsBackward extends AbstractIntegrationTest {

    @Override
    protected EventStore createEventStore() {
        return eventstoreSupplier.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void failsToProcessWhenBatchSizeIsZero() {
        eventstore.streamAllEventsBackward(Position.START, 0, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failsToProcessWhenBatchSizeOutOfRange() {
        eventstore.streamAllEventsBackward(Position.START, 4097, false);
    }

    @Test
    public void returnsEmptyStreamIfAskedToProcessFromStart() {
        final String stream = generateStreamName();

        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, newTestEvents(20)).join();

        assertFalse(eventstore.streamAllEventsBackward(Position.START, 1, false).findFirst().isPresent());
    }

    @Test
    public void readsFirstEvent() {
        final String stream = generateStreamName();

        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, newTestEvents(20)).join();

        List<ResolvedEvent> firstEvents = eventstore.readAllEventsForward(Position.START, 2, false).join().events;
        Position position = firstEvents.get(1).originalPosition;

        Optional<ResolvedEvent> event = eventstore.streamAllEventsBackward(position, 1, false).findFirst();

        assertTrue(event.isPresent());
        assertEquals(firstEvents.get(0).event.eventId, event.get().event.eventId);
    }

    @Test
    public void readsEventsInReversedOrderComparedToWritten() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> result = eventstore.streamAllEventsBackward(Position.END, 6, false)
            .limit(20)
            .collect(toList());

        assertThat(recordedEventsFrom(result), containsInOrder(reverse(events)));
    }

    @Test
    public void parallelReadsEventsInReversedOrderComparedToWritten() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> result = eventstore.streamAllEventsBackward(Position.END, 6, false)
            .parallel()
            .limit(20)
            .collect(toList());

        assertThat(recordedEventsFrom(result), containsInOrder(reverse(events)));
    }

    @Test
    public void readsAllEventsOneByOneUntilEndOfStream() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 1, false).collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void parallelReadsAllEventsOneByOneUntilEndOfStream() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 1, false)
            .parallel()
            .collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void readsAllEventsUntilEndOfStream() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 5, false).collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void parallelReadsAllEventsUntilEndOfStream() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 5, false)
            .parallel()
            .collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void readsAllEventsUntilEndOfStreamUsingMaxBatchSize() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 4096, false).collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void parallelReadsAllEventsUntilEndOfStreamUsingMaxBatchSize() {
        final String stream = generateStreamName();

        List<EventData> events = newTestEvents(20);
        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        List<ResolvedEvent> allEvents = eventstore.streamAllEventsBackward(Position.END, 4096, false)
            .parallel()
            .collect(toList());

        List<ResolvedEvent> allEventsSlice = allEvents.stream().limit(events.size()).collect(toList());

        assertEquals(20, allEventsSlice.size());
        assertThat(recordedEventsFrom(allEventsSlice), containsInOrder(reverse(events)));
    }

    @Test
    public void aggregatesEvents() {
        final String stream = generateStreamName();

        List<EventData> events = range(0, 101)
            .mapToObj(i -> EventData.newBuilder()
                .type(i % 2 == 0 ? "even" : "odd")
                .data(String.valueOf(i))
                .build())
            .collect(toList());

        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        assertEquals(51, eventstore.streamAllEventsBackward(Position.END, 17, false)
            .filter(e -> e.event.eventStreamId.equals(stream))
            .filter(e -> e.event.eventType.equals("even"))
            .count());

        assertEquals(50, eventstore.streamAllEventsBackward(Position.END, 17, false)
            .filter(e -> e.event.eventStreamId.equals(stream))
            .filter(e -> e.event.eventType.equals("odd"))
            .count());
    }

    @Test
    public void parallelAggregatesEvents() {
        final String stream = generateStreamName();

        List<EventData> events = range(0, 101)
            .mapToObj(i -> EventData.newBuilder()
                .type(i % 2 == 0 ? "even" : "odd")
                .data(String.valueOf(i))
                .build())
            .collect(toList());

        eventstore.appendToStream(stream, ExpectedVersion.NO_STREAM, events).join();

        assertEquals(51, eventstore.streamAllEventsBackward(Position.END, 17, false)
            .parallel()
            .filter(e -> e.event.eventStreamId.equals(stream))
            .filter(e -> e.event.eventType.equals("even"))
            .count());

        assertEquals(50, eventstore.streamAllEventsBackward(Position.END, 17, false)
            .parallel()
            .filter(e -> e.event.eventStreamId.equals(stream))
            .filter(e -> e.event.eventType.equals("odd"))
            .count());
    }

}
