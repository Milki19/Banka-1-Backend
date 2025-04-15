package com.banka1.banking.services;

import com.banka1.banking.dto.CreateEventDTO;
import com.banka1.banking.dto.CreateEventDeliveryDTO;
import com.banka1.banking.dto.interbank.InterbankMessageDTO;
import com.banka1.banking.models.Event;
import com.banka1.banking.models.EventDelivery;
import com.banka1.banking.models.helper.DeliveryStatus;
import com.banka1.banking.models.helper.IdempotenceKey;
import com.banka1.banking.models.interbank.EventDirection;
import com.banka1.banking.repository.EventDeliveryRepository;
import com.banka1.banking.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventService {

    private final EventRepository eventRepository;
    private final EventDeliveryRepository eventDeliveryRepository;

    public int attemptCount(Event event) {
        return event.getDeliveries().size();
    }

    public void changeEventStatus(Event event, DeliveryStatus status) {
        event.setStatus(status);
        eventRepository.save(event);
    }

    public Event receiveEvent(InterbankMessageDTO<?> dto, String rawPayload, String sourceUrl) {
        Event event = new Event();
        event.setMessageType(dto.getMessageType().name());
        event.setPayload(rawPayload);
        event.setUrl(sourceUrl);
        event.setIdempotenceKey(dto.getIdempotenceKey());
        event.setDirection(EventDirection.INCOMING);
        event.setStatus(DeliveryStatus.PENDING);

        return eventRepository.save(event);
    }

    public Event createEvent(CreateEventDTO createEventDTO) {
        Event event = new Event();
        event.setPayload(createEventDTO.getPayload());
        event.setUrl(createEventDTO.getUrl());
        System.out.println("Event created: " + event.getPayload());
        event.setMessageType(createEventDTO.getMessageType());
        System.out.println("Event created: " + event.getMessageType());
        event.setDirection(EventDirection.OUTGOING);

        IdempotenceKey idempotenceKey = new IdempotenceKey();
        idempotenceKey.setRoutingNumber(111);
        idempotenceKey.setLocallyGeneratedKey(UUID.randomUUID().toString());

        event.setIdempotenceKey(idempotenceKey);

        Event saved = eventRepository.save(event);

        return saved;
    }

    public EventDelivery createEventDelivery(CreateEventDeliveryDTO createEventDeliveryDTO) {

        EventDelivery eventDelivery = new EventDelivery();
        eventDelivery.setEvent(createEventDeliveryDTO.getEvent());
        eventDelivery.setStatus(createEventDeliveryDTO.getStatus());
        eventDelivery.setHttpStatus(createEventDeliveryDTO.getHttpStatus());
        eventDelivery.setDurationMs(createEventDeliveryDTO.getDurationMs());
        eventDelivery.setResponseBody(createEventDeliveryDTO.getResponseBody());

        eventDelivery.setSentAt(Instant.now());

        System.out.println("Event delivery created: " + eventDelivery.getResponseBody());

        return eventDeliveryRepository.save(eventDelivery);
    }

    public List<EventDelivery> getEventDeliveriesForEvent(Long eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new RuntimeException("Event not found"));

        return eventDeliveryRepository.findByEvent(event);
    }

}
