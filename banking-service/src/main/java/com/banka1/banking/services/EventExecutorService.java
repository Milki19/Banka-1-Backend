package com.banka1.banking.services;

import com.banka1.banking.dto.CreateEventDeliveryDTO;
import com.banka1.banking.models.Event;
import com.banka1.banking.models.EventDelivery;
import com.banka1.banking.models.helper.DeliveryStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class EventExecutorService {

    private final EventService eventService;
    private final TaskScheduler taskScheduler = new ConcurrentTaskScheduler();

    private static final int MAX_RETRIES = 5;
    private static final Duration RETRY_DELAY = Duration.ofSeconds(20);

    @Async
    public void attemptEventAsync(Event event) {
        attemptDelivery(event, 1);
    }

    private RestTemplate getTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) throws IOException {
                return false;
            }

            @Override
            public void handleError(URI url, HttpMethod method, ClientHttpResponse response) throws IOException {

            }
        });

        return restTemplate;
    }

    private void attemptDelivery(Event event, int attempt) {
        Instant start = Instant.now();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        headers.set("X-Api-Key", ":DDDDDDDDDDDDDDDD");

        HttpEntity<String> entity = new HttpEntity<>(event.getPayload(), headers);

        String responseBody = null;
        int httpStatus = 0;
        DeliveryStatus status;

        try {

            ResponseEntity<String> response = getTemplate().postForEntity(event.getUrl(), entity, String.class);
            responseBody = response.getBody() != null ? new ObjectMapper().writeValueAsString(response.getBody()) : "";

            httpStatus = response.getStatusCodeValue();

            status = response.getStatusCode().is2xxSuccessful() ? DeliveryStatus.SUCCESS : DeliveryStatus.FAILED;

        } catch (Exception ex) {
            System.out.println("Error during event delivery: " + ex.getMessage());
            System.out.println(ex.getStackTrace());
            status = DeliveryStatus.FAILED;
            httpStatus = -1;
            responseBody = ex.getMessage();
        }

        if (status == DeliveryStatus.FAILED && attempt < MAX_RETRIES) {
            eventService.changeEventStatus(event, DeliveryStatus.RETRYING);
            taskScheduler.schedule(() -> attemptDelivery(event, attempt + 1), Instant.now().plus(RETRY_DELAY));
        } else if (status == DeliveryStatus.SUCCESS) {
            eventService.changeEventStatus(event, DeliveryStatus.SUCCESS);
        } else if (attempt >= MAX_RETRIES) {
            eventService.changeEventStatus(event, DeliveryStatus.CANCELED);
        }

        long durationMs = Instant.now().toEpochMilli() - start.toEpochMilli();

        CreateEventDeliveryDTO createEventDeliveryDTO = new CreateEventDeliveryDTO();
        createEventDeliveryDTO.setEvent(event);
        createEventDeliveryDTO.setDurationMs(durationMs);
        createEventDeliveryDTO.setHttpStatus(httpStatus);
        createEventDeliveryDTO.setResponseBody(responseBody);
        createEventDeliveryDTO.setStatus(status);

        System.out.println("Response body: " + responseBody);
        System.out.println("Response status code: " + httpStatus);

        EventDelivery eventDelivery = eventService.createEventDelivery(createEventDeliveryDTO);
    }

}
