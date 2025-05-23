package com.banka1.banking.models.helper;

import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Embeddable
@AllArgsConstructor
@AttributeOverrides({
        @AttributeOverride(name = "routingNumber", column = @Column(name = "routing_number")),
        @AttributeOverride(name = "locallyGeneratedKey", column = @Column(name = "locally_generated_key"))
})
public class IdempotenceKey {

    private String routingNumber;

    @Column(length = 64)
    private String locallyGeneratedKey;

    public IdempotenceKey() {

    }
}