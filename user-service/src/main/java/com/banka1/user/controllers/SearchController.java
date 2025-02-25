package com.banka1.user.controllers;

import com.banka1.user.service.CustomerService;
import com.banka1.user.service.EmployeeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/")
@Tag(name = "Pretraga korisnika")
public class SearchController {
    private final CustomerService customerService;
    private final EmployeeService employeeService;

    @Operation(
            summary = "Pretraga svih zaposlenih",
            description = "Vraca sve zaposlene koji zadovoljavaju (opcioni) filter, sortirani po zadatom polju. Rezultati se vracaju podeljeni na stranice."
    )
    @GetMapping("employees")
    //@Authorization(permissions = { Permission.LIST_EMPLOYEES })
    public ResponseEntity<?> searchEmployees(
//            @RequestHeader("Authorization")
//            String authorization,
            @Parameter(
                    description = "Redni broj stranice rezultata (podrazumevana vrednost je prva stranica)",
                    example = "0"
            )
            @RequestParam Optional<Integer> page,
            @Parameter(
                    description = "Velicina jedne stranice (podrazumevana vrednost je 10)",
                    example = "10"
            )
            @RequestParam Optional<Integer> pageSize,
            @Parameter(
                    description = "Polje po kojem se sortira. Ako nije postavljeno, sortira se po ID-u.",
                    example = "firstName"
            )
            @RequestParam Optional<String> sortField,
            @Parameter(
                    description = "Redosled sortiranja. Mora biti asc ili desc, podrazumeva se asc.",
                    example = "desc"
            )
            @RequestParam Optional<String> sortOrder,
            @Parameter(
                    description = "Polje po kojem se filtriraju rezultati. Nema filtera ako se ne postavi.",
                    example = "lastName"
            )
            @RequestParam Optional<String> filterField,
            @Parameter(
                    description = "Tekst koji se trazi u filtriranom polju. Mora se postaviti ako je postavljen filterField.",
                    example = "Petrovic"
            )
            @RequestParam Optional<String> filterValue
    ) {
        try {
            var employees = employeeService.search(
                    page.orElse(0), pageSize.orElse(10),
                    sortField, sortOrder, filterField, filterValue
            );
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", employees
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    @Operation(
            summary = "Pretraga svih musterija",
            description = "Vraca sve musterije koji zadovoljavaju (opcioni) filter, sortirani po zadatom polju. Rezultati se vracaju podeljeni na stranice."
    )
    @GetMapping("customers")
    //@Authorization(permissions = { Permission.LIST_CUSTOMERS })
    public ResponseEntity<?> searchCustomers(
//            @RequestHeader("Authorization")
//            String authorization,
            @Parameter(
                    description = "Redni broj stranice rezultata (podrazumevana vrednost je prva stranica)",
                    example = "0"
            )
            @RequestParam Optional<Integer> page,
            @Parameter(
                    description = "Velicina jedne stranice (podrazumevana vrednost je 10)",
                    example = "10"
            )
            @RequestParam Optional<Integer> pageSize,
            @Parameter(
                    description = "Polje po kojem se sortira. Ako nije postavljeno, sortira se po ID-u.",
                    example = "firstName"
            )
            @RequestParam Optional<String> sortField,
            @Parameter(
                    description = "Redosled sortiranja. Mora biti asc ili desc, podrazumeva se asc.",
                    example = "desc"
            )
            @RequestParam Optional<String> sortOrder,
            @Parameter(
                    description = "Polje po kojem se filtriraju rezultati. Nema filtera ako se ne postavi.",
                    example = "lastName"
            )
            @RequestParam Optional<String> filterField,
            @Parameter(
                    description = "Tekst koji se trazi u filtriranom polju. Mora se postaviti ako je postavljen filterField.",
                    example = "Petrovic"
            )
            @RequestParam Optional<String> filterValue
    ) {
        try {
            var employees = customerService.search(
                    page.orElse(0), pageSize.orElse(10),
                    sortField, sortOrder, filterField, filterValue
            );
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", employees
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }
}
