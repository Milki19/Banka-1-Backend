package com.banka1.banking.services;

import com.banka1.banking.dto.CustomerDTO;
import com.banka1.banking.dto.ExchangeMoneyTransferDTO;
import com.banka1.banking.dto.NotificationDTO;
import com.banka1.banking.listener.MessageHelper;
import com.banka1.banking.models.Account;
import com.banka1.banking.models.Currency;
import com.banka1.banking.models.Transfer;
import com.banka1.banking.models.helper.CurrencyType;
import com.banka1.banking.repository.AccountRepository;
import com.banka1.banking.repository.CurrencyRepository;
import com.banka1.banking.repository.TransferRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ExchangeServiceTest {

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransferRepository transferRepository;

    @Mock
    private CurrencyRepository currencyRepository;

    @Mock
    private JmsTemplate jmsTemplate;

    @Mock
    private MessageHelper messageHelper;

    @Mock
    private UserServiceCustomer userServiceCustomer;

    @Mock
    private OtpTokenService otpTokenService;

    @InjectMocks
    private ExchangeService exchangeService;

    private Account fromAccount;
    private Account toAccount;
    private Currency currencyEUR;
    private Currency currencyUSD;
    private ExchangeMoneyTransferDTO exchangeMoneyTransferDTO;
    private CustomerDTO customerDTO;

    @BeforeEach
    void setUp() {
        fromAccount = new Account();
        fromAccount.setId(1L);
        fromAccount.setOwnerID(10L);
        fromAccount.setCurrencyType(CurrencyType.EUR);

        toAccount = new Account();
        toAccount.setId(2L);
        toAccount.setOwnerID(10L);
        toAccount.setCurrencyType(CurrencyType.USD);

        currencyEUR = new Currency();
        currencyEUR.setCode(CurrencyType.EUR);

        currencyUSD = new Currency();
        currencyUSD.setCode(CurrencyType.USD);

        exchangeMoneyTransferDTO = new ExchangeMoneyTransferDTO();
        exchangeMoneyTransferDTO.setAccountFrom(1L);
        exchangeMoneyTransferDTO.setAccountTo(2L);
        exchangeMoneyTransferDTO.setAmount(500.0);

//        customerDTO = new CustomerDTO();
//        customerDTO.setId(10L);
//        customerDTO.setFirstName("Marko");
//        customerDTO.setLastName("Markovic");
//        customerDTO.setBirthDate(19990101L);
//        customerDTO.setEmail("test@test.com");
//        customerDTO.setPhoneNumber("010101010");
//        customerDTO.setAddress("MARSALA TULBUHINA");

         customerDTO = new CustomerDTO(10L,"Marko","Markovic",0101L,"test@test.com","0101010101","MARSALA TULBUHINA");


        ReflectionTestUtils.setField(exchangeService, "destinationEmail", "test-destination");
    }

    @Test
    void validateExchangeTransferValidCaseReturnsTrue() {
        when(accountRepository.findById(1L)).thenReturn(Optional.of(fromAccount));
        when(accountRepository.findById(2L)).thenReturn(Optional.of(toAccount));

        boolean result = exchangeService.validateExchangeTransfer(exchangeMoneyTransferDTO);
        assertTrue(result);
    }

    @Test
    void validateExchangeTransferSameCurrencyReturnsFalse() {
        toAccount.setCurrencyType(CurrencyType.EUR);
        when(accountRepository.findById(1L)).thenReturn(Optional.of(fromAccount));
        when(accountRepository.findById(2L)).thenReturn(Optional.of(toAccount));

        boolean result = exchangeService.validateExchangeTransfer(exchangeMoneyTransferDTO);
        assertFalse(result);
    }

    @Test
    void createExchangeTransferSuccess() {
        when(accountRepository.findById(1L)).thenReturn(Optional.of(fromAccount));
        when(accountRepository.findById(2L)).thenReturn(Optional.of(toAccount));
        when(currencyRepository.findByCode(CurrencyType.EUR)).thenReturn(Optional.of(currencyEUR));
        when(currencyRepository.findByCode(CurrencyType.USD)).thenReturn(Optional.of(currencyUSD));
        when(userServiceCustomer.getCustomerById(10L)).thenReturn(customerDTO);
        doReturn("123456").when(otpTokenService).generateOtp(anyLong());

        when(transferRepository.saveAndFlush(any(Transfer.class))).thenAnswer(invocation -> {
            Transfer savedTransfer = invocation.getArgument(0);
            savedTransfer.setId(102L);
            return savedTransfer;
        });

        when(transferRepository.save(any(Transfer.class))).thenAnswer(invocation -> invocation.getArgument(0));

        doReturn("Simulirana poruka").when(messageHelper).createTextMessage(any(NotificationDTO.class));

        exchangeService.createExchangeTransfer(exchangeMoneyTransferDTO);

        verify(transferRepository, times(1)).saveAndFlush(any(Transfer.class));
        verify(transferRepository, times(1)).save(any(Transfer.class));
        verify(jmsTemplate, times(1)).convertAndSend(eq("test-destination"), eq("Simulirana poruka"));
    }
}

