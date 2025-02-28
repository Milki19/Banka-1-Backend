package com.banka1.user.service;

import com.banka1.user.DTO.NotificationDTO;
import com.banka1.user.DTO.request.ResetPasswordDTO;
import com.banka1.user.DTO.request.ResetPasswordRequestDTO;
import com.banka1.user.listener.MessageHelper;
import com.banka1.user.model.ResetPassword;
import com.banka1.user.repository.CustomerRepository;
import com.banka1.user.repository.EmployeeRepository;
import com.banka1.user.repository.ResetPasswordRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;

@Service
public class ResetPasswordService {

    private final CustomerRepository customerRepository;
    private final ResetPasswordRepository resetPasswordRepository;
    private final EmployeeRepository employeeRepository;
    private final JmsTemplate jmsTemplate;
    private final MessageHelper messageHelper;
    private final String destinationEmail;

    @Value("${frontend.url}")
    private String frontendUrl;

    public ResetPasswordService(CustomerRepository customerRepository, ResetPasswordRepository resetPasswordRepository, EmployeeRepository employeeRepository, JmsTemplate jmsTemplate, MessageHelper messageHelper, @Value("${destination.email}") String destinationEmail) {
        this.customerRepository = customerRepository;
        this.resetPasswordRepository = resetPasswordRepository;
        this.employeeRepository = employeeRepository;
        this.jmsTemplate = jmsTemplate;
        this.messageHelper = messageHelper;
        this.destinationEmail = destinationEmail;
    }

    public void requestPasswordReset(ResetPasswordRequestDTO resetPasswordRequestDTO) {
        var customer = customerRepository.findByEmail(resetPasswordRequestDTO.getEmail());
        var employee = employeeRepository.findByEmail(resetPasswordRequestDTO.getEmail());
        if (customer.isEmpty() && employee.isEmpty())
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Email not found.");
        var resetPassword = new ResetPassword();
        resetPassword.setToken(generateToken());
        resetPassword.setUsed(false);
        resetPassword.setExpirationDate(System.currentTimeMillis() + 1000 * 60 * 60 * 24);
        resetPassword.setCreatedDate(System.currentTimeMillis());

        NotificationDTO emailDTO = new NotificationDTO();

        if (customer.isPresent()) {
            resetPassword.setUserId(customer.get().getId());
            resetPassword.setType(0);
            emailDTO.setEmail(customer.get().getEmail());
        } else {
            resetPassword.setUserId(employee.get().getId());
            resetPassword.setType(1);
            emailDTO.setEmail(employee.get().getEmail());
        }

        emailDTO.setSubject("Zahtev za resetovanje lozinke");
        emailDTO.setMessage("Zahtev za resetovanje lozinke je uspešno poslat." +
                " Kliknite na link da biste resetovali lozinku: " + frontendUrl +
                "/reset-password?token=" + resetPassword.getToken());
        emailDTO.setType("email");

        jmsTemplate.convertAndSend(destinationEmail, messageHelper.createTextMessage(emailDTO));

        resetPasswordRepository.save(resetPassword);
    }

    public void resetPassword(ResetPasswordDTO resetPasswordDTO) {
        var resetPassword = resetPasswordRepository.findByToken(resetPasswordDTO.getToken());
        if (resetPassword == null)
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Token not found.");

        if (resetPassword.getExpirationDate() < System.currentTimeMillis())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Token expired.");
        if (resetPassword.getUsed())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Token already used.");
        String salt = generateSalt();
        String hashed = BCrypt.hashpw(resetPasswordDTO.getPassword() + salt, BCrypt.gensalt());
        if (resetPassword.getType() == 0) {
            var customer = customerRepository.findById(resetPassword.getUserId());
            if(customer.isEmpty()) {
                throw new IllegalStateException("Impossible state reached");
            }
            customer.get().setPassword(hashed);
            customer.get().setSaltPassword(salt);
            customerRepository.save(customer.get());
        } else {
            var employee = employeeRepository.findById(resetPassword.getUserId());
            if (employee.isEmpty()) {
                throw new IllegalStateException("Impossible state reached");
            }
            employee.get().setPassword(hashed);
            employee.get().setSaltPassword(salt);
            employeeRepository.save(employee.get());
        }
        resetPassword.setUsed(true);
        resetPasswordRepository.save(resetPassword);
    }

    public String generateToken() {
        return UUID.randomUUID().toString();
    }

    private String generateSalt() {
        byte[] saltBytes = new byte[16];
        new SecureRandom().nextBytes(saltBytes);
        return Base64.getEncoder().encodeToString(saltBytes);
    }
}
