package com.banka1.banking.services;

import com.banka1.banking.dto.CreateCompanyDTO;
import com.banka1.banking.models.Company;
import com.banka1.banking.repository.AccountRepository;
import com.banka1.banking.repository.CompanyRepository;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.util.List;

@Service

public class CompanyService {
    private final CompanyRepository companyRepository;
    private final ModelMapper modelMapper;
    private final AccountRepository accountRepository;


    public CompanyService(CompanyRepository companyRepository, ModelMapper modelMapper, AccountRepository accountRepository) {
        this.companyRepository = companyRepository;
        this.modelMapper = modelMapper;
        this.accountRepository = accountRepository;
    }

    public Company createCompany(CreateCompanyDTO createCompanyDTO) {
        Company company = modelMapper.map(createCompanyDTO, Company.class);
        System.out.println(company.getName());
        company = companyRepository.save(company);
        return company;
    }

    public Company getCompany(Long companyId) {
        return companyRepository.findById(companyId).orElse(null);
    }

    public List<Company> getCompanies() {
        return companyRepository.findAll();
    }
}
