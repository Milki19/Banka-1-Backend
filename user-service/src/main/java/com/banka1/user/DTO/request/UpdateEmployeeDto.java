package com.banka1.user.DTO.request;

import com.banka1.user.model.helper.Department;
import com.banka1.user.model.helper.Gender;
import com.banka1.user.model.helper.Position;
import lombok.Data;

@Data
public class UpdateEmployeeDto {
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String address;
    private Position position;
    private Department department;
    private Boolean active;
    private Boolean isAdmin;
    private Gender gender;
}
