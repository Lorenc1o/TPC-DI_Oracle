LOAD DATA 
INTO TABLE S_Broker 
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
    EmployeeID,
    ManagerID,
    EmployeeFirstName,
    EmployeeLastName,
    EmployeeMI,
    EmployeeJobCode,
    EmployeeBranch,
    EmployeeOffice,
    EmployeePhone
)