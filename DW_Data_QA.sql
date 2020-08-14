--DimSchool
------------------------------------------------------------------------------------------
--ODS
SELECT SchoolKey,
       _sourceKey,
       DistrictSchoolCode,
       StateSchoolCode,
       UmbrellaSchoolCode,
       ShortNameOfInstitution,
       NameOfInstitution,
       SchoolCategoryType,
       SchoolCategoryType_Elementary_Indicator,
       SchoolCategoryType_Middle_Indicator,
       SchoolCategoryType_HighSchool_Indicator,
       SchoolCategoryType_Combined_Indicator,
       SchoolCategoryType_Other_Indicator,
       TitleIPartASchoolDesignationTypeCodeValue,
       TitleIPartASchoolDesignation_Indicator,
       OperationalStatusTypeDescriptor_CodeValue,
       OperationalStatusTypeDescriptor_Description,
       ValidFrom,
       ValidTo,
       IsCurrent,
       LineageKey 
FROM EdFiDW.[dbo].[DimSchool] 
WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType

--Legacy
SELECT SchoolKey,
       _sourceKey,
       DistrictSchoolCode,
       StateSchoolCode,
       UmbrellaSchoolCode,
       ShortNameOfInstitution,
       NameOfInstitution,
       SchoolCategoryType,
       SchoolCategoryType_Elementary_Indicator,
       SchoolCategoryType_Middle_Indicator,
       SchoolCategoryType_HighSchool_Indicator,
       SchoolCategoryType_Combined_Indicator,
       SchoolCategoryType_Other_Indicator,
       TitleIPartASchoolDesignationTypeCodeValue,
       TitleIPartASchoolDesignation_Indicator,
       OperationalStatusTypeDescriptor_CodeValue,
       OperationalStatusTypeDescriptor_Description,
       ValidFrom,
       ValidTo,
       IsCurrent,
       LineageKey 
FROM EdFiDW.[dbo].[DimSchool] 
WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0




--DimStudent
------------------------------------------------------------------------------------------
--ODS
--Legacy

--DimTime
------------------------------------------------------------------------------------------
--ODS
--Legacy

