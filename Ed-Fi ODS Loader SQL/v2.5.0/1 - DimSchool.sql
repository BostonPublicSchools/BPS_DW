DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimSchool')
BEGIN
    INSERT INTO EdFiDW.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.DimSchool', 
	 GETDATE(), 
	 NULL, 
	 'F'
	 , -- full 
	 ''
	); -- Processing
	SET @lineageKey = SCOPE_IDENTITY() ;
END;
ELSE
BEGIN
     SELECT @lineageKey = LineageKey
	 FROM EdFiDW.[dbo].[Lineage]
	 WHERE TableName= 'dbo.DimSchool'
END 



INSERT INTO EdFiDW.[dbo].[DimSchool]
           ([_sourceKey]
		   ,[DistrictSchoolCode]
		   ,[StateSchoolCode]
		   ,[UmbrellaSchoolCode]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[SchoolCategoryType]
           ,[SchoolCategoryType_Elementary_Indicator]
           ,[SchoolCategoryType_Middle_Indicator]
           ,[SchoolCategoryType_HighSchool_Indicator]
           ,[SchoolCategoryType_Combined_Indicator]       
		   ,[SchoolCategoryType_Other_Indicator]
           ,[TitleIPartASchoolDesignationTypeCodeValue]
           ,[TitleIPartASchoolDesignation_Indicator]
		   ,OperationalStatusTypeDescriptor_CodeValue
		   ,OperationalStatusTypeDescriptor_Description		   
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
SELECT 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.SchoolId) AS [_sourceKey],
        eoic_sch.IdentificationCode AS DistrictSchoolCode,
		eoic.IdentificationCode AS StateSchoolCode,
        CASE
		    WHEN eoic_sch.IdentificationCode IN (1291, 1292, 1293, 1294) THEN '1290'
			when eoic_sch.IdentificationCode IN (1440,1441) THEN '1440' 
			WHEN eoic_sch.IdentificationCode IN (4192,4192) THEN '4192' 
			WHEN eoic_sch.IdentificationCode IN (4031,4033) THEN '4033' 
			WHEN eoic_sch.IdentificationCode IN (1990,1991) THEN '1990' 
			WHEN eoic_sch.IdentificationCode IN (1140,4391) THEN '1140' 
			ELSE eoic_sch.IdentificationCode
		END AS UmbrellaSchoolCode,
		edorg.ShortNameOfInstitution, 
		edorg.NameOfInstitution,
		sct.CodeValue AS SchoolCategoryType, 
	    CASE  WHEN sct.CodeValue  IN ('Elementary School') THEN 1 ELSE 0 END  [SchoolCategoryType_Elementary_Indicator],
	    CASE  WHEN sct.CodeValue  IN ('Middle School') THEN 1 ELSE 0 END  [SchoolCategoryType_Middle_Indicator],
	    CASE  WHEN sct.CodeValue  IN ('High School') THEN 1 ELSE 0 END  [SchoolCategoryType_HighSchool_Indicator],
	    CASE  WHEN sct.CodeValue  NOT IN ('Elementary School','Middle School','High School') THEN 1 ELSE 0 END  [SchoolCategoryType_Combined_Indicator],
		0  [SchoolCategoryType_Other_Indicator],
		COALESCE(tIt.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,
		CASE WHEN tIt.CodeValue NOT IN ('Not designated as a Title I Part A school','N/A') THEN 1 ELSE 0 END AS TitleIPartASchoolDesignation_Indicator,
		COALESCE(ost.CodeValue,'N/A') AS OperationalStatusTypeDescriptor_CodeValue,	
		COALESCE(ost.[Description],'N/A') AS OperationalStatusTypeDescriptor_Description,
	    '07/01/2015' AS ValidFrom,
	    CASE WHEN COALESCE(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN '12/31/9999' ELSE edorg.LastModifiedDate END AS ValidTo,
	    CASE WHEN COALESCE(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN 1  ELSE 0  END AS IsCurrent,
	    @lineageKey AS [LineageKey]
--SELECT distinct *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType ost ON edorg.OperationalStatusTypeId = ost.OperationalStatusTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId 
                                                                               AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state code
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic_sch ON edorg.EducationOrganizationId = eoic_sch.EducationOrganizationId 
                                                                               AND eoic_sch.EducationOrganizationIdentificationSystemDescriptorId = 428 --district code
																			   

--SELECT * FROM EdFiDW.[dbo].[DimSchool]


--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;





