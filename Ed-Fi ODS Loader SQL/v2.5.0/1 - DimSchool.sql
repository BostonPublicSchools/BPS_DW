DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimSchool')
BEGIN
    INSERT INTO LongitudinalPOC.[dbo].[Lineage]
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
	 FROM LongitudinalPOC.[dbo].[Lineage]
	 WHERE TableName= 'dbo.DimSchool'
END 



INSERT INTO LongitudinalPOC.[dbo].[DimSchool]
           ([_sourceKey]
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
        eoic.IdentificationCode AS StateSchoolCode,
        CASE
		    WHEN s.SchoolId IN (1291, 1292, 1293, 1294) THEN '1290'
			when s.SchoolId IN (1440,1441) THEN '1440' 
			WHEN s.SchoolId IN (4192,4192) THEN '4192' 
			WHEN s.SchoolId IN (4031,4033) THEN '4033' 
			WHEN s.SchoolId IN (1990,1991) THEN '1990' 
			WHEN s.SchoolId IN (1140,4391) THEN '1140' 
			ELSE CAST(s.SchoolId AS NVARCHAR(50))
		END AS UmbrellaSchoolCode,
		edorg.ShortNameOfInstitution, 
		edorg.NameOfInstitution,
		sct.CodeValue AS SchoolCategoryType, 
	    CASE  WHEN sct.CodeValue  IN ('Elementary School') THEN 1 ELSE 0 END  [SchoolCategoryType_Elementary_Indicator],
	    CASE  WHEN sct.CodeValue  IN ('Middle School') THEN 1 ELSE 0 END  [SchoolCategoryType_Middle_Indicator],
	    CASE  WHEN sct.CodeValue  IN ('High School') THEN 1 ELSE 0 END  [SchoolCategoryType_HighSchool_Indicator],
	    CASE  WHEN sct.CodeValue  NOT IN ('Elementary School','Middle School','High School') THEN 1 ELSE 0 END  [SchoolCategoryType_Combined_Indicator],
		0  [SchoolCategoryType_Other_Indicator],
		ISNULL(tIt.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,
		CASE WHEN tIt.CodeValue NOT IN ('Not designated as a Title I Part A school','N/A') THEN 1 ELSE 0 END AS TitleIPartASchoolDesignation_Indicator,
		ISNULL(ost.CodeValue,'N/A') AS OperationalStatusTypeDescriptor_CodeValue,	
		ISNULL(ost.[Description],'N/A') AS OperationalStatusTypeDescriptor_Description,
	    GETDATE() AS ValidFrom,
	    CASE WHEN ISNULL(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN '12/31/9999' ELSE GETDATE() END AS ValidTo,
	    CASE WHEN ISNULL(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN 1  ELSE 0  END AS IsCurrent,
	    @lineageKey AS [LineageKey]
--SELECT distinct sct.CodeValue
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType ost ON edorg.OperationalStatusTypeId = ost.OperationalStatusTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId 
                                                                               AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state
WHERE NOT EXISTS(SELECT 1 
					FROM LongitudinalPOC.[dbo].[DimSchool] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.SchoolId) = ds._sourceKey);

--SELECT * FROM LongitudinalPOC.[dbo].[DimSchool]



--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*

UPDATE ds 
SET  ds.StateSchoolCode = ISNULL(eoic.IdentificationCode,'N/A'),
     ds.UmbrellaSchoolCode = CASE
						WHEN s.SchoolId IN (1291, 1292, 1293, 1294) THEN '1290'
						when s.SchoolId IN (1440,1441) THEN '1440' 
						WHEN s.SchoolId IN (4192,4192) THEN '4192' 
						WHEN s.SchoolId IN (4031,4033) THEN '4033' 
						WHEN s.SchoolId IN (1990,1991) THEN '1990' 
						WHEN s.SchoolId IN (1140,4391) THEN '1140' 
						ELSE CAST(s.SchoolId AS NVARCHAR(50))
					END 
--select *
FROM LongitudinalPOC.dbo.DimSchool ds --WHERE UmbrellaSchoolCode = 1290
     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s on ds._sourceKey = 'Ed-Fi|' + CAST(s.SchoolId AS NVARCHAR(50))
     LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic on ds._sourceKey = 'Ed-Fi|' + CAST(eoic.EducationOrganizationId AS NVARCHAR(50)) AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state
	 


*/

	




